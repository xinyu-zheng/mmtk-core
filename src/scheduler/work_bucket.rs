use super::*;
use crate::vm::VMBinding;
use enum_map::Enum;
use spin::RwLock;
use std::cmp;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// A unique work-packet id for each instance of work-packet
#[derive(Eq, PartialEq, Clone, Copy)]
struct WorkUID(u64);

impl WorkUID {
    pub fn new() -> Self {
        static WORK_UID: AtomicU64 = AtomicU64::new(0);
        Self(WORK_UID.fetch_add(1, Ordering::Relaxed))
    }
}

struct PrioritizedWork<VM: VMBinding> {
    priority: usize,
    work_uid: WorkUID,
    work: Box<dyn GCWork<VM>>,
}

impl<VM: VMBinding> PrioritizedWork<VM> {
    pub fn new(priority: usize, work: Box<dyn GCWork<VM>>) -> Self {
        Self {
            priority,
            work,
            work_uid: WorkUID::new(),
        }
    }
}

impl<VM: VMBinding> PartialEq for PrioritizedWork<VM> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.work_uid == other.work_uid
    }
}

impl<VM: VMBinding> Eq for PrioritizedWork<VM> {}

impl<VM: VMBinding> Ord for PrioritizedWork<VM> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl<VM: VMBinding> PartialOrd for PrioritizedWork<VM> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct WorkBucket<VM: VMBinding> {
    active: AtomicBool,
    /// A priority queue
    queue: RwLock<BinaryHeap<PrioritizedWork<VM>>>,
    monitor: Arc<(Mutex<()>, Condvar)>,
    can_open: Option<Box<dyn (Fn() -> bool) + Send>>,
    stage: WorkBucketStage,
    single_threaded_id: Option<usize>,
    busy: AtomicBool,
}

impl<VM: VMBinding> WorkBucket<VM> {
    pub const DEFAULT_PRIORITY: usize = 1000;
    pub fn new(
        active: bool,
        monitor: Arc<(Mutex<()>, Condvar)>,
        stage: WorkBucketStage,
        single_threaded_id: Option<usize>,
    ) -> Self {
        Self {
            active: AtomicBool::new(active),
            queue: Default::default(),
            monitor,
            can_open: None,
            stage,
            single_threaded_id,
            busy: AtomicBool::new(false),
        }
    }
    fn notify_one_worker(&self) {
        let _guard = self.monitor.0.lock().unwrap();
        self.monitor.1.notify_one()
    }
    fn notify_all_workers(&self) {
        let _guard = self.monitor.0.lock().unwrap();
        self.monitor.1.notify_all()
    }
    pub fn is_activated(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    /// Enable the bucket
    pub fn activate(&self) {
        self.active.store(true, Ordering::SeqCst);
    }
    /// Test if the bucket is drained
    pub fn is_empty(&self) -> bool {
        //let sz = self.queue.read().len();
        //debug!(
        //    "stage: {:?}, single: {}, len: {}, busy: {}",
        //    self.stage(),
        //    self.is_single_threaded(),
        //    sz,
        //    self.busy()
        //);
        self.queue.read().len() == 0
    }
    pub fn is_drained(&self) -> bool {
        self.is_activated() && self.is_empty()
    }
    /// Disable the bucket
    pub fn deactivate(&self) {
        debug_assert!(
            self.queue.read().is_empty(),
            "Bucket not drained before close"
        );
        self.active.store(false, Ordering::SeqCst);
    }
    /// Add a work packet to this bucket, with a given priority
    pub fn add_with_priority(&self, priority: usize, work: Box<dyn GCWork<VM>>) {
        self.queue
            .write()
            .push(PrioritizedWork::new(priority, work));
        self.notify_one_worker(); // FIXME: Performance
    }
    /// Add a work packet to this bucket, with a default priority (1000)
    pub fn add<W: GCWork<VM>>(&self, work: W) {
        self.add_with_priority(Self::DEFAULT_PRIORITY, box work);
    }
    pub fn bulk_add_with_priority(&self, priority: usize, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        {
            let mut queue = self.queue.write();
            for w in work_vec {
                queue.push(PrioritizedWork::new(priority, w));
            }
        }
        self.notify_all_workers(); // FIXME: Performance
    }
    pub fn bulk_add(&self, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        self.bulk_add_with_priority(1000, work_vec)
    }
    pub fn busy(&self) -> bool {
        self.busy.load(Ordering::SeqCst)
    }
    pub fn be_busy(&self) {
        self.busy.store(true, Ordering::SeqCst);
    }
    pub fn be_idle(&self) {
        self.busy.store(false, Ordering::SeqCst);
    }
    pub fn compare_exchange_busy(&self) -> Result<bool, bool> {
        self.busy
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
    }
    /// Get a work packet (with the greatest priority) from this bucket
    pub fn poll(&self) -> Option<Box<dyn GCWork<VM>>> {
        if !self.active.load(Ordering::SeqCst) {
            return None;
        }
        self.queue.write().pop().map(|v| v.work)
    }
    pub fn poll_single_threaded(&self) -> Option<(Box<dyn GCWork<VM>>, usize)> {
        if !self.active.load(Ordering::SeqCst) {
            return None;
        }
        if self.compare_exchange_busy() == Ok(false) {
            debug_assert!(self.busy());
            let work = self.queue.write().pop().map(|v| (v.work, self.id()));
            if let Some(_) = work {
                work
            } else {
                // Unset busy if no work is popped.
                self.be_idle();
                work
            }
        } else {
            None
        }
    }
    pub fn set_open_condition(&mut self, pred: impl Fn() -> bool + Send + 'static) {
        self.can_open = Some(box pred);
    }
    pub fn update(&self, scheduler: &GCWorkScheduler<VM>) -> bool {
        if let Some(can_open) = self.can_open.as_ref() {
            if !self.is_activated() && can_open() {
                self.activate();
                if self.stage() == WorkBucketStage::Closure {
                    scheduler
                        .single_threaded_work_buckets
                        .iter()
                        .for_each(|bucket| bucket.activate());
                }
                return true;
            }
        }
        false
    }
    pub fn stage(&self) -> WorkBucketStage {
        self.stage
    }
    pub fn id(&self) -> usize {
        self.single_threaded_id.unwrap()
    }
    pub fn number_of_works(&self) -> usize {
        self.queue.read().len()
    }
}

#[derive(Debug, Enum, Copy, Clone, Eq, PartialEq)]
pub enum WorkBucketStage {
    Unconstrained,
    Prepare,
    Closure,
    // TODO: We only support final reference at the moment. If we have references of multiple strengths,
    // we may need more than one buckets for each reference strength.
    RefClosure,
    RefForwarding,
    Release,
    Final,
}
