use super::stat::WorkerLocalStat;
use super::work_bucket::*;
use super::*;
use crate::mmtk::MMTK;
use crate::util::{opaque_pointer::*, Address};
use crate::vm::{Collection, VMBinding};
use std::ffi::c_void;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Weak};

/// Thread-local data for each worker thread.
///
/// For mmtk, each gc can define their own worker-local data, to contain their required copy allocators and other stuffs.
pub trait GCWorkerLocal {
    fn init(&mut self, _tls: VMWorkerThread) {}
}

/// This struct will be accessed during trace_object(), which is performance critical.
/// However, we do not know its concrete type as the plan and its copy context is dynamically selected.
/// Instead use a void* type to store it, and during trace_object() we cast it to the correct copy context type.
#[derive(Copy, Clone)]
pub struct GCWorkerLocalPtr {
    data: *mut c_void,
    // Save the type name for debug builds, so we can later do type check
    #[cfg(debug_assertions)]
    ty: &'static str,
}

impl GCWorkerLocalPtr {
    pub const UNINITIALIZED: Self = GCWorkerLocalPtr {
        data: std::ptr::null_mut(),
        #[cfg(debug_assertions)]
        ty: "uninitialized",
    };

    pub fn new<W: GCWorkerLocal>(worker_local: W) -> Self {
        GCWorkerLocalPtr {
            data: Box::into_raw(Box::new(worker_local)) as *mut c_void,
            #[cfg(debug_assertions)]
            ty: std::any::type_name::<W>(),
        }
    }

    /// # Safety
    /// The user needs to guarantee that the type supplied here is the same type used to create this pointer.
    pub unsafe fn as_type<W: GCWorkerLocal>(&mut self) -> &mut W {
        #[cfg(debug_assertions)]
        debug_assert_eq!(self.ty, std::any::type_name::<W>());
        &mut *(self.data as *mut W)
    }
}

const LOCALLY_CACHED_WORKS: usize = 1;

pub struct GCWorker<VM: VMBinding> {
    pub tls: VMWorkerThread,
    pub ordinal: usize,
    pub parked: AtomicBool,
    scheduler: Arc<GCWorkScheduler<VM>>,
    local: GCWorkerLocalPtr,
    pub local_work_bucket: WorkBucket<VM>,
    pub sender: Sender<CoordinatorMessage<VM>>,
    pub stat: WorkerLocalStat<VM>,
    mmtk: Option<&'static MMTK<VM>>,
    is_coordinator: bool,
    local_work_buffer: Vec<(WorkBucketStage, Box<dyn GCWork<VM>>)>,
    edges: [Vec<Address>; 32],
}

unsafe impl<VM: VMBinding> Sync for GCWorker<VM> {}
unsafe impl<VM: VMBinding> Send for GCWorker<VM> {}

impl<VM: VMBinding> GCWorker<VM> {
    pub fn new(
        ordinal: usize,
        scheduler: Weak<GCWorkScheduler<VM>>,
        is_coordinator: bool,
        sender: Sender<CoordinatorMessage<VM>>,
    ) -> Self {
        let scheduler = scheduler.upgrade().unwrap();
        Self {
            tls: VMWorkerThread(VMThread::UNINITIALIZED),
            ordinal,
            parked: AtomicBool::new(true),
            local: GCWorkerLocalPtr::UNINITIALIZED,
            // Single-threaded works don't use it so can be unsingle-threaded.
            local_work_bucket: WorkBucket::new(
                true,
                scheduler.worker_monitor.clone(),
                false,
                WorkBucketStage::Unconstrained,
                0,
            ),
            sender,
            scheduler,
            stat: Default::default(),
            mmtk: None,
            is_coordinator,
            local_work_buffer: Vec::with_capacity(LOCALLY_CACHED_WORKS),
            edges: Default::default(),
        }
    }

    #[inline]
    pub fn add_edges<E: ProcessEdgesWork<VM = VM>>(
        &mut self,
        edges: Vec<Address>,
        mmtk: &'static MMTK<VM>,
    ) {
        let mask = self.scheduler.hash_mask();
        for edge in edges {
            let id = edge & mask;
            self.edges[id].push(edge);
            if self.edges[id].len() == E::CAPACITY {
                let mut new_edges = Vec::new();
                mem::swap(&mut new_edges, &mut self.edges[id]);
                self.add_single_threaded_work(
                    WorkBucketStage::Closure,
                    id,
                    E::new(new_edges, false, mmtk),
                )
            }
        }
    }

    #[inline]
    pub fn add_edges_and_flush<E: ProcessEdgesWork<VM = VM>>(
        &mut self,
        edges: Vec<Address>,
        mmtk: &'static MMTK<VM>,
    ) {
        self.add_edges::<E>(edges, mmtk);
        let mask = self.scheduler.hash_mask();
        for id in 0..=mask {
            let mut new_edges = Vec::new();
            mem::swap(&mut new_edges, &mut self.edges[id]);
            self.add_single_threaded_work(
                WorkBucketStage::Closure,
                id,
                E::new(new_edges, false, mmtk),
            )
        }
    }

    #[inline]
    pub fn add_single_threaded_work(
        &mut self,
        stage: WorkBucketStage,
        id: usize,
        work: impl GCWork<VM>,
    ) {
        self.scheduler
            .single_threaded_work_buckets
            .as_ref()
            .unwrap()[stage][id]
            .add_with_priority(1000, box work);
    }

    #[inline]
    pub fn add_work(&mut self, stage: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[stage].is_activated() {
            self.scheduler.work_buckets[stage].add_with_priority(1000, box work);
            return;
        }
        self.local_work_buffer.push((stage, box work));
        if self.local_work_buffer.len() > LOCALLY_CACHED_WORKS {
            self.flush();
        }
    }

    #[cold]
    fn flush(&mut self) {
        let mut buffer = Vec::with_capacity(LOCALLY_CACHED_WORKS);
        std::mem::swap(&mut buffer, &mut self.local_work_buffer);
        for (bucket, work) in buffer {
            self.scheduler.work_buckets[bucket].add_with_priority(1000, work);
        }
    }

    pub fn is_parked(&self) -> bool {
        self.parked.load(Ordering::SeqCst)
    }

    pub fn is_coordinator(&self) -> bool {
        self.is_coordinator
    }

    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    /// # Safety
    /// The user needs to guarantee that the type supplied here is the same type used to create this pointer.
    #[inline]
    pub unsafe fn local<W: 'static + GCWorkerLocal>(&mut self) -> &mut W {
        self.local.as_type::<W>()
    }

    pub fn set_local(&mut self, local: GCWorkerLocalPtr) {
        self.local = local;
    }

    pub fn init(&mut self, tls: VMWorkerThread) {
        self.tls = tls;
    }

    pub fn do_work(&'static mut self, mut work: impl GCWork<VM>) {
        work.do_work(self, self.mmtk.unwrap());
    }

    pub fn run(&mut self, mmtk: &'static MMTK<VM>) {
        self.mmtk = Some(mmtk);
        self.parked.store(false, Ordering::SeqCst);
        loop {
            while let Some((stage, mut work)) = self.local_work_buffer.pop() {
                debug_assert!(self.scheduler.work_buckets[stage].is_activated());
                work.do_work_with_stat(self, mmtk);
            }
            let (mut work, bucket) = self.scheduler().poll(self);
            debug_assert!(!self.is_parked());
            if let Some((stage, id)) = bucket {
                work.do_single_threaded_work_with_stat(self, stage, id, mmtk);
            } else {
                work.do_work_with_stat(self, mmtk);
            }
        }
    }
}

pub struct WorkerGroup<VM: VMBinding> {
    pub workers: Vec<GCWorker<VM>>,
}

impl<VM: VMBinding> WorkerGroup<VM> {
    pub fn new(
        workers: usize,
        scheduler: Weak<GCWorkScheduler<VM>>,
        sender: Sender<CoordinatorMessage<VM>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            workers: (0..workers)
                .map(|i| GCWorker::new(i, scheduler.clone(), false, sender.clone()))
                .collect(),
        })
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn parked_workers(&self) -> usize {
        self.workers.iter().filter(|w| w.is_parked()).count()
    }

    pub fn all_parked(&self) -> bool {
        self.parked_workers() == self.worker_count()
    }

    pub fn spawn_workers(&'static self, tls: VMThread, _mmtk: &'static MMTK<VM>) {
        for i in 0..self.worker_count() {
            let worker = &self.workers[i];
            VM::VMCollection::spawn_worker_thread(tls, Some(worker));
        }
    }
}
