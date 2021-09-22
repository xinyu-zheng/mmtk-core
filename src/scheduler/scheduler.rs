use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, WorkerGroup};
use super::*;
use crate::mmtk::MMTK;
use crate::util::opaque_pointer::*;
use crate::vm::VMBinding;
use enum_map::{enum_map, EnumMap};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock};

pub enum CoordinatorMessage<VM: VMBinding> {
    Work(Box<dyn CoordinatorWork<VM>>),
    AllWorkerParked,
    BucketDrained,
}

pub struct GCWorkScheduler<VM: VMBinding> {
    pub work_buckets: EnumMap<WorkBucketStage, WorkBucket<VM>>,
    /// Number of single-threaded Closure buckets.
    pub colors: usize,
    pub single_threaded_work_buckets: Option<EnumMap<WorkBucketStage, Vec<WorkBucket<VM>>>>,
    /// Work for the coordinator thread
    pub coordinator_work: WorkBucket<VM>,
    /// workers
    worker_group: Option<Arc<WorkerGroup<VM>>>,
    /// Condition Variable for worker synchronization
    pub worker_monitor: Arc<(Mutex<()>, Condvar)>,
    mmtk: Option<&'static MMTK<VM>>,
    coordinator_worker: Option<RwLock<GCWorker<VM>>>,
    /// A message channel to send new coordinator work and other actions to the coordinator thread
    channel: (
        Sender<CoordinatorMessage<VM>>,
        Receiver<CoordinatorMessage<VM>>,
    ),
    startup: Mutex<Option<Box<dyn CoordinatorWork<VM>>>>,
    finalizer: Mutex<Option<Box<dyn CoordinatorWork<VM>>>>,
    /// A callback to be fired after the `Closure` bucket is drained.
    /// This callback should return `true` if it adds more work packets to the
    /// `Closure` bucket. `WorkBucket::can_open` then consult this return value
    /// to prevent the GC from proceeding to the next stage, if we still have
    /// `Closure` work to do.
    ///
    /// We use this callback to process ephemeron objects. `closure_end` can re-enable
    /// the `Closure` bucket multiple times to iteratively discover and process
    /// more ephemeron objects.
    closure_end: Mutex<Option<Box<dyn Send + Fn() -> bool>>>,
}

// The 'channel' inside Scheduler disallows Sync for Scheduler. We have to make sure we use channel properly:
// 1. We should never directly use Sender. We clone the sender and let each worker have their own copy.
// 2. Only the coordinator can use Receiver.
// TODO: We should remove channel from Scheduler, and directly send Sender/Receiver when creating the coordinator and
// the workers.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new() -> Arc<Self> {
        let worker_monitor: Arc<(Mutex<()>, Condvar)> = Default::default();
        Arc::new(Self {
            work_buckets: enum_map! {
                WorkBucketStage::Unconstrained => WorkBucket::new(true, worker_monitor.clone(), false, WorkBucketStage::Unconstrained, 0),
                WorkBucketStage::Prepare => WorkBucket::new(false, worker_monitor.clone(), false, WorkBucketStage::Prepare, 0),
                WorkBucketStage::Closure => WorkBucket::new(false, worker_monitor.clone(), false, WorkBucketStage::Closure, 0),
                WorkBucketStage::RefClosure => WorkBucket::new(false, worker_monitor.clone(), false, WorkBucketStage::RefClosure, 0),
                WorkBucketStage::RefForwarding => WorkBucket::new(false, worker_monitor.clone(), false, WorkBucketStage::RefForwarding, 0),
                WorkBucketStage::Release => WorkBucket::new(false, worker_monitor.clone(), false, WorkBucketStage::Release, 0),
                WorkBucketStage::Final => WorkBucket::new(false, worker_monitor.clone(), false, WorkBucketStage::Final, 0),
            },
            colors: 0,
            single_threaded_work_buckets: None,
            coordinator_work: WorkBucket::new(
                true,
                worker_monitor.clone(),
                false,
                WorkBucketStage::Unconstrained,
                0,
            ),
            worker_group: None,
            worker_monitor,
            mmtk: None,
            coordinator_worker: None,
            channel: channel(),
            startup: Mutex::new(None),
            finalizer: Mutex::new(None),
            closure_end: Mutex::new(None),
        })
    }

    #[inline]
    pub fn num_workers(&self) -> usize {
        self.worker_group.as_ref().unwrap().worker_count()
    }

    pub fn initialize(
        self: &'static Arc<Self>,
        num_workers: usize,
        mmtk: &'static MMTK<VM>,
        tls: VMThread,
    ) {
        use crate::scheduler::work_bucket::WorkBucketStage::*;
        let num_workers = if cfg!(feature = "single_worker") {
            1
        } else {
            num_workers
        };

        let mut self_mut = self.clone();
        let self_mut = unsafe { Arc::get_mut_unchecked(&mut self_mut) };

        // Let colors be power of 2 so that hashing can rely on bits pattern of Address.
        let mut colors = 1;
        while colors < num_workers {
            colors *= 2;
        }
        self_mut.colors = colors;
        debug_assert!(self.colors >= num_workers && self.colors < 2 * num_workers);
        self_mut.single_threaded_work_buckets = Some(enum_map! {
            WorkBucketStage::Unconstrained => vec![WorkBucket::new(true, self.worker_monitor.clone(), true, WorkBucketStage::Unconstrained, 0)],
            WorkBucketStage::Prepare => vec![WorkBucket::new(false, self.worker_monitor.clone(), true, WorkBucketStage::Prepare, 0)],
            WorkBucketStage::Closure => (0..self.colors).map(|id| WorkBucket::new(false, self.worker_monitor.clone(), true, WorkBucketStage::Closure, id)).collect(),
            WorkBucketStage::RefClosure => vec![WorkBucket::new(false, self.worker_monitor.clone(), true, WorkBucketStage::RefClosure, 0)],
            WorkBucketStage::RefForwarding => vec![WorkBucket::new(false, self.worker_monitor.clone(), true, WorkBucketStage::RefForwarding, 0)],
            WorkBucketStage::Release => vec![WorkBucket::new(false, self.worker_monitor.clone(), true, WorkBucketStage::Release, 0)],
            WorkBucketStage::Final => vec![WorkBucket::new(false, self.worker_monitor.clone(), true, WorkBucketStage::Final, 0)],
        });
        self_mut.mmtk = Some(mmtk);
        self_mut.coordinator_worker = Some(RwLock::new(GCWorker::new(
            0,
            Arc::downgrade(self),
            true,
            self.channel.0.clone(),
        )));
        self_mut.worker_group = Some(WorkerGroup::new(
            num_workers,
            Arc::downgrade(self),
            self.channel.0.clone(),
        ));
        self.worker_group.as_ref().unwrap().spawn_workers(tls, mmtk);

        {
            // Unconstrained is always open. Prepare will be opened at the beginning of a GC.
            // This vec will grow for each stage we call with open_next()
            let mut open_stages: Vec<WorkBucketStage> = vec![Unconstrained, Prepare];
            // The rest will open after the previous stage is done.
            let mut open_next = |s: WorkBucketStage| {
                let cur_stages = open_stages.clone();
                self_mut.work_buckets[s].set_open_condition(move || {
                    let should_open =
                        self.are_buckets_drained(&cur_stages) && self.worker_group().all_parked();
                    // Additional check before the `RefClosure` bucket opens.
                    if should_open && s == WorkBucketStage::RefClosure {
                        if let Some(closure_end) = self.closure_end.lock().unwrap().as_ref() {
                            if closure_end() {
                                // Don't open `RefClosure` if `closure_end` added more works to `Closure`.
                                return false;
                            }
                        }
                    }
                    should_open
                });
                open_stages.push(s);
            };

            open_next(Closure);
            open_next(RefClosure);
            open_next(RefForwarding);
            open_next(Release);
            open_next(Final);
        }
    }

    fn are_buckets_drained(&self, stages: &[WorkBucketStage]) -> bool {
        stages.iter().all(|&s| {
            self.work_buckets[s].is_drained()
                && self.single_threaded_work_buckets.as_ref().unwrap()[s]
                    .iter()
                    .all(|b| b.is_drained())
        })
    }

    pub fn initialize_worker(self: &Arc<Self>, tls: VMWorkerThread) {
        let mut coordinator_worker = self.coordinator_worker.as_ref().unwrap().write().unwrap();
        coordinator_worker.init(tls);
    }

    pub fn set_initializer<W: CoordinatorWork<VM>>(&self, w: Option<W>) {
        *self.startup.lock().unwrap() = w.map(|w| box w as Box<dyn CoordinatorWork<VM>>);
    }

    pub fn set_finalizer<W: CoordinatorWork<VM>>(&self, w: Option<W>) {
        *self.finalizer.lock().unwrap() = w.map(|w| box w as Box<dyn CoordinatorWork<VM>>);
    }

    pub fn on_closure_end(&self, f: Box<dyn Send + Fn() -> bool>) {
        *self.closure_end.lock().unwrap() = Some(f);
    }

    pub fn worker_group(&self) -> Arc<WorkerGroup<VM>> {
        self.worker_group.as_ref().unwrap().clone()
    }

    fn all_buckets_empty(&self) -> bool {
        //let a = self.work_buckets.values().all(|bucket| bucket.is_empty());
        //let b = self
        //    .single_threaded_work_buckets
        //    .values()
        //    .all(|bucket| bucket.is_empty());
        //a && b
        self.work_buckets.values().all(|bucket| bucket.is_empty())
            && self
                .single_threaded_work_buckets
                .as_ref()
                .unwrap()
                .values()
                .all(|buckets| buckets.iter().all(|bucket| bucket.is_empty()))
    }

    /// Open buckets if their conditions are met
    fn update_buckets(&self) {
        let mut buckets_updated = false;
        for (stage, bucket) in self.work_buckets.iter() {
            if stage == WorkBucketStage::Unconstrained {
                continue;
            }
            // Single-threaded buckets of the same stage are also updated.
            buckets_updated |= bucket.update(self);
        }
        if buckets_updated {
            // Notify the workers for new work
            let _guard = self.worker_monitor.0.lock().unwrap();
            self.worker_monitor.1.notify_all();
        }
    }

    /// Execute coordinator work, in the controller thread
    fn process_coordinator_work(&self, mut work: Box<dyn CoordinatorWork<VM>>) {
        let mut coordinator_worker = self.coordinator_worker.as_ref().unwrap().write().unwrap();
        let mmtk = self.mmtk.unwrap();
        work.do_work_with_stat(&mut coordinator_worker, mmtk);
    }

    /// Drain the message queue and execute coordinator work. Only the coordinator should call this.
    pub fn wait_for_completion(&self) {
        // At the start of a GC, we probably already have received a `ScheduleCollection` work. Run it now.
        if let Some(initializer) = self.startup.lock().unwrap().take() {
            self.process_coordinator_work(initializer);
        }
        loop {
            let message = self.channel.1.recv().unwrap();
            match message {
                CoordinatorMessage::Work(work) => {
                    self.process_coordinator_work(work);
                }
                CoordinatorMessage::AllWorkerParked | CoordinatorMessage::BucketDrained => {
                    self.update_buckets();
                }
            }
            let _guard = self.worker_monitor.0.lock().unwrap();
            if self.worker_group().all_parked() && self.all_buckets_empty() {
                break;
            }
        }
        for message in self.channel.1.try_iter() {
            if let CoordinatorMessage::Work(work) = message {
                self.process_coordinator_work(work);
            }
        }
        self.deactivate_all();
        // Finalization: Resume mutators, reset gc states
        // Note: Resume-mutators must happen after all work buckets are closed.
        //       Otherwise, for generational GCs, workers will receive and process
        //       newly generated remembered-sets from those open buckets.
        //       But these remsets should be preserved until next GC.
        if let Some(finalizer) = self.finalizer.lock().unwrap().take() {
            self.process_coordinator_work(finalizer);
        }
        debug_assert!(!self.work_buckets[WorkBucketStage::Prepare].is_activated());
        debug_assert!(!self.work_buckets[WorkBucketStage::Closure].is_activated());
        debug_assert!(!self.work_buckets[WorkBucketStage::RefClosure].is_activated());
        debug_assert!(!self.work_buckets[WorkBucketStage::RefForwarding].is_activated());
        debug_assert!(!self.work_buckets[WorkBucketStage::Release].is_activated());
        debug_assert!(!self.work_buckets[WorkBucketStage::Final].is_activated());

        debug_assert!(self.single_threaded_work_buckets.as_ref().unwrap()
            [WorkBucketStage::Prepare]
            .iter()
            .all(|bucket| !bucket.is_activated()));
        debug_assert!(self.single_threaded_work_buckets.as_ref().unwrap()
            [WorkBucketStage::Closure]
            .iter()
            .all(|bucket| !bucket.is_activated()));
        debug_assert!(self.single_threaded_work_buckets.as_ref().unwrap()
            [WorkBucketStage::RefClosure]
            .iter()
            .all(|bucket| !bucket.is_activated()));
        debug_assert!(self.single_threaded_work_buckets.as_ref().unwrap()
            [WorkBucketStage::RefForwarding]
            .iter()
            .all(|bucket| !bucket.is_activated()));
        debug_assert!(self.single_threaded_work_buckets.as_ref().unwrap()
            [WorkBucketStage::Release]
            .iter()
            .all(|bucket| !bucket.is_activated()));
        debug_assert!(
            self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Final]
                .iter()
                .all(|bucket| !bucket.is_activated())
        );
    }

    pub fn deactivate_all(&self) {
        self.work_buckets[WorkBucketStage::Prepare].deactivate();
        self.work_buckets[WorkBucketStage::Closure].deactivate();
        self.work_buckets[WorkBucketStage::RefClosure].deactivate();
        self.work_buckets[WorkBucketStage::RefForwarding].deactivate();
        self.work_buckets[WorkBucketStage::Release].deactivate();
        self.work_buckets[WorkBucketStage::Final].deactivate();

        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Prepare]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Closure]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::RefClosure]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::RefForwarding]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Release]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Final]
            .iter()
            .for_each(|bucket| bucket.deactivate());
    }

    pub fn reset_state(&self) {
        // self.work_buckets[WorkBucketStage::Prepare].deactivate();
        self.work_buckets[WorkBucketStage::Closure].deactivate();
        self.work_buckets[WorkBucketStage::RefClosure].deactivate();
        self.work_buckets[WorkBucketStage::RefForwarding].deactivate();
        self.work_buckets[WorkBucketStage::Release].deactivate();
        self.work_buckets[WorkBucketStage::Final].deactivate();

        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Closure]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::RefClosure]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::RefForwarding]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Release]
            .iter()
            .for_each(|bucket| bucket.deactivate());
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Final]
            .iter()
            .for_each(|bucket| bucket.deactivate());
    }

    pub fn add_coordinator_work(&self, work: impl CoordinatorWork<VM>, worker: &GCWorker<VM>) {
        worker
            .sender
            .send(CoordinatorMessage::Work(box work))
            .unwrap();
    }

    #[inline]
    fn pop_scheduable_work(
        &self,
        worker: &GCWorker<VM>,
    ) -> Option<(Box<dyn GCWork<VM>>, bool, Option<(WorkBucketStage, usize)>)> {
        if let Some(work) = worker.local_work_bucket.poll() {
            return Some((work, worker.local_work_bucket.is_empty(), None));
        }
        for work_bucket in self.work_buckets.values() {
            if let Some(work) = work_bucket.poll() {
                return Some((work, work_bucket.is_empty(), None));
            }
        }
        for single_threaded_work_buckets in
            self.single_threaded_work_buckets.as_ref().unwrap().values()
        {
            for single_threaded_work_bucket in single_threaded_work_buckets {
                if let Some((work, stage, id)) = single_threaded_work_bucket.poll_single_threaded()
                {
                    return Some((
                        work,
                        single_threaded_work_bucket.is_empty(),
                        Some((stage, id)),
                    ));
                }
            }
        }
        None
    }

    /// Get a scheduable work (with its stage and id if it's single-threaded). Called by workers
    #[inline]
    pub fn poll(
        &self,
        worker: &GCWorker<VM>,
    ) -> (Box<dyn GCWork<VM>>, Option<(WorkBucketStage, usize)>) {
        if let Some((work, bucket_is_empty, bucket)) = self.pop_scheduable_work(worker) {
            if bucket_is_empty {
                worker
                    .sender
                    .send(CoordinatorMessage::BucketDrained)
                    .unwrap();
            }
            (work, bucket)
        } else {
            self.poll_slow(worker)
        }
    }

    #[cold]
    fn poll_slow(
        &self,
        worker: &GCWorker<VM>,
    ) -> (Box<dyn GCWork<VM>>, Option<(WorkBucketStage, usize)>) {
        debug_assert!(!worker.is_parked());
        let mut guard = self.worker_monitor.0.lock().unwrap();
        loop {
            debug_assert!(!worker.is_parked());
            if let Some((work, bucket_is_empty, stage)) = self.pop_scheduable_work(worker) {
                if bucket_is_empty {
                    worker
                        .sender
                        .send(CoordinatorMessage::BucketDrained)
                        .unwrap();
                }
                return (work, stage);
            }
            // Park this worker
            worker.parked.store(true, Ordering::SeqCst);
            if self.worker_group().all_parked() {
                worker
                    .sender
                    .send(CoordinatorMessage::AllWorkerParked)
                    .unwrap();
            }
            // Wait
            guard = self.worker_monitor.1.wait(guard).unwrap();
            // Unpark this worker
            worker.parked.store(false, Ordering::SeqCst);
        }
    }

    pub fn enable_stat(&self) {
        for worker in &self.worker_group().workers {
            worker.stat.enable();
        }
        let coordinator_worker = self.coordinator_worker.as_ref().unwrap().read().unwrap();
        coordinator_worker.stat.enable();
    }

    pub fn statistics(&self) -> HashMap<String, String> {
        let mut summary = SchedulerStat::default();
        for worker in &self.worker_group().workers {
            summary.merge(&worker.stat);
        }
        let coordinator_worker = self.coordinator_worker.as_ref().unwrap().read().unwrap();
        summary.merge(&coordinator_worker.stat);
        summary.harness_stat()
    }

    pub fn notify_mutators_paused(&self, mmtk: &'static MMTK<VM>) {
        mmtk.plan.base().control_collector_context.clear_request();
        debug_assert!(!self.work_buckets[WorkBucketStage::Prepare].is_activated());
        debug_assert!(self.single_threaded_work_buckets.as_ref().unwrap()
            [WorkBucketStage::Prepare]
            .iter()
            .all(|bucket| !bucket.is_activated()));
        self.work_buckets[WorkBucketStage::Prepare].activate();
        self.single_threaded_work_buckets.as_ref().unwrap()[WorkBucketStage::Prepare]
            .iter()
            .for_each(|bucket| bucket.activate());
        let _guard = self.worker_monitor.0.lock().unwrap();
        self.worker_monitor.1.notify_all();
    }
}
