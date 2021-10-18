use super::worker::*;
use crate::mmtk::MMTK;
use crate::vm::VMBinding;
use std::any::{type_name, TypeId};

/// A special kind of work that will execute on the coordinator (i.e. controller) thread
///
/// The coordinator thread holds the global monitor lock when executing `CoordinatorWork`s.
/// So, directly adding new work to any buckets will cause dead lock.
/// For this case, use `WorkBucket::add_with_priority_unsync` instead.
pub trait CoordinatorWork<VM: VMBinding>: 'static + Send + GCWork<VM> {}

pub trait GCWork<VM: VMBinding>: 'static + Send {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>);
    fn do_single_threaded_work(
        &mut self,
        worker: &mut GCWorker<VM>,
        id: usize,
        mmtk: &'static MMTK<VM>,
    ) {
        self.do_work(worker, mmtk);
        let bucket = &worker.scheduler().single_threaded_work_buckets[id];
        bucket.be_idle();
    }
    #[inline]
    fn do_work_with_stat(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug!("{}", std::any::type_name::<Self>());
        let stat = worker
            .stat
            .measure_work(TypeId::of::<Self>(), type_name::<Self>(), mmtk);
        self.do_work(worker, mmtk);
        stat.end_of_work(&mut worker.stat);
    }
    #[inline]
    fn do_single_threaded_work_with_stat(
        &mut self,
        worker: &mut GCWorker<VM>,
        id: usize,
        mmtk: &'static MMTK<VM>,
    ) {
        debug_assert!(worker.scheduler().single_threaded_work_buckets[id].busy());
        self.do_work_with_stat(worker, mmtk);
    }
}
