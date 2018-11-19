import os
import gc
from tensorflow.python.lib.io import file_io as io

from subdag.constants import NOT_STARTED, WAITING, RUNNING, UPSTREAM_FAILED, FAILED, SUCCESS
from subdag.constants import RESTART, FAIL, SKIP, CLEAN_AND_RESTART, MARK_SUCCESS
import subdag.utils as u

RESUME_STRATEGY = { # when trying to run a task that's already running
    RUNNING: RESTART, # resume tasks that appear to be running
    FAILED: CLEAN_AND_RESTART, # restart failed tasks
    UPSTREAM_FAILED: RESTART,
    SUCCESS: SKIP, # skip successful tasks
}
RESTART_STRATEGY = {
    RUNNING: CLEAN_AND_RESTART,
    FAILED: CLEAN_AND_RESTART,
    UPSTREAM_FAILED: RESTART,
    SUCCESS: SKIP,
}
DEFAULT_STRATEGY = RESUME_STRATEGY

class TaskRunner(object):

    STRATEGIES = { # task state => possible STRATEGIES
        RUNNING: [FAIL, MARK_SUCCESS, RESTART, CLEAN_AND_RESTART],
        FAILED: [FAIL, MARK_SUCCESS, RESTART, CLEAN_AND_RESTART],
        UPSTREAM_FAILED: [FAIL, RESTART, CLEAN_AND_RESTART],
        SUCCESS: [SKIP, RESTART],
    }

    def __init__(self, kw, fn_formatter, strategy=DEFAULT_STRATEGY):
        for k, v in strategy.items():
            assert v in TaskRunner.STRATEGIES[k]

        self.kw = kw
        self.ff = fn_formatter
        self.strategy = strategy
        self.persist = not kw.get("persist", True)

    @property
    def has_pid(self):
        return io.file_exists(self.ff("pid"))

    @property
    def pid_process_exists(self):
        if not self.has_pid: return False
        try:
            os.kill(self.pid, 0)
        except OSError:
            return False
        else:
            return True

    @property
    def pid(self):
        with io.FileIO(self.ff("pid"), "r") as f:
            return int(f.read().strip())

    def write_pid(self):
        self.delete_pid()
        with io.FileIO(self.ff("pid"), "w") as f:
            f.write(str(os.getpid()))

    def delete_pid(self):
        if self.has_pid:
            io.delete_file(self.ff("pid"))

    def prepare_tasks(self, dst_tasks):
        for task in dst_tasks:
            task.recursive_set_downstream()
        for task in dst_tasks:
            self.recursive_prepare(task)

    def recursive_prepare(self, task):
        self.prepare(task)
        for us in task.immediate_upstream:
            self.recursive_prepare(us)

    def prepare(self, task):
        self.prepare_artifacts(task)
        if task.state in {NOT_STARTED, WAITING}:
            task.transition(WAITING)
        else:
            action = self.strategy[task.state]
            if action == CLEAN_AND_RESTART:
                task.clean()
                task.transition(WAITING, checks=False)
            if action == RESTART:
                task.transition(WAITING, checks=False)
            elif action == FAIL:
                task.transition(FAILED, checks=False)
            elif action == MARK_SUCCESS:
                task.transition(SUCCESS, checks=False)
            elif action == SKIP:
                pass # nothing to do

    def prepare_artifacts(self, task):
        for a in task.inputs + task.outputs:
            # make args defined in p.task("task", arg1="val") available to ff
            def ff(*a, **k):
                k_ = u.merge(k, task.extra_kwargs)
                return self.ff(*a, **k_)

            all_kw = u.merge(self.kw, task.extra_kwargs)
            a.set_loc(ff, **all_kw)

    def exec_kw(self, task):
        exec_kw = u.merge(self.kw, task.extra_kwargs)
        for i in task.inputs:
            if i.fn_id in exec_kw:
                del exec_kw[i.fn_id]
        return exec_kw

class SequentialRunner(TaskRunner):

    def run_all(self, dst_tasks, all_tasks):
        for task in dst_tasks:
            task.run(self)

class DirectRunner(SequentialRunner):

    def __init__(self, tasks_module, kw, fn_formatter, strategy=DEFAULT_STRATEGY):
        super(DirectRunner, self).__init__(kw, fn_formatter, strategy)
        self.tasks_module = tasks_module

    def run(self, task):
        from subdag.globals import loc

        inputs = [i.load() for i in task.inputs]

        task.create_dirs()
        outputs = loc.task_fns[task.name](*inputs, **self.exec_kw(task))

        task.maybe_persist(outputs)
        del inputs # potential memory leak
        gc.collect()

        return True

class DaskRunner(DirectRunner):

    def __init__(self, tasks_module, strategy=DEFAULT_STRATEGY, parallelism=None):
        super(DaskRunner, self).__init__(tasks_module, strategy)

        if parallelism is None:
            from dask import get
            self.get = get
        elif parallelism == "thread":
            from dask.threaded import get
            self.get = get
        elif parallelism == "process":
            from dask.multiprocessing import get
            self.get = get

    def run_all(self, dst_tasks, all_tasks):
        def run_task(task, *dep_names):
            self.loggers.wrap_tf_logger(task)
            task.create_dirs()
            task._run(self)

        dask_tasks = {}
        for name, task in all_tasks.items():
            deps = [t.name for t in task.immediate_upstream]
            dask_tasks[name] = (run_task, t) + tuple(deps)

        self.get(dask_tasks, [t.name for t in dst_tasks])
