import logging
import traceback
import datetime
import json
import os

from subdag.task import Task
import subdag.db as db
from subdag.utils import lazy

from subdag.constants import NOT_STARTED, RUNNING, FAILED, SUCCESS
from subdag.constants import RESUME, RESTART, SKIP
from subdag.globals import loc

RESUME_RESTART_STRATEGY = {
    RUNNING: RESUME,
    FAILED: RESTART,
    SUCCESS: SKIP,
}
SKIP_STRATEGY = {
    RUNNING: RESUME,
    FAILED: SKIP,
    SUCCESS: SKIP,
}
DEFAULT_STRATEGY = RESUME_RESTART_STRATEGY


class Pipeline(object):

    STATES = [RUNNING, FAILED, SUCCESS] # possible states
    TRANSITIONS = { # # pipeline state => possible next new state (always allow transition to same state)
        NOT_STARTED: [RUNNING],
        RUNNING: [FAILED, SUCCESS],
        FAILED: [RUNNING],
        SUCCESS: [RUNNING],
    }

    def __init__(self, name, run_id=None, skip_checks=False, strategy=DEFAULT_STRATEGY):
        self.name = name
        self.strategy = strategy
        self.skip_checks = skip_checks
        self.tasks = {}
        self.run_id = run_id or self.new_run_id()

        db.Pipeline.get_or_create(id=self.name)
        self.db = db.PipelineRun.get_or_create(
            pipeline_id=self.name, run_id=self.run_id)[0]

    def __repr__(self):
        return self.name

    def task(self, *args, **kwargs):
        t = self._task(*args, **kwargs)
        if len(t.outputs) == 0:
            return t
        elif len(t.outputs) == 1:
            return t, t.outputs[0]
        else:
            return t, t.outputs

    def _task(self, name, inputs=[], deps=[], **extra_kwargs):
        task = Task(
            name, self.run_id,
            inputs=inputs,
            outputs=loc.task_outputs.get(name, []),
            deps=deps,
            **extra_kwargs
        )
        self.tasks[name] = task
        return task

    def new_run_id(self):
        ts = datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
        return "%s_%s" % (ts, self.name)

    def run(self, dst_task_ids, runner):
        if self.incomplete_run_exists(runner): return

        dst_tasks = [self.tasks[tid] for tid in dst_task_ids]
        runner.prepare_tasks(dst_tasks)
        self.transition(RUNNING)
        self.persist_run_args(runner.kw, dst_task_ids)

        try:
            runner.run_all(dst_tasks, self.tasks)
            if all([task.state == SUCCESS for task in dst_tasks]):
                self.transition(SUCCESS)
            else:
                self.transition(FAILED)
        except Exception as e:
            traceback.print_exc()
            self.error("failing due to an exception: " + str(e))
            self.transition(FAILED)

    def persist_run_args(self, kw, final_tasks):
        db.db.connection().ping(True)
        self.db.args = json.dumps(kw)
        self.db.final_tasks = json.dumps(final_tasks)
        self.db.save

    def incomplete_run_exists(self, runner):
        if runner.has_pid:
            if runner.pid_process_exists:
                raise Exception("A process with pid %s exists." % runner.pid)

            action = self.strategy[self.state]
            if action in (RESUME, RESTART):
                runner.write_pid()
            elif action == SKIP:
                runner.delete_pid()
                return True
        else:
            runner.write_pid()

    def transition(self, new_state):
        if new_state == self.state: return
        self.info("%s => %s" % (self.state, new_state))
        assert new_state in Pipeline.TRANSITIONS[self.state]

        self.set_state(new_state)

        if new_state in {FAILED, SUCCESS}:
            if not self.skip_checks:
                if self.some_task_failed():
                    self.set_state(FAILED)

    def some_task_failed(self):
        failed = False
        for t in db.TaskRun.select().where(db.TaskRun.pipeline_run_id == self.run_id):
            if t.state not in {NOT_STARTED, FAILED, SUCCESS}:
                failed = True
                self.error("can't be finalized because task %s is %s" %
                    (t.task_name, t.state))
        return failed

    @property
    def state(self):
        return self.db.state

    def set_state(self, new_state):
        db.db.connection().ping(True)
        self.db.state = new_state
        self.db.save()

    def error(self, msg):
        self.logger.error(msg, extra={"task": ""}, exc_info=True)

    def info(self, msg):
        self.logger.info(msg, extra={"task": ""})

    def debug(self, msg):
        self.logger.debug(msg, extra={"task": ""})

    @lazy
    def logger(self):
        return logging.getLogger("pipeline")

    @staticmethod
    def run_unfinished(max_retries, tasks_module):
        from runner import DirectRunner
        from filenames import fn_formatter # user's fn_formatter
        from logs import setup_logger

        logger = setup_logger("rerunner", "%s/_logs/rerunner.txt" % os.environ["PREFIX"])
        num_rerun = 0

        for p in db.Pipeline.select():
            pruns = db.PipelineRun.select()
            pruns = pruns.where(db.PipelineRun.pipeline_id == p.id and db.PipelineRun.scheduled)
            pruns = pruns.where((db.PipelineRun.state == RUNNING) or (db.PipelineRun.state == FAILED))
            pruns = pruns.where(db.PipelineRun.retries <= max_retries)
            pruns = pruns.order_by(db.PipelineRun.start_time.desc())

            for pr in pruns.limit(1):
                num_rerun += 1
                ppl = Pipeline(p.id, pr.run_id)
                dst_task_ids = json.loads(ppl.final_tasks)
                kw = json.loads(ppl.args)
                logger.info(
                    "re-run %s (%s) with args %s" %
                    (pr.run_id, ", ".join(dst_task_ids), ppl.args))

                runner = DirectRunner(tasks_module, kw, fn_formatter(kw))
                ppl.run(dst_task_ids, runner)

        logger.info("re-ran %i pipelines" % num_rerun)
