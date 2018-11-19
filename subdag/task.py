import traceback
import logging
import json

from subdag.utils import lazy
import subdag.db as db
from subdag.artifacts import Artifact

from constants import NOT_STARTED, WAITING, RUNNING, UPSTREAM_FAILED, FAILED, SUCCESS

class Task(object):

    STATES = [WAITING, RUNNING, UPSTREAM_FAILED, FAILED, SUCCESS] # possible states
    # Task state => possible next new state (always allow transition to same state).
    # These are legal transitions during *execution* of pipeline; during prepare we allow all transitions.
    TRANSITIONS = {
        NOT_STARTED: [WAITING],
        WAITING: [RUNNING, UPSTREAM_FAILED],
        RUNNING: [FAILED, SUCCESS],
        FAILED: [],
        SUCCESS: [],
        UPSTREAM_FAILED: [],
    }

    def __init__(
        self, name, run_id,
        inputs=[], outputs=[], deps=[], **extra_kwargs
    ):
        self.name = name
        self.run_id = run_id
        self.inputs = inputs
        self.outputs = outputs
        self.deps = deps
        self.extra_kwargs = extra_kwargs
        self.immediate_downstream = set()

        for a in inputs:
            assert isinstance(a, Artifact) or a is None, (name, a)

        for a in outputs:
            assert isinstance(a, Artifact), (name, a)
            a.set_created_by(self)

        self.db = db.TaskRun.get_or_create(
            task_name=self.name,
            pipeline_run_id=self.run_id,
            extra_kwargs=json.dumps(extra_kwargs),
        )[0]

    def __repr__(self):
        return self.name

    def run(self, runner):
        if self.upstream_ready():
            try:
                self._run(runner)
            except Exception as e:
                self.error("failing due to an exception: " + str(e))
                traceback.print_exc()
                self.transition(FAILED)
        else:
            for task in self.immediate_upstream:
                self.debug("^^ run upstream %s ^^" % task)
                task.run(runner)

    def _run(self, runner):
        if self.maybe_skip(): return
        if self.maybe_fail("pre"): return

        self.transition(RUNNING)
        if runner.run(self):
            if not self.maybe_fail("post"):
                self.transition(SUCCESS)
                for t in self.immediate_downstream:
                    if t.state == WAITING:
                        self.debug("<< start running downstream %s <<" % t)
                        t.run(runner)
                        self.debug("-- finish running downstream %s --" % t)
                    else:
                        self.debug("downstream %s already running")
        else:
            self.error("failed by runner")
            self.transition(FAILED)

    def maybe_skip(self):
        if self.state != WAITING:
            self.debug("not running %s since already ran" % self)
            return True

    def maybe_fail(self, prepost):
        conds = getattr(self, "failed_%sconditions" % prepost)
        if conds:
            for e in conds:
                self.error("failed %scondition: %s" % (prepost, e))
            self.transition(FAILED, checks=False)
            return True

    def upstream_ready(self):
        for dep in self.immediate_upstream:
            if dep.state in {FAILED, UPSTREAM_FAILED}:
                self.transition(UPSTREAM_FAILED)
                return False
            elif dep.state in {NOT_STARTED, WAITING, RUNNING}:
                return False
            elif dep.state == SUCCESS:
                pass
            else:
                raise Exception("forgot to handle %s" % dep.state)
        return True

    @lazy
    def failed_preconditions(self):
        return Artifact.nonexistent(self.inputs, "pre") + \
            Artifact.existing(self.outputs, "pre")

    @lazy
    def failed_postconditions(self):
        return Artifact.nonexistent(self.outputs, "post")

    def create_dirs(self):
        for a in self.outputs: a.create_dirs()

    def clean(self):
        for a in self.outputs: a.clean()

    def transition(self, new_state, checks=True):
        if new_state == self.state: return
        self.info("%s => %s" % (self.state, new_state))
        if checks:
            assert new_state in Task.TRANSITIONS[self.state], \
                ("%s => %s" % (self.state, new_state),
                 new_state + " not in",
                 Task.TRANSITIONS[self.state])

        self.set_state(new_state)

        if new_state in {FAILED, UPSTREAM_FAILED}:
            for t in self.immediate_downstream:
                t.transition(UPSTREAM_FAILED)

    @lazy
    def immediate_upstream(self):
        return self.deps + self.input_deps

    @lazy
    def ancestors(self):
        ret = set()
        for us in self.immediate_upstream:
            ret.add(us)
            for a in us.ancestors:
                ret.add(a)
        return ret

    @lazy
    def input_deps(self):
        return [a.created_by for a in self.inputs if a.created_by]

    def recursive_set_downstream(self):
        for us in self.immediate_upstream:
            us.immediate_downstream.add(self)
            us.recursive_set_downstream()

    def load_inputs(self):
        return {a.fn_id: a.load() for a in self.inputs}

    def maybe_persist(self, outputs):
        if len(self.outputs) == 0:
            assert outputs is None, outputs
        elif len(self.outputs) == 1:
            a = self.outputs[0]
            if a.autopersist:
                if type(outputs) == dict and a.fn_id in outputs:
                    a.persist(outputs[a.fn_id])
                else:
                    a.persist(outputs)
        else:
            if type(outputs) == dict:
                for o_artifact in self.outputs:
                    o_artifact.persist(outputs[o_artifact.fn_id])
            elif type(outputs) == tuple:
                assert len(outputs) == len(self.outputs)
                for o_artifact, o_ret in zip(self.outputs, outputs):
                    o_artifact.persist(o_ret)
            else:
                raise Exception(
                    "task %s has %i output artifacts but impl returned a %s" %
                    (self.name, len(self.outputs), type(outputs))
                )

    @property
    def state(self):
        return self.db.state

    def set_state(self, new_state):
        db.db.connection().ping(True)
        self.db.state = new_state
        self.db.save()

    def error(self, msg):
        self.logger.error(msg, extra={"task": self.name})

    def info(self, msg):
        self.logger.info(msg, extra={"task": self.name})

    def debug(self, msg):
        self.logger.debug(msg, extra={"task": self.name})

    @lazy
    def logger(self):
        return logging.getLogger("pipeline")
