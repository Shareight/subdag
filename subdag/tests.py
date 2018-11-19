import pytest
import os
from copy import deepcopy
os.environ["DB_NAME"] = "dags_test" # TODO
from tensorflow.python.lib.io import file_io as io
from collections import defaultdict

import db
from subdag.pipeline import Pipeline
from subdag.runner import SequentialRunner, DirectRunner, TaskRunner, DEFAULT_STRATEGY
from subdag.constants import FAILED, WAITING, SUCCESS, RUNNING, NOT_STARTED, UPSTREAM_FAILED
from subdag.constants import RESTART, CLEAN_AND_RESTART, SKIP, FAIL, MARK_SUCCESS
from subdag.fn_formatter import fn_formatter
import subdag.artifacts as a
import subdag.decorators as decorators
import subdag.utils as u
import dask.bag as bag

PREFIX = "/dags_test"
PIPELINE_NAME = "test_pipeline"
RUN_ID = "test_run_id"
TASK_NAME = "test_task_name"

def star(p):
    @decorators.task(PIPELINE_NAME)
    @decorators.outputs(a.File("f1"))
    def star_t1(*inputs, **kw):
        return {"f1": "contnents"}

    @decorators.task(PIPELINE_NAME)
    @decorators.outputs(a.File("f2"))
    def star_t2(*inputs, **kw):
        return {"f2": "contnents"}

    @decorators.task(PIPELINE_NAME)
    @decorators.outputs(a.File("f3"))
    def star_t3(*inputs, **kw):
        return {"f3": "contnents"}

    @decorators.task(PIPELINE_NAME)
    def star_t4(*inputs, **kw):
        pass

    @decorators.task(PIPELINE_NAME)
    def star_t5(*inputs, **kw):
        pass

    t1 = p.task("star_t1")
    t2 = p.task("star_t2")
    t3 = p.task("star_t3", inputs=[t1.out, t2.out])
    t4 = p.task("star_t4", inputs=[t3.out])
    t5 = p.task("star_t5", inputs=[t3.out])
    return t1.out, t2.out, t3.out, t1, t2, t3, t4, t5

def star_nofiles(p):
    @decorators.task(PIPELINE_NAME)
    def star_nf_t1(*inputs, **kw):
        pass

    @decorators.task(PIPELINE_NAME)
    def star_nf_t2(*inputs, **kw):
        pass

    @decorators.task(PIPELINE_NAME)
    def star_nf_t3(*inputs, **kw):
        pass

    @decorators.task(PIPELINE_NAME)
    def star_nf_t4(*inputs, **kw):
        pass

    @decorators.task(PIPELINE_NAME)
    def star_nf_t5(*inputs, **kw):
        pass

    t1 = p.task("star_nf_t1")
    t2 = p.task("star_nf_t2")
    t3 = p.task("star_nf_t3", deps=[t1, t2])
    t4 = p.task("star_nf_t4", deps=[t3])
    t5 = p.task("star_nf_t5", deps=[t3])
    return t1, t2, t3, t4, t5

def assert_star(t1, t2, t3, t4, t5):
    assert set(t1.immediate_upstream) == set([])
    assert set(t2.immediate_upstream) == set([])
    assert set(t3.immediate_upstream) == {t1, t2}
    assert set(t4.immediate_upstream) == {t3}
    assert set(t5.immediate_upstream) == {t3}

    assert set(t1.immediate_downstream) == set([])
    assert set(t2.immediate_downstream) == set([])
    assert set(t3.immediate_downstream) == set([])
    assert set(t4.immediate_downstream) == set([])
    assert set(t5.immediate_downstream) == set([])

    t4.recursive_set_downstream()
    assert set(t1.immediate_downstream) == {t3}
    assert set(t2.immediate_downstream) == {t3}
    assert set(t3.immediate_downstream) == {t4} # not t5 because that's not in dst_tasks
    assert set(t4.immediate_downstream) == set([])
    assert set(t5.immediate_downstream) == set([])

    t5.recursive_set_downstream()
    assert set(t1.immediate_downstream) == {t3}
    assert set(t2.immediate_downstream) == {t3}
    assert set(t3.immediate_downstream) == {t4, t5}
    assert set(t4.immediate_downstream) == set([])
    assert set(t5.immediate_downstream) == set([])

def assert_actions(from_state, action_to_state, t, runner_with):
    for action, desired_end_state in action_to_state.items():
        t.set_state(from_state)
        runner_with(from_state, action).prepare(t)
        assert t.state == desired_end_state

def ff(prefix):
    kw = {"prefix": str(prefix)}
    fns = {
        "tmp_loc": "{prefix}/tmp_loc.csv",
        "tmp_dir": "{prefix}/",
        "tmp_chunk": "{prefix}/*.json",
        "datasets": "{prefix}/datasets/",
        "dataset": "{prefix}/datasets/{ds}/",
        "ds_file": "{prefix}/datasets/{ds}/{split}/*.json",
        "es_dump": "{prefix}/es_dump/{idx}/",
        "es_dump_mapping": "{prefix}/es_dump/{idx}/mapping.json",
        "es_dump_data_file": "{prefix}/es_dump/{idx}/data.json",
        "es_dump_data_chunked": "{prefix}/es_dump/{idx}/data/*.json",

    }
    return fn_formatter(kw, fns)


class MockRunner(SequentialRunner):

    def run(self, task):
        return True

class CountRunner(SequentialRunner):
    def __init__(self, kw, ff):
        super(CountRunner, self).__init__(kw, ff)
        self.times_run = 0
        self.tasks_run = defaultdict(lambda: 0)

    def run(self, task):
        self.times_run += 1
        self.tasks_run[task.name] += 1
        print(self.tasks_run)
        return True

class CountPersistedArtifact(a.File):
    def __init__(self, *args, **kw):
        super(CountPersistedArtifact, self).__init__(*args, **kw)
        self.times_run = 0

    def persist(self, output):
        self.times_run += 1

class DBTest(object):

    @pytest.fixture(autouse=True)
    def db_setup_teardown(self):
        db.setup()
        yield
        db.teardown()

    @pytest.fixture
    def pipeline_def(self):
        @decorators.pipeline()
        def test_pipeline(*args, **kw):
            pass

    @pytest.fixture()
    def p(self, db_setup_teardown, pipeline_def):
        return Pipeline(PIPELINE_NAME, RUN_ID)

    @pytest.fixture()
    def p_nochecks(self, db_setup_teardown, pipeline_def):
        return Pipeline(PIPELINE_NAME, RUN_ID, skip_checks=True)

    @pytest.fixture
    def fns(self):
        return {
            "f1": "{prefix}/runs/{run_id}/f1.csv",
            "f2": "{prefix}/runs/{run_id}/f2.csv",
            "f3": "{prefix}/runs/{run_id}/f3.csv",
            "ex": "{prefix}/runs/{run_id}/ex.csv",
            "ne": "{prefix}/runs/{run_id}/ne.csv",

            "cats_mapping": "{prefix}/runs/{run_id}/categories/mapping.json",
            "cats_data": "{prefix}/runs/{run_id}/categories/data.data",
            # "prods_mapping": "{prefix}/runs/{run_id}/products/mapping.json",
            # "prods_data": "{prefix}/runs/{run_id}/products/data/*.json",
            # "products": "{prefix}/runs/{run_id}/products/clean/*.json",
            # "counts": "{prefix}/runs/{run_id}/counts/",
            # "vocabs": "{prefix}/runs/{run_id}/vocabs/",
            # "download_imgs_succ": "{prefix}/runs/{run_id}/download_imgs/ids_succ/*.csv",
            # "download_imgs_fail": "{prefix}/runs/{run_id}/download_imgs/ids_fail/*.csv",
            # "extract_embs_succ": "{prefix}/runs/{run_id}/extract_embs/ids_succ/*.csv",
            # "extract_embs_fail": "{prefix}/runs/{run_id}/extract_embs/ids_fail/*.csv",
            # "check": "{prefix}/runs/{run_id}/check/{hparams_id}/",
            # "check_detailed": "{prefix}/runs/{run_id}/check_detailed/{hparams_id}/",
            # "model": "{prefix}/models/{model_id}/",
            # "preds": "{prefix}/runs/{run_id}/predictions/{model_id}/*.pickle",
            # "dataset": "{prefix}/runs/{run_id}/datasets/{ds}/",
            # "ds_file": "{prefix}/runs/{run_id}/datasets/{ds}/{split}/*.json",
        }

    @pytest.fixture
    def kw(self):
        return {"prefix": PREFIX}

    @pytest.fixture
    def ff(self, kw, fns):
        return fn_formatter(kw, fns, run_id=RUN_ID)

    @pytest.fixture
    def mock_runner(self, fns, kw, ff):
        return MockRunner({}, ff)

    @pytest.fixture
    def sequential_runner(self, fns, kw, ff):
        return SequentialRunner({}, ff)

    @pytest.fixture
    def count_runner(self, fns, kw, ff):
        return CountRunner({}, ff)

    @pytest.fixture
    def runner_with(self, fns, kw):
        def func(key, val, cls=SequentialRunner):
            strategy = deepcopy(DEFAULT_STRATEGY)
            strategy[key] = val
            return cls({}, fn_formatter(kw, fns, run_id=RUN_ID), strategy=strategy)

        return func


class TestFormatter():

    def test_formatter1(self):
        fns = {
            "cats_mapping": "{prefix}/runs/{run_id}/categories/mapping.json",
            "check": "{prefix}/runs/{run_id}/check/{hparams_id}/",
        }
        cli_args = {
            "prefix": "/data",
            "run_id": None,
        }
        f = fn_formatter(cli_args, fns, run_id="pipeline_run_id")

        assert f("cats_mapping") == "/data/runs/pipeline_run_id/categories/mapping.json"
        assert f("cats_mapping", run_id="rid_override") == "/data/runs/rid_override/categories/mapping.json"
        with pytest.raises(Exception):
            f("check")

    def test_formatter2(self):
        fns = {
            "check": "{prefix}/runs/{run_id}/check/{hparams_id}/",
        }

        cli_args = {
            "prefix": "/data",
            "run_id": None,
            "model_type": "deep",
            "input_type": "full_emb",
        }
        f = fn_formatter(cli_args, fns, run_id="pipeline_run_id")
        assert f("check") == "/data/runs/pipeline_run_id/check/deep_full_emb/"

    def test_formatter3(self):
        fns = {
            "check": "{prefix}/runs/{run_id}/check/{hparams_id}/",
        }

        cli_args = {
            "prefix": "/data",
            "run_id": None,
            "model_type": "deep",
            "input_type": "full_emb",
        }
        f = fn_formatter(cli_args, fns, run_id="pipeline_run_id", hparams_id="hparams_id_override")
        assert f("check") == "/data/runs/pipeline_run_id/check/hparams_id_override/"

class TestPipeline(DBTest):

    @pytest.fixture
    def dump_cats_def(self, pipeline_def):
        @decorators.task(PIPELINE_NAME)
        @decorators.outputs(
            a.File("cats_mapping"),
            a.File("cats_data"),
        )
        def dump_cats(*inputs, **kw):
            return {
                "cat_mapping": ["l1", "l2", "l3"],
                "cat_data": ["l1", "l2", "l3"],
            }

    def test_created(self):
        assert db.Pipeline.get_or_none(id=PIPELINE_NAME) is None
        Pipeline(PIPELINE_NAME, RUN_ID)

        db_p = db.Pipeline.get_or_none(id=PIPELINE_NAME)
        assert db_p is not None
        assert db_p.id == PIPELINE_NAME

    def test_success_with_empty_tasks(self, fns, mock_runner):
        p = Pipeline(PIPELINE_NAME, RUN_ID)

        db_pr = db.PipelineRun.get_or_none(run_id=RUN_ID)
        assert db_pr.state == NOT_STARTED

        p.run([], mock_runner)
        db_pr = db.PipelineRun.get_or_none(run_id=RUN_ID)
        assert db_pr.state == SUCCESS

    def test_task_created(self, dump_cats_def, p):
        p.task("dump_cats")
        db_t = db.TaskRun.get_or_none(pipeline_run_id=RUN_ID, task_name="dump_cats")
        assert db_t.state == NOT_STARTED

    def test_task_not_created_without_task_def(self):
        p = Pipeline(PIPELINE_NAME, RUN_ID)

        with pytest.raises(Exception):
            undefined_task, _ = p.task("undefined_task")

    def test_multiple_tasks_not_created_with_same_args1(self, dump_cats_def):
        p = Pipeline(PIPELINE_NAME, RUN_ID)
        t1 = p.task("dump_cats")
        t2 = p.task("dump_cats")
        assert t1.db == t2.db

    def test_multiple_tasks_not_created_with_same_args2(self, dump_cats_def):
        p = Pipeline(PIPELINE_NAME, RUN_ID)
        t1 = p.task("dump_cats", arg1="val1")
        t2 = p.task("dump_cats", arg1="val1")
        assert t1.db == t2.db

    def test_multiple_tasks_created_with_diff_args1(self, dump_cats_def):
        p = Pipeline(PIPELINE_NAME, RUN_ID)
        t1 = p.task("dump_cats", arg1="val1")
        t2 = p.task("dump_cats", arg1="othervalue")
        assert t1.db != t2.db

    def test_multiple_tasks_created_with_diff_args2(self, dump_cats_def):
        p = Pipeline(PIPELINE_NAME, RUN_ID)
        t1 = p.task("dump_cats", arg1="value")
        t2 = p.task("dump_cats", diffarg="value")
        assert t1.db != t2.db

    def test_transition(self, dump_cats_def):
        p = Pipeline(PIPELINE_NAME, RUN_ID)
        t = p.task("dump_cats")
        p.transition(RUNNING)

        with pytest.raises(Exception):
            t.transition(RUNNING)

        t.transition(WAITING)
        t.transition(RUNNING)
        t.transition(SUCCESS)

        p.transition(SUCCESS)
        assert True

    def test_prepare_tasks(self, sequential_runner):
        p = Pipeline(PIPELINE_NAME, RUN_ID, skip_checks=True)
        f1, f2, f3, t1, t2, t3, t4, t5 = star(p)
        sequential_runner.prepare_tasks([t4])
        assert all(t.state == WAITING for t in [t1, t2, t3, t4])
        assert t5.state == NOT_STARTED

class TestTask(DBTest):

    def test_upstream_downstream1(self):
        p = Pipeline(PIPELINE_NAME, RUN_ID)
        f1, f2, f3, t1, t2, t3, t4, t5 = star(p)
        assert_star(t1, t2, t3, t4, t5)

    def test_upstream_downstream2(self, p):
        t1, t2, t3, t4, t5 = star_nofiles(p)
        assert_star(t1, t2, t3, t4, t5)

    def test_maybe_fail(self, p, count_runner):
        class Nonexistent(a.Artifact):
            def exists(self): return False
            def _validate(self): return True # noqa
            def create_dirs(self): pass # noqa

        class Existent(a.Artifact):
            def exists(self): return True
            def _validate(self): return True # noqa
            def create_dirs(self): pass # noqa

        @decorators.task(PIPELINE_NAME)
        @decorators.outputs(Nonexistent("ne"))
        def mf_t1(*inputs, **kw):
            return {"ne": "contents"}

        @decorators.task(PIPELINE_NAME)
        @decorators.outputs(Existent("ex"))
        def mf_t2(*inputs, **kw):
            return {"ex": "contents"}

        @decorators.task(PIPELINE_NAME)
        def mf_t3(*inputs, **kw):
            pass

        @decorators.task(PIPELINE_NAME)
        def mf_t4(*inputs, **kw):
            pass

        t1 = p.task("mf_t1")
        t2 = p.task("mf_t2")
        t3 = p.task("mf_t3", inputs=[t1.out])
        t4 = p.task("mf_t4", inputs=[t2.out])

        count_runner.prepare_tasks([t3, t4])

        assert t3.state != FAILED
        assert t4.state != FAILED

        t3.run(count_runner)
        assert t3.state == UPSTREAM_FAILED
        assert count_runner.tasks_run["mf_t1"] == 1
        assert count_runner.tasks_run.get("mf_t3", 0) == 0 # precond fail

        t4.run(count_runner)
        assert t4.state == UPSTREAM_FAILED
        assert count_runner.tasks_run["mf_t2"] == 0 # precond failed
        assert count_runner.tasks_run["mf_t4"] == 0 # upstream failed

    def test_upstream_ready1(self, p):
        t1, t2, t3, t4, t5 = star_nofiles(p)

        assert t1.upstream_ready()
        assert t2.upstream_ready()
        assert not t3.upstream_ready()
        assert not t4.upstream_ready()
        assert not t5.upstream_ready()

        t1.set_state(SUCCESS)
        assert not t3.upstream_ready()
        assert not t4.upstream_ready()
        assert not t5.upstream_ready()

        t2.set_state(SUCCESS)
        assert t3.upstream_ready()
        assert not t4.upstream_ready()
        assert not t5.upstream_ready()

        t3.set_state(SUCCESS)
        assert t3.upstream_ready()
        assert t4.upstream_ready()
        assert t5.upstream_ready()

    def test_upstream_ready2(self, p):
        t1, t2, t3, t4, t5 = star_nofiles(p)

        t1.set_state(SUCCESS)
        t2.set_state(FAILED)
        t3.set_state(WAITING)
        t4.set_state(WAITING)
        t5.set_state(WAITING)
        assert not t3.upstream_ready()
        assert not t4.upstream_ready()
        assert not t5.upstream_ready()

    def test_upstream_ready3(self, p):
        t1, t2, t3, t4, t5 = star_nofiles(p)

        t1.set_state(SUCCESS)
        t2.set_state(SUCCESS)
        t3.set_state(SUCCESS)
        t4.set_state(FAILED)
        assert t5.upstream_ready()

    def test_upstream_failed(self, p):
        t1, t2, t3, t4, t5 = star_nofiles(p)
        t1.set_state(SUCCESS)
        t2.set_state(RUNNING)
        t3.set_state(WAITING)
        t4.set_state(WAITING)
        t5.set_state(WAITING)
        t4.recursive_set_downstream()
        t5.recursive_set_downstream()

        t2.transition(FAILED)
        assert t3.state == UPSTREAM_FAILED
        assert t4.state == UPSTREAM_FAILED
        assert t5.state == UPSTREAM_FAILED

    def test_run_success1(self, p, count_runner):
        t1, t2, t3, t4, t5 = star_nofiles(p)
        count_runner.prepare_tasks([t4, t5])

        t4.run(count_runner)
        assert count_runner.times_run == 5

    def test_run_success2(self, p, count_runner):
        t1, t2, t3, t4, t5 = star_nofiles(p)
        count_runner.prepare_tasks([t4])

        t4.run(count_runner)
        assert count_runner.times_run == 4

    def test_run_same_task_diff_params(self, p, count_runner):
            @decorators.task(PIPELINE_NAME)
            def rstdp_t1(*inputs, **kw):
                pass

            @decorators.task(PIPELINE_NAME)
            def rstdp_t2(*inputs, **kw):
                pass

            t1 = p.task("rstdp_t1", arg1="value")
            t2 = p.task("rstdp_t1", arg1="diff_value")
            t3 = p.task("rstdp_t2", deps=[t1, t2])

            count_runner.prepare_tasks([t3])
            t3.run(count_runner)

            assert count_runner.times_run == 3

    def test_run_fail1(self, p, count_runner):
        t1, t2, t3, t4, t5 = star_nofiles(p)
        count_runner.prepare_tasks([t4])

        setattr(t3, "_lazy_failed_preconditions", ["intentional_fail"])
        t4.run(count_runner)

        assert count_runner.times_run == 2
        assert t1.state == SUCCESS
        assert t2.state == SUCCESS
        assert t3.state == FAILED
        assert t4.state == UPSTREAM_FAILED
        assert t5.state == NOT_STARTED

    def test_run_downstream_only_once(self, p, count_runner):
        t1, t2, t3, t4, t5 = star_nofiles(p)
        all_tasks = {t.name: t for t in [t1, t2, t3, t4, t5]}
        count_runner.prepare_tasks([t4, t5])
        count_runner.run_all([t4, t5], all_tasks)
        assert count_runner.times_run == 5


class TestTaskRunner(DBTest):

    @pytest.fixture
    def r_nocache(self, ff):
        return TaskRunner({"persist": False}, ff)

    @pytest.fixture
    def ma1(self):
        class M1(CountPersistedArtifact):
            def persist(self, o):
                assert o == 1
                self.times_run += 1
        return M1("tmp_loc1")

    @pytest.fixture
    def ma2(self):
        class M2(CountPersistedArtifact):
            def persist(self, o):
                assert o == 2
                self.times_run += 1
        return M2("tmp_loc2")

    def test_maybe_persist_none(self, p):
        @decorators.task(PIPELINE_NAME)
        def maybe_persist_none(*args, **kwargs):
            pass

        t = p.task("maybe_persist_none")
        t.maybe_persist(None)

        with pytest.raises(Exception):
            t.maybe_persist("not None")
        assert t.out is None

    def test_maybe_persist_one(self, p):
        ma = CountPersistedArtifact("tmp_loc")
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma)
        def maybe_persist_one(*args, **kwargs):
            return "str"

        t = p.task("maybe_persist_one")
        t.maybe_persist("str")

        assert ma.times_run == 1
        assert isinstance(t.out, a.File)

    def test_maybe_persist_many_dict(self, p, ma1, ma2):
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma1, ma2)
        def maybe_persist_many_dict(*args, **kwargs):
            return ret

        ret = {"tmp_loc1": 1, "tmp_loc2": 2}
        t = p.task("maybe_persist_many_dict")
        t.maybe_persist(ret)

        assert ma1.times_run == 1
        assert ma2.times_run == 1
        assert isinstance(t.out, dict)
        assert isinstance(t.out["tmp_loc1"], a.File)
        assert isinstance(t.out["tmp_loc2"], a.File)

    def test_maybe_persist_many_tuple(self, p, ma1, ma2):
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma1, ma2)
        def maybe_persist_many_tuple(*args, **kwargs):
            return ret

        ret = (1, 2)
        t = p.task("maybe_persist_many_tuple")
        t.maybe_persist(ret)

        assert ma1.times_run == 1
        assert ma2.times_run == 1
        assert isinstance(t.out, dict)
        assert isinstance(t.out["tmp_loc1"], a.File)
        assert isinstance(t.out["tmp_loc2"], a.File)

    def test_maybe_persist_many_object(self, p, ma1, ma2):
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma1, ma2)
        def maybe_persist_many_tuple(*args, **kwargs):
            return ret

        ret = "some_object"
        t = p.task("maybe_persist_many_tuple")

        with pytest.raises(Exception):
            t.maybe_persist(ret)

class TestSequentialRunner(DBTest):

    @pytest.fixture
    def t(self, p):
        @decorators.task(PIPELINE_NAME)
        def task(*args, **kwargs):
            pass

        return p.task("task")

    def test_prepare_not_started(self, p, t, sequential_runner):
        sequential_runner.prepare(t)
        assert t.state == WAITING

    def test_prepare_waiting(self, p, t, sequential_runner):
        t.set_state(RUNNING)
        sequential_runner.prepare(t)
        assert t.state == WAITING

    def test_prepare_running(self, p, t, runner_with):
        action_to_state = {
            FAIL: FAILED,
            MARK_SUCCESS: SUCCESS,
            RESTART: WAITING,
            CLEAN_AND_RESTART: WAITING,
        }
        assert_actions(RUNNING, action_to_state, t, runner_with)

        # ensure we'vw tested all strategies
        assert set(SequentialRunner.STRATEGIES[RUNNING]) == set(action_to_state.keys())

    def test_prepare_failed(self, p,  t, runner_with):
        action_to_state = {
            FAIL: FAILED,
            MARK_SUCCESS: SUCCESS,
            RESTART: WAITING,
            CLEAN_AND_RESTART: WAITING,
        }
        assert_actions(FAILED, action_to_state, t, runner_with)

        # ensure we'vw tested all strategies
        assert set(SequentialRunner.STRATEGIES[FAILED]) == set(action_to_state.keys())

    def test_prepare_upstream_failed(self, p, t, runner_with):
        action_to_state = {
            FAIL: FAILED,
            RESTART: WAITING,
            CLEAN_AND_RESTART: WAITING,
        }
        assert_actions(UPSTREAM_FAILED, action_to_state, t, runner_with)

        # ensure we'vw tested all strategies
        assert set(SequentialRunner.STRATEGIES[UPSTREAM_FAILED]) == set(action_to_state.keys())

    def test_prepare_success(self, p, t, runner_with):
        action_to_state = {
            SKIP: SUCCESS,
            RESTART: WAITING,
        }
        assert_actions(SUCCESS, action_to_state, t, runner_with)

        # ensure we'vw tested all strategies
        assert set(SequentialRunner.STRATEGIES[SUCCESS]) == set(action_to_state.keys())

    def test_recursive_prepare1(self, p, sequential_runner):
        f1, f2, f3, t1, t2, t3, t4, t5 = star(p)
        sequential_runner.recursive_prepare(t4)
        assert all(t.state == WAITING for t in [t1, t2, t3, t4])
        assert t5.state == NOT_STARTED

    def test_recursive_prepare2(self, p, sequential_runner):
        f1, f2, f3, t1, t2, t3, t4, t5 = star(p)
        sequential_runner.recursive_prepare(t3)
        assert all(t.state == WAITING for t in [t1, t2, t3])
        assert all(t.state == NOT_STARTED for t in [t4, t5])

class TestArtifacts(object):

    @pytest.fixture()
    def d(self):
        ret = PREFIX + "/path/to/"
        if io.is_directory(ret):
            io.delete_recursively(ret)
        return ret

    def test_file(self, tmpdir):
        tmp_loc = tmpdir.join("tmp_loc.csv")
        f = a.File("tmp_loc")
        f.set_loc(ff(tmpdir))

        assert not f.exists()

        tmp_loc.write("stuff")
        assert f.exists()

        f.clean()
        assert not f.exists()

    def test_file_dir(self, d):
        f = a.File("tmp_loc")
        f.set_loc(ff(d))

        assert not io.is_directory(d)

        f.create_dirs()
        assert not f.exists()
        assert io.is_directory(d)

    def test_files(self, tmpdir):
        f = a.Files("tmp_dir")
        f.set_loc(ff(tmpdir))

        tmpdir.mkdir("subdir")
        assert not f.exists()

        tmpdir.join("filename1.txt").write("stuff")
        tmpdir.join("filename2.txt").write("stuff")
        assert f.exists()

        f.clean()
        assert not f.exists()

    def test_files_dir(self, d):
        f = a.Files("tmp_dir")
        f.set_loc(ff(d))

        assert not f.exists()

        f.create_dirs()
        assert not f.exists()
        assert io.is_directory(d)

        f.clean()
        assert not f.exists()
        assert not io.is_directory(d)

    def test_chunked_file(self, tmpdir):
        f = a.ChunkedFile("tmp_chunk")
        f.set_loc(ff(tmpdir))

        tmpdir.mkdir("subdir")
        assert not f.exists()

        tmpdir.join("1.json").write("stuff")
        tmpdir.join("2.json").write("stuff")
        assert f.exists()

        f.clean()
        assert not f.exists()

    def test_chunked_file_dir(self, d):
        f = a.ChunkedFile("tmp_chunk")
        f.set_loc(ff(d))

        assert not f.exists()

        f.create_dirs()
        assert not f.exists()
        assert io.is_directory(d)

        f.clean()
        assert not f.exists()
        assert not io.is_directory(d)

    def test_datasets(self, tmpdir):
        dss = {
            "manual": ["train", "valid"],
            "rule_based": ["train", "valid"],
            "all": ["predict"],
        }
        datasets = a.Datasets("datasets", dss)
        datasets.set_loc(ff(tmpdir))

        assert not datasets.exists()

        datasets_dir = tmpdir.mkdir("datasets")
        for ds, splits in dsd.items():
            dsdir = datasets_dir.mkdir(ds)
            for s in splits:
                splitdir = dsdir.mkdir(s)
                splitdir.join("1.json").write("stuff")
                splitdir.join("2.json").write("stuff")

        assert datasets.exists()

    def test_dataset(self, tmpdir):
        splits = ["train", "valid"]
        ds = "manual"
        f = a.Dataset("dataset", ds=ds, splits=splits)
        f.set_loc(ff(tmpdir))

        tmpdir.mkdir("subdir")
        assert not f.exists()

        dsdir = tmpdir.mkdir("datasets").mkdir(ds)
        for s in splits:
            splitdir = dsdir.mkdir(s)
            splitdir.join("1.json").write("stuff")
            splitdir.join("2.json").write("stuff")

        assert f.exists()
        f.clean()
        assert not f.exists()

class TestArtifactPersistence(DBTest):

    def test_none(self, p, tmpdir):
        @decorators.task(PIPELINE_NAME)
        def task(*args, **kwargs):
            return impl()

        impl = lambda: None

        t = p.task("task")
        tasks_module = u.Bunch(task=u.Bunch(impl=impl))
        r = DirectRunner(tasks_module, {}, ff(tmpdir))

        r.prepare(t)
        assert r.run(t)

    def test_dict_of_lists(self, p, tmpdir):
        class MockArtifact(CountPersistedArtifact):
            def persist(self, output):
                assert output == tmp_loc_out
                self.times_run += 1

        ma = MockArtifact("tmp_loc")
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma)
        def task(*args, **kwargs):
            return impl()

        tmp_loc_out = ["line1", "line2"]
        impl = lambda: tmp_loc_out

        t = p.task("task")
        tasks_module = u.Bunch(task=u.Bunch(impl=impl))
        r = DirectRunner(tasks_module, {}, ff(tmpdir))

        r.prepare(t)
        assert r.run(t)
        assert ma.times_run == 1

    def test_object(self, p, tmpdir):
        class MockArtifact(CountPersistedArtifact):
            def persist(self, output):
                assert output == my_obj
                self.times_run += 1

        ma = MockArtifact("tmp_loc")
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma)
        def task(*args, **kwargs):
            return impl()

        my_obj = u.Bunch(some_field="some value")
        impl = lambda: my_obj

        t = p.task("task")
        tasks_module = u.Bunch(task=u.Bunch(impl=impl))
        r = DirectRunner(tasks_module, {}, ff(tmpdir))

        r.prepare(t)
        assert r.run(t)
        assert ma.times_run == 1

    def test_file_saved(self, p, tmpdir):
        ma = a.File("tmp_loc")
        @decorators.task(PIPELINE_NAME) # noqa
        @decorators.outputs(ma)
        def task(*args, **kwargs):
            return impl()

        tmp_loc_out = ["line1", "line2"]
        impl = lambda: tmp_loc_out

        t = p.task("task")
        tasks_module = u.Bunch(task=u.Bunch(impl=impl))
        r = DirectRunner(tasks_module, {}, ff(tmpdir))

        r.prepare(t)
        assert r.run(t)
        assert ma.exists()
        assert ma.load() == tmp_loc_out

    def test_chunked_file_load_from_files(self, p, tmpdir):
        ma = a.ChunkedFile("tmp_chunk")
        @decorators.task(PIPELINE_NAME) # noqa
        def task(*args, **kwargs):
            pass

        tmpdir.join("1.json").write('{"key": "val1"}\n{"key": "val2"}')
        tmpdir.join("2.json").write('{"key": "val3"}\n{"key": "val4"}')

        t = p.task("task", inputs=[ma])

        tasks_module = u.Bunch(task=u.Bunch(impl=lambda *a, **k: None))
        r = DirectRunner(tasks_module, {}, ff(tmpdir))

        r.prepare(t)
        assert r.run(t)
        assert ma.load().compute() == [
            {"key": "val1"},
            {"key": "val2"},
            {"key": "val3"},
            {"key": "val4"},
        ]

    def test_chunked_file_written_to_files(self, tmpdir):
        f = a.ChunkedFile("tmp_chunk")
        f.set_loc(ff(tmpdir))

        data = [
            {"key": "val1"},
            {"key": "val2"},
            {"key": "val3"},
            {"key": "val4"},
        ]
        f.persist(data)

        assert f.load().compute() == data

    def test_esdump_artifact_kw1(self, tmpdir):
        esd = a.ESDump("es_dump", idx="categories", chunk_data=True)
        esd.set_loc(ff(tmpdir))

        assert isinstance(esd.mapping, a.File)
        assert isinstance(esd.data, a.ChunkedFile)

        data = {
            "mapping": ["l1", "l2"],
            "data": bag.from_sequence([{"arg1": "val1"}, {"arg1": "val2"}])
        }

        esd.create_dirs()
        esd.persist(data)
        esd.load()

        assert esd.mapping.load() == data["mapping"]
        assert esd.data.load().compute() == data["data"].compute()

    def test_esdump_artifact_kw2(self, tmpdir):
        esd = a.ESDump("es_dump", idx="categories", chunk_data=False)
        esd.set_loc(ff(tmpdir))

        assert isinstance(esd.mapping, a.File)
        assert isinstance(esd.data, a.File)

        data = {
            "mapping": ["l1", "l2"],
            "data": ["l3", "l4"],
        }

        esd.create_dirs()
        esd.persist(data)
        esd.load()

        assert esd.mapping.load() == data["mapping"]
        assert esd.data.load() == data["data"]
