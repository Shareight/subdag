import logging
import os
from tensorflow.python.lib.io import file_io as io
from tensorflow.python.platform import tf_logging

default_formatter = logging.Formatter('%(asctime)s - %(message)s')

class TFFilter(logging.Filter):

    def filter(self, record):
        return not record.msg.startswith("Initialize variable")

class Loggers(object):

    latest_nr = 0
    loggers = {}

    def __init__(self, run_id):
        if run_id is None:
            raise Exception("pass run_id with -r")
        self.run_id = run_id
        self.set_latest_nr()
        self.maybe_create_dir()
        self.setup_pipeline_logger()

    def set_latest_nr(self):
        logs_dir = '{prefix}/runs/{run_id}/_logs/'.format(
            prefix=os.environ["PREFIX"],
            run_id=self.run_id,
        )

        if io.is_directory(logs_dir):
            nrs = [int(fn) for fn in io.list_directory(logs_dir) if fn.isdigit()]
            self.latest_nr = sorted(nrs)[-1] + 1 if nrs else 1
        else:
            self.latest_nr = 1

    def maybe_create_dir(self):
        d = "{prefix}/runs/{run_id}/_logs/{nr}".format(
            prefix=os.environ["PREFIX"],
            run_id=self.run_id,
            nr=self.latest_nr,
        )
        io.recursive_create_dir(d)

    def loc(self, task_id):
        return '{prefix}/runs/{run_id}/_logs/{nr}/{task_id}.txt'.format(
            prefix=os.environ["PREFIX"],
            run_id=self.run_id,
            nr=self.latest_nr,
            task_id=task_id,
        )

    def setup_pipeline_logger(self):
        if "pipeline" in self.loggers: return
        formatter1 = logging.Formatter('%(asctime)s - ' + self.run_id + ' %(task)s - %(message)s')
        formatter2 = logging.Formatter('%(asctime)s -     %(message)s')
        self.loggers["pipeline"] = setup_logger("pipeline", self.loc("pipeline"), formatter1)
        self.loggers["system"] = setup_logger("system", self.loc("pipeline"), formatter2)

    def setup_task_logger(self, task_name):
        if task_name in self.loggers: return
        self.loggers[task_name] = setup_logger(task_name, self.loc(task_name))

    def wrap_tf_logger(self, task_name):
        tf_logger = tf_logging._get_logger()
        formatter = logging.Formatter('%(asctime)s - %(message)s')

        fh = logging.FileHandler(self.loc(task_name))
        ch = logging.StreamHandler()
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        tf_logger.handlers = [fh, ch]
        tf_logger.addFilter(TFFilter())
        tf_logger.propagate = False

def setup_logger(id, loc, formatter=default_formatter):
    logger = logging.getLogger(id)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fh = logging.FileHandler(loc)
    ch = logging.StreamHandler()

    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.handlers = [fh, ch]
    return logger
