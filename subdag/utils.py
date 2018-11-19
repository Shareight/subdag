from tensorflow.python.lib.io import file_io as io
import logging
import json
import collections
import dask.bag as db
import pickle

def lazy(fn):
    # Decorator that makes a property lazy-evaluated.
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

def read(fn, lazy=True):
    log("reading from %s %s" % (fn, "lazily" if lazy else "eagerly"))
    if lazy:
        ext = fn.split(".")[-1].lower()
        from_csv = lambda x: x.strip().split(",")
        func = from_csv if ext == "csv" else json.loads
        return db.read_text(fn).map(func)
    else:
        with io.FileIO(fn, "r") as f:
            return [l.strip() for l in f.readlines()]

def write(fn, data):
    if type(data) == dict:
        write_dict(fn, data)
    elif isinstance(data, db.core.Bag):
        def to_csv(x):
            if type(x) == str:
                return x
            else:
                return ",".join(map(str, x))

        ext = fn.split(".")[-1].lower()
        func = to_csv if ext == "csv" else json.dumps
        write_lines(fn, data.map(func).compute())
    elif isinstance(data, collections.Iterable):
        write_lines(fn, data)
    else:
        raise Exception("don't know how to write " + str(type(data)))

def write_dict(fn, d):
    log("writing %i key/value pairs to to %s" % (len(d.keys()), fn))
    with io.FileIO(fn, "w") as f:
        f.write(json.dumps(d))
        f.write("") # create empty file if no data

def write_lines(fn, lines=[""]):
    log("writing lines to %s" % (fn))
    with io.FileIO(fn, "w") as f:
        f.write("") # create empty file if no data
        for i, l in enumerate(lines):
            l = json.dumps(l) if type(l) == dict else l
            if i > 0:
                f.write("\n")
            f.write(l)

def merge(*dicts):
    ret = {}
    for d in dicts:
        for k, v in d.items():
            if v is None:
                if k not in ret:
                    ret[k] = None
            else:
                ret[k] = v
        ret.update(d)
    return ret

def load_pickle(fn):
    with io.FileIO(fn, "rb") as f:
        return pickle.load(f)

def log(s, level="info"):
    logger = logging.getLogger("system")
    getattr(logger, level)(s)

class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
