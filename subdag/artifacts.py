import logging
from subdag.utils import lazy
import subdag.utils as u
from tensorflow.python.lib.io import file_io as io
import dask.bag as db
import dask.dataframe as df
import collections
from glob import glob
import json

class Artifact(object):

    def __init__(self, fn_id, autopersist=True, preconds=True, postconds=True, **kw):
        self.fn_id = fn_id
        self.artifact_kw = kw
        self.loc = None
        self.created_by = None
        self.autopersist = autopersist
        self.preconds = preconds
        self.postconds = postconds

    def set_loc(self, ff, **kw):
        self.loc = ff(self.fn_id, **u.merge(kw, self.artifact_kw))
        self._prepare()
        self._validate()

    def exists(self):
        raise Exception("not implemented")

    def clean(self):
        raise Exception("not implemented")

    @staticmethod
    def existing(artifacts, prepost=None, neg=False):
        ret = []
        for a in artifacts:
            if prepost and not getattr(a, prepost + "conds"): continue
            if not a.exists() if neg else a.exists():
                does_not = "does not " if neg else ""
                ret.append("artifact %sexist: %s" % (does_not, a.loc))
        return ret

    @staticmethod
    def nonexistent(artifacts, prepost=None):
        return Artifact.existing(artifacts, prepost, neg=True)

    def _prepare(self):
        pass

    def set_created_by(self, task):
        self.created_by = task

    def __repr__(self):
        return "Artifact(%s)" % self.fn_id

    def clean_dir(self, dir):
        if io.is_directory(dir):
            io.delete_recursively(dir)

class File(Artifact):

    def _prepare(self):
        s = self.loc.split(".")
        if len(s) == 2:
            self.ext = s[1]
        else:
            self.ext = None

    def _validate(self):
        assert self.loc[-1] != "/", self.loc
        assert self.ext is not None, self.loc

    def exists(self):
        ret = io.file_exists(self.loc)
        if not ret:
            u.log(self.loc + " does not exist", level="warn")
        return ret

    def create_dirs(self):
        io.recursive_create_dir(self.dir)

    def clean(self):
        io.delete_file(self.loc)

    @lazy
    def dir(self):
        return "/".join(self.loc.split("/")[:-1])

    def load(self):
        return u.read(self.loc)

    def persist(self, output):
        if output is None: return # user handles their own artifacts
        u.write(self.loc, output)

class Dir(Artifact):

    def __init__(self, fn_id, pattern="*", **kw):
        super(Dir, self).__init__(fn_id, **kw)
        self.pattern = pattern

    def _validate(self):
        assert self.loc[-1] == "/", self.loc

    def create_dirs(self): pass

    def persist(self, o): pass

    def load(self):
        return self.loc

    def exists(self):
        num_matches = len(glob(self.loc + self.pattern))
        return num_matches > 0

    def clean(self):
        self.clean_dir(self.dir)

    @lazy
    def dir(self):
        return "/".join(self.loc.split("/")[:-1])

class Files(Artifact):

    def __init__(self, fn_id, files, **kw):
        super(Files, self).__init__(fn_id, **kw)
        self.files = files

    def set_loc(self, ff, **kw):
        self.loc = ff(self.fn_id, **self.artifact_kw)
        for f in self.files: f.set_loc(ff, **kw)
        self._validate()

    def _validate(self):
        assert self.loc[-1] == "/", self.loc

    def exists(self):
        return all(f.exists() for f in self.files)

    def create_dirs(self):
        io.recursive_create_dir(self.loc)

    def clean(self):
        self.clean_dir(self.loc)

    def load(self):
        return self.files

    def persist(self, output):
        for fo, fa in zip(output, self.files):
            fa.persist(fo)

class ChunkedFile(File):

    def __init__(self, fn_id, npartitions=10, *args, **kwargs):
        self.npartitions = npartitions
        super(ChunkedFile, self).__init__(fn_id, *args, **kwargs)

    def _validate(self):
        assert self.ext is not None, self.loc
        assert len(self.loc.split("*")) == 2

    def exists(self):
        nr = len(str(self.npartitions - 1))
        for i in range(nr + 3):
            fn = "%s/%s.%s" % (self.dir, "0" * i, self.ext)
            if io.file_exists(fn):
                return True

    def clean(self):
        self.clean_dir(self.dir)

    def load(self):
        if self.ext == "json":
            return db.read_text(self.loc, blocksize=1e7).map(json.loads)
        elif self.ext == "pickle":
            return db.from_sequence(glob(self.loc))
        else:
            raise Exception(self.ext + " ChunkedFile not supported")

    def load_and_transpose(self, fn):
        chunk = u.load_pickle(fn)
        for i in range(len(chunk.values()[0])):
            item = {"fn": fn}
            for k in chunk.keys():
                item["_".join(k)] = chunk[k][i]
            yield item

    def persist(self, output):
        assert output is not None
        if type(output) == df.core.DataFrame:
            if self.ext == "csv":
                df.to_csv(output, self.loc, index=False, encoding="utf-8")
            elif self.wxt == "json":
                df.to_json(output, self.loc, encoding="utf-8")
            else:
                raise Exception(self.ext + " not supported")
        else:
            if type(output) != db.core.Bag:
                logging.getLogger("system").warn("WARNING: converting to bag")
                assert isinstance(output, collections.Iterable)
                output = db.from_sequence(output, npartitions=self.npartitions)
            output.map(json.dumps).to_textfiles(self.loc)

class Datasets(Artifact):

    def __init__(self, fn_id, splits, **kw):
        super(Datasets, self).__init__(fn_id, **kw)
        self.splits = splits

    def set_loc(self, ff, datasets=None, datasets_ml=None, **kw):
        # TODO this is a hack; how to overcome same artifact id
        # with potentially different arg values?
        self.datasets = [
            Dataset("dataset", ds=ds, splits=self.splits[ds])
            for ds in (datasets_ml if datasets is None else datasets)
        ]
        for ds in self.datasets: ds.set_loc(ff)
        super(Datasets, self).set_loc(ff)

    def _validate(self):
        assert self.loc[-1] == "/", self.loc
        for ds in self.datasets: ds._validate()

    def exists(self):
        return all([ds.exists() for ds in self.datasets])

    def create_dirs(self):
        for ds in self.datasets: ds.create_dirs()

    def clean(self):
        self.clean_dir(self.loc)

    def load(self):
        return self.datasets

    def persist(self, output):
        for ds in self.datasets:
            ds_id = ds.artifact_kw["ds"]
            if ds_id in output:
                ds.persist(output[ds_id])

class Dataset(Artifact):

    def __init__(self, fn_id, **kw):
        super(Dataset, self).__init__(fn_id, **kw)
        self.splits = [
            ChunkedFile("ds_file", ds=kw["ds"], split=s)
            for s in kw.pop("splits")
        ]

    def set_loc(self, ff, **kw):
        for s in self.splits: s.set_loc(ff)
        super(Dataset, self).set_loc(ff)

    def _validate(self):
        assert self.loc[-1] == "/", self.loc
        for s in self.splits: s._validate()

    def exists(self):
        return all([s.exists() for s in self.splits])

    def create_dirs(self):
        for s in self.splits: s.create_dirs()

    def clean(self):
        self.clean_dir(self.loc)

    def persist(self, output):
        for s in self.splits:
            s.persist(output[s.artifact_kw["split"]])

class ESDump(Artifact):

    def __init__(self, fn_id, chunk_data=True, **kw):
        super(ESDump, self).__init__(fn_id, **kw)
        self.idx = kw.get("idx", None)
        if chunk_data:
            self.data = ChunkedFile("es_dump_data_chunked", **kw)
        else:
            self.data = File("es_dump_data_file", **kw)
        self.mapping = File("es_dump_mapping", **kw)

    def set_loc(self, ff, idx=None, **kw):
        idx = self.idx if idx is None else idx
        self.data.set_loc(ff, idx=idx)
        self.mapping.set_loc(ff, idx=idx)
        self.loc = ff(self.fn_id, idx=idx)

    def _validate(self):
        assert self.loc[-1] == "/", self.loc
        self.mapping._validate()
        self.data._validate()

    def exists(self):
        self.mapping and self.mapping.exists()
        self.data and self.data.exists()

    def create_dirs(self):
        self.mapping.create_dirs()
        self.data.create_dirs()

    def clean(self):
        self.clean_dir(self.loc)

    def load(self):
        pass

    def persist(self, output):
        self.mapping.persist(output["mapping"])
        self.data.persist(output["data"])

    def set_created_by(self, task):
        self.created_by = task
        self.mapping.created_by = task
        self.data.created_by = task
