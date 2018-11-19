from string import Formatter
import subdag.utils as u

class FilenamesFormatter(Formatter):

    def __init__(self, cli_kwargs={}):
        Formatter.__init__(self)
        self.cli_kwargs = cli_kwargs

    def get_value(self, key, args, format_kwargs):
        if isinstance(key, str):
            if format_kwargs.get(key) is not None:
                ret = format_kwargs[key]
            elif self.cli_kwargs.get(key) is not None:
                ret = self.cli_kwargs[key]
            else:
                raise Exception("Can't replace %s in fn" % key)
            # assert ret is not None, "fn_formatter: %s is None" % key
            # print("fn_formatter: %s is None" % key)
            return ret
        else:
            return Formatter.get_value(key, args, format_kwargs)

def fn_formatter(cli_kwargs, fns_map, **extra_kwargs):
    def format(fn_id, **kw):
        fn = fns_map[fn_id]
        if "chunk" in kw:
            fn = fn.replace("*", str(kw["chunk"]))
        return fmt.format(fn, **kw)

    kw = set_defaults(u.merge(cli_kwargs, extra_kwargs))

    fmt = FilenamesFormatter(kw)
    return format

def set_defaults(kw):
    try:
        kw["hparams_id"] = kw.get("hparams_id") or "%s_%s" % (kw["model_type"], kw["input_type"])
    except KeyError: pass
    try:
        kw["model_id"] = kw.get("model_id") or "%s_%s" % (kw["run_id"], kw["hparams_id"])
    except KeyError: pass
    return kw
