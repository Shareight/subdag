import click
from copy import deepcopy
from functools import update_wrapper

from subdag.runner import DirectRunner
from subdag.logs import Loggers
from subdag.globals import loc

from sd_filenames import fn_formatter

@click.group()
def cli():
    pass

@cli.group()
def run():
    pass


def ppl_args(pipeline_name):
    assert_exists(pipeline_name)
    return [
        arg[0][0].replace("--", "")
        for arg in loc.pipeline_params[pipeline_name]
    ]

def task_args(pipeline_name, task_name):
    assert_exists(pipeline_name, task_name)
    return [
        arg[0][0].replace("--", "")
        for arg in loc.task_params[task_name]
    ]

def assert_exists(pipeline_name, task_name=None):
    assert pipeline_name in loc.pipeline_fns, \
        ("pipeline %s not defined" % pipeline_name, loc.pipeline_fns.keys())
    if task_name:
        assert task_name in loc.task_fns, \
            ("task %s#%s not defined" % (pipeline_name, task_name), loc.task_fns.keys())

def pipeline(description=""):
    def decorator(f):

        pipeline_name = f.__name__
        loc.pipeline_fns[pipeline_name] = f
        ppl_params = getattr(f, "__ss_options__", {})
        loc.pipeline_params[pipeline_name] = ppl_params
        loc.pipeline_descriptions[pipeline_name] = description

        return f
    return decorator

def endpoint(pipeline_name, final_tasks=[]):
    def dependent_tasks():
        ret = set()
        for tn in final_tasks:
            t = p.tasks[tn]
            ret.add(t)
            for a in t.ancestors:
                ret.add(a)
        return list(ret)

    def pass_pipeline_set_loggers(f):
        def new_func(*args, **kwargs):
            ctx = click.get_current_context()
            p = get_p(ctx, kwargs)
            kwargs["run_id"] = p.run_id

            loggers = Loggers(kwargs["run_id"])
            for t in dep_tasks:
                loggers.setup_task_logger(t.name)
                loggers.wrap_tf_logger(t.name)

            return ctx.invoke(f, p, final_tasks, *args, **kwargs)

        return update_wrapper(new_func, f)

    def get_p(ctx, kwargs):
        p_arg_names = [
            a[-1].replace("--", "")
            for a, _ in loc.pipeline_params[pipeline_name]
        ]
        sel_kw = {
            k: kwargs[k]
            for k in p_arg_names
            if k in kwargs
        }
        return ctx.invoke(loc.pipeline_fns[pipeline_name], **sel_kw)

    def decorator(f):
        endpoint_name = f.__name__
        f = pass_pipeline_set_loggers(f)
        f = run.command(endpoint_name)(f)

        for args, kwargs in loc.pipeline_params[pipeline_name]:
            f = click.option(*args, **kwargs)(f)

        for t in dep_tasks:
            for args, kwargs in loc.task_params.get(t.name, {}):
                f = click.option(*args, **kwargs)(f)

        return f

    assert_exists(pipeline_name)
    p = loc.pipeline_fns[pipeline_name](run_id=None)
    help = "-- %s (PIPELINE) --" % loc.pipeline_descriptions[pipeline_name]

    if pipeline_name not in loc.command_groups:
        loc.command_groups[pipeline_name] = run.group(pipeline_name, help=help)(lambda *a, **kw: None)
    g = loc.command_groups[pipeline_name]
    dep_tasks = dependent_tasks()

    if pipeline_name == "default":
        for t in p.tasks.values():
            define_click_cmd(p, t, run)

    for t in dep_tasks:
        define_click_cmd(p, t, g)

    return decorator

def execute_task(p, t, f):
    def new_func(*args, **kwargs):

        kwargs["run_id"] = kwargs.get("run_id", p.run_id)
        loggers = Loggers(kwargs["run_id"])
        loggers.setup_task_logger(t.name)

        r = DirectRunner(None, kwargs, fn_formatter(kwargs))
        r.prepare(t)

        kwargs.update(t.load_inputs())
        r.run(t)
        return f

    return update_wrapper(new_func, f)

def define_click_cmd(p, t, define_on):
    f = deepcopy(loc.task_fns[t.name])
    f = execute_task(p, t, f)
    f = define_on.command(t.name)(f)

    assert_exists(p.name)
    if t.name not in loc.pipeline_params_defined:
        loc.pipeline_params_defined.add(t.name)
        for args, kwargs in loc.pipeline_params.get(p.name, {}):
            f = click.option(*args, **kwargs)(f)

    loc.defined_click_cmds[id(define_on)].add(t.name)

def task():
    def decorator(f):
        task_name = f.__name__

        loc.task_params[task_name] = getattr(f, "__ss_options__", [])
        loc.task_outputs[task_name] = list(getattr(f, "__ss_loc.task_outputs__", []))
        loc.task_fns[task_name] = f

        return f
    return decorator

def option(*param_decls, **option_attrs):
    def decorator(f):

        if not hasattr(f, "__ss_options__"):
            setattr(f, "__ss_options__", [])
        option_attrs["show_default"] = True
        f.__ss_options__.append((param_decls, option_attrs))

        return click.option(*param_decls, **option_attrs)(f)
    return decorator

def shared_option(shared_option):
    args, kwargs = shared_option
    return option(*args, **kwargs)

def shared_options(opt_keys, opts):
    def decorator(f):
        if not hasattr(f, "__ss_options__"):
            setattr(f, "__ss_options__", [])

        for k in opt_keys:
            param_decls, option_attrs = opts[k]
            option_attrs["show_default"] = True

            f.__ss_options__.append((param_decls, option_attrs))
            f = click.option(*param_decls, **option_attrs)(f)
        return f

    return decorator

def outputs(*args):
    def decorator(f):
        setattr(f, "__ss_loc.task_outputs__", args)
        return f
    return decorator
