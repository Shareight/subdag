# subdag - a minimal task manager for ML workflows [alpha]

subdag takes a simple approach to defining workflows: any appropriately decorated Python function becomes a `task`, which becomes available in one or more `pipeline` definitions.
Tasks can be run independently with `sd run pipeline task` (or `sd run task` for the `default` pipeline), or as part of a pipeline run.

The following creates a task named `dump_users` that accepts two arguments (which can be supplied when executing with `sd run`).

```python
import subdag as sd
import os
import json

@sd.task()
@sd.option("--es_host", default=os.environ.get("ES_HOST"))
@sd.option("--es_index", default="items")
def dump_users(es_host, es_index, **kw):
    iterator = get_es_data(es_host, es_index)

    with open("/path/to/users.json", "w") as f:
        for u in iterator:
            f.write(json.dumps(u))
```

A pipeline describes the superset of tasks that can potentially be executed together.
Pipelines are defined in a file (or module) called `sd_pipelines.py` by decorating a method with ` @sd.pipeline("pipeline description") `.
The function should set up a `sd.Pipeline` object, define which tasks belong to it, and return the `sd.Pipeline` object.
Calling ` sd.Pipeline.task(task_name) ` returns an instance of the task task_name (which has to be defined earlier by decorating a function called `task_name` with ` @sd.task `).

```python
@sd.pipeline("full retrain")
@sd.shared_option(shared_options["prefix"])
@sd.shared_option(shared_options["run_id"])
def default(**kw):
    p = sd.Pipeline("default", kw["run_id"])

    dump_products, (products) = p.task("dump_products")
    dump_users, (users) = p.task("dump_users")
    preprocess, (dataset) = p.task("preprocess", inputs=[users, products])
    train, (model_dir) = p.task("train", inputs=[dataset])
    predict, (predictions) = p.task("predict", inputs=[model_dir])
    update = p.task("update", deps=[predict])

    return p
```

Pipelines are not executed directly - an `endpoint` defines a set of tasks in a pipeline which will be executed along with their upstream dependencies

A pipeline can accept options via decorators the same way as tasks can, which will be available to the tasks in this pipeline via keyword arguments.
If several tasks define the same option, its meaning (+ description and default value) should be the same across the different tasks, since when running an endpoint one can specify a single value for that option.
There is a convenience method ` @sd.shared_option ` that accepts a tuple `(args, kwargs)`, and ` @sd.shared_options `, that accepts two arguments: a list of keywords and a dictionary with `keyword => (args, kwargs)` items. The usage of both is demonstrated below.

```python
shared_opts = {
    "es_host": (
        ("-eh", "--es_host"),
        {"default": "http://localhost:9200", "help": "ElasticSearch host"}
    ),
    "es_host": (
        ("-ei", "--es_index"),
        {"type": str, "help": "ElasticSearch index name, e.g. products, users"}
    ),
}

@sd.task()
@sd.shared_option(shared_opts["es_host"])
@sd.shared_option(shared_opts["es_index"])
def dump_users(es_host, es_index, **kw):
    ...

@sd.task()
@sd.shared_options(["es_host", "es_index"], shared_opts)
def dump_products(es_host, es_index, **kw):
    ...
```
