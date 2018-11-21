# subdag - a minimal task manager for ML workflows [alpha]

subdag takes a simple approach to defining workflows: any appropriately decorated Python function becomes a `task`, which becomes available in one or more `pipeline` definitions.
Tasks can be run independently with `sd run pipeline task` (or `sd run task` for the `default` pipeline), or triggered via an `endpoint` which executes a subset (or all) tasks in a given pipeline.

## Tasks

The following creates a task named `dump_users` that accepts two arguments (which can be supplied when executing with `sd run`).

```python
# sd_tasks/dump.py
import subdag as sd

@sd.task()
@sd.option("--es_host", default=os.environ.get("ES_HOST"))
def dump_users(es_host, **kw):
    iterator = get_es_data(es_host, "users", query)

    with open("/path/to/users.json", "w") as f:
        for u in iterator:
            f.write(json.dumps(u))
```

## Pipelines

A pipeline describes the superset of tasks that can potentially be executed together.
Pipelines are defined in a file (or module) called `sd_pipelines.py` by decorating a method with ` @sd.pipeline("pipeline description") `.
The function should set up a `sd.Pipeline` object, define which tasks belong to it, and return the `sd.Pipeline` object.
Calling ` sd.Pipeline.task(task_name) ` returns an instance of the task task_name (which has to be defined earlier by decorating a function called `task_name` with ` @sd.task `), and a tuple of the outputs produced by the task (if any).

```python
# sd_pipelines.py
@sd.pipeline("full retrain")
@sd.shared_option(shared_options["prefix"])
@sd.shared_option(shared_options["run_id"])
def pipeline_name(**kw):
    p = sd.Pipeline("default", kw["run_id"])

    dump_products, (products) = p.task("dump_products")
    dump_users, (users) = p.task("dump_users")
    preprocess, (dataset) = p.task("preprocess", inputs=[users, products])
    train, (model_dir) = p.task("train", inputs=[dataset])
    predict, (predictions) = p.task("predict", inputs=[model_dir])
    update = p.task("update", deps=[predict])

    return p
```

## Endpoints

Pipelines are not executed directly - an `endpoint` defines a set of tasks in a pipeline which will be executed along with their upstream dependencies.
The `final_tasks` argument in the endpoint decorator determines which tasks are the leaves that get executed, and the actual `sd.Task` instances are passed to the function as the `final_tasks` argument.
The `sd.Pipeline` is also passed as `p`.
Usually endpoint definitions look like the one below, with the the only difference in the arguments to the decorator.
The below endpoint can be invoked by `sd run pp_train_update`.

```python
# sd_endpoints.py
@sd.endpoint("pipeline_name", final_tasks=["update"])
def pp_train_update(p, final_tasks, **kw):
    runner = sd.DirectRunner(tasks_module, kw, fn_formatter(kw))
    p.run(final_tasks, runner)
```

Currently the only tested runner is `sd.DirectRunner`, which calls the relevant tasks in the appropriate order directly from Python.
Additional runners based on Dask or Kubernetes schedulers could be implemented.

## Artifacts

subdag has a notion of `artifacts` that tasks write as output or expect as input.
Output artifacts are defined via decorators as below.
Currently the built-in Artifacts are `File`, `Files`, `ChunkedFile`, `Dir`, `Dataset`.
The first argument to an artifact is always `artifact_id`; custom artifacts can define their own set of keyword arguments.

To manually save the contents of an artifact pass `autopersist=False` when creating an `sd.Artifact` in the decorator.
`autopersist=False` by default, which means the output is expected to be a dict where they keys are `artifact_id`s and the values are the data to be persisted (either (hopefully lazy) sequences or dicts).
The artifact class is responsible for persisting the data for all autopersisted outputs.

```python
# sd_tasks/dump.py
@sd.task()
@sd.option("--es_host", default=os.environ.get("ES_HOST"))
@sd.outputs(
    sd.File("users_mapping"),
    sd.File("users_data"),
    sd.File("some_other_file", autopersist=False),
)
def dump_users(es_host, **kw):
    users_iterator = ...
    users_mapping = ...

    with open("/path/to/some_other_file", "w") as f:
        f.write(...)

    return {"users_mapping": users_mapping, "users_data": users_iterator}
```

Before executing a task, subdag checks that its input artifacts exist, and that its output artifacts don't; after execution it ensures that the output artifacts have been written.
Note that artifacts need not be file-based: it is possible to write a custom one that checks whether certain data has been written to a database table.

## Pipeline and task arguments

A pipeline can accept options via decorators the same way as tasks can, which will be available to the tasks in this pipeline via keyword arguments.
If several tasks define the same option, its meaning (+ description and default value) should be the same across the different tasks, since when running an endpoint one can specify a single value for that option.
There is a convenience method ` @sd.shared_option ` that accepts a tuple `(args, kwargs)`, and ` @sd.shared_options `, that accepts two arguments: a list of keywords and a dictionary with `keyword => (args, kwargs)` items. The usage of both is demonstrated below.

```python
# sd_tasks/dump.py
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

## fn_formatter

Locations of artifacts should be determined using a formatting function that takes as input an artifact_id and optionally a set of keyword arguments.
We store a dictionary of artifact_id => file_pattern pairs.
The formating function looks up the `artifact_id` and replaces all placeholder values with either (1) the keyword arguments passed to the formatting function, (2) extra arguments when defining `p.task(task_name, **extra_args)`, (3) the arguments passed to the pipeline / task from the CLI (or their default values).

To achieve this, we define a file sd_filenames.py as below.

```python
# fn_filenames.py
import subdag as sd

fns = {
    "cats_mapping": "{prefix}/runs/{run_id}/categories/mapping.json",
    "cats_data": "{prefix}/runs/{run_id}/categories/data.data",
    "prods_mapping": "{prefix}/runs/{run_id}/products/mapping.json",
    "prods_data": "{prefix}/runs/{run_id}/products/data/*.json",
    "es_mapping": "{prefix}/runs/{run_id}/{index}/mapping.json",
    "es_data": "{prefix}/runs/{run_id}/{index}/data/*.json",
}

def fn_formatter(cli_kwargs, **extra_kwargs):
    return sd.fn_formatter(cli_kwargs, fns, **extra_kwargs)

# somewhere in a task, endpoint or artifact
# in a task or endpoint, kw is the **kw at the end of the task argument list
kw = {"prefix": "/data", "run_id": "rid"}
ff = fn_formatter(kw)
ff("es_mapping") => /data/runs/rid/products/mapping.json
ff("es_mapping", index="users") => /data/runs/rid/users/mapping.json

```python
# somewhere in a task, endpoint or artifact
# in a task or endpoint, kw is the **kw at the end of the task argument list
kw = {"prefix": "/data", "run_id": "rid"}
ff = fn_formatter(kw)
ff("es_mapping") => /data/runs/rid/products/mapping.json
ff("es_mapping", index="users") => /data/runs/rid/users/mapping.json

```

## Project structure



# Motivation for the design

* It should be simple to define and run individual tasks and group them into pipelines that need to be run together. Many DAG management / ETL tools are good at building pipelines with parameters that are known up front, but these tools are cumbersome to use in the more iterative workloads that are common to machine learning / data science projects.
* It should be easy to define arguments with defaults for individual tasks, share arguments among related tasks, and specify these in a uniform way when running individual tasks or full pipelines.
* Artifacts and fn_formatter help reduce the errors related to creating the location of a given file, which would be done in several parts of the code. It is easy to forget to update one of those code locations.
* Artifacts encapsulate the assumptions that a task has about the state before and after its execution. The task should fail immediately if those assumptions are violated, rather than starting a long-running job that is guaranteed to fail just before completion.
* Autopersisting outputs both reduces code duplication and enables switching between computation modes where the outputs of one task are persisted to disk or kept in memory and immediately passed to a downstream task.
