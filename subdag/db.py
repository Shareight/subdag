import peewee as pw
import datetime
import os

kw = dict(
    user=os.environ.get("DB_USER", "root"),
    host=os.environ.get("DB_HOST", "127.0.0.1"),
    port=os.environ.get("DB_PORT", 3306),
)
if os.environ.get("DB_PASS"):
    kw["password"] = os.environ.get("DB_PASS")

db = pw.MySQLDatabase(
    os.environ.get("DB_NAME", "dags"),
    **kw
)
# db.connect()

class BaseModel(pw.Model):
    class Meta:
        database = db

class Pipeline(BaseModel):
    id = pw.CharField(unique=True, primary_key=True)

class PipelineRun(BaseModel):
    class Meta:
        table_name = "pipeline_run"

    pipeline = pw.ForeignKeyField(Pipeline, backref='pipeline_runs')
    run_id = pw.CharField(unique=True, primary_key=True)

    args = pw.TextField(null=True)
    final_tasks = pw.CharField(null=True)
    scheduled = pw.BooleanField(default=False)
    retries = pw.IntegerField(default=0)
    state = pw.CharField(null=True)
    start_time = pw.DateTimeField(default=datetime.datetime.now)
    end_time = pw.DateTimeField(null=True)

class TaskRun(BaseModel):
    class Meta:
        table_name = "task_run"
        indexes = (
            (("pipeline_run", "task_name", "extra_kwargs"), True),
        )

    pipeline_run = pw.ForeignKeyField(PipelineRun, backref='task_runs')
    task_name = pw.CharField()
    extra_kwargs = pw.CharField()
    # id is auto incremented

    state = pw.CharField(null=True)
    start_time = pw.DateTimeField(default=datetime.datetime.now)
    end_time = pw.DateTimeField(null=True)


def setup():
    print("Setting up DB.")
    db.create_tables([Pipeline, PipelineRun, TaskRun])

def teardown():
    print("Tearing down DB.")
    db.drop_tables([Pipeline, PipelineRun, TaskRun], safe=False)

# teardown()
# setup()
