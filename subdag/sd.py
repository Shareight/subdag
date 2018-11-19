import click

# load user-defined endpoints, pipelines and tasks
# the order of loading these is important!
import dags.sd_tasks as sd_tasks
import dags.sd_pipelines # TODO
import dags.sd_endpoints as e
import db as db_module
from pipeline import Pipeline

cli = e.sd.cli
run = e.sd.run

@cli.group()
def db():
    pass

@db.command()
def setup():
    db_module.setup()

@db.command()
def teardown():
    db_module.teardown()

@db.command()
def reset():
    db_module.teardown()
    db_module.setup()

@cli.command()
@click.option("--max_retries", default=2)
def run_unfinished(max_retries):
    Pipeline.run_unfinished(max_retries, sd_tasks)

if __name__ == '__main__':
    cli()
