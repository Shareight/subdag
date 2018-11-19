
# Status

NOT_STARTED = None
WAITING = "waiting" # task_run
RUNNING = "running" # pipeline_run, task_run
FAILED = "failed" # pipeline_run, task_run
UPSTREAM_FAILED = "upstream_failed" # task_run
SUCCESS = "success" # pipeline_run, task_run

# Strategies

RESUME = "resume"
RESTART = "restart"
CLEAN_AND_RESTART = "clean_and_restart"
SKIP = "skip"
FAIL = "fail"
MARK_SUCCESS = "mark_success"
