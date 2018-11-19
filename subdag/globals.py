from threading import local
from collections import defaultdict

loc = local()
loc.pipeline_fns = {}
loc.pipeline_params = {}
loc.pipeline_params_defined = set()
loc.pipeline_descriptions = {}
loc.task_fns = {}
loc.task_params = {}
loc.task_outputs = {}
loc.command_groups = {}
loc.defined_click_cmds = defaultdict(lambda: set())
