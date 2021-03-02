import csv
import inspect
import sys

from configparser import ConfigParser
from pathlib import Path
from typing import Callable, Any

from simulator.experiment import Experiments

from simulator.scheduler import concrete

# seed used for reproducible experiments
default_seed = 1

# period between two runs of the scheduler
# if update_on_arrival or update_on_departure are enabled, the time at which the scheduler is called is min(current_time + period, next_arrival, next_departure)
default_period = 10

# number of experiments to run
# experiment n will have seed seed + n
default_num_experiments = 1

# number of servers per experiment
default_num_servers = 5
# number of jobs per experiment
default_num_jobs = 40

# specify if the scheduler is run when a job arrives
default_update_on_arrival = False
# specify if the scheduler is run when a job terminates
default_update_on_departure = False

# all classes in the module concrete are schedulers
# if they have a "NAME" member, it is used as name, else the class' name is used
schedulers = {(cls.NAME.lower() if hasattr(cls, "NAME") else name.lower()): cls for name, cls in inspect.getmembers(concrete, inspect.isclass)}


def run_experiments(name, scheduler, args):
    base_seed = read_parameter(args, "seed", default_seed, mapper_int, gen_predicate_int_geq(0))
    period = read_parameter(args, "period", default_period, mapper_int, gen_predicate_int_geq(1))
    num_experiments = read_parameter(args, "num_experiments", default_num_experiments, mapper_int, gen_predicate_int_geq(1))
    num_servers = read_parameter(args, "num_servers", default_num_servers, mapper_int, gen_predicate_int_geq(1))
    num_jobs = read_parameter(args, "num_jobs", default_num_jobs, mapper_int, gen_predicate_int_geq(1))
    update_on_arrival = read_parameter(args, "update_on_arrival", default_update_on_arrival, mapper_bool, predicate_true)
    update_on_departure = read_parameter(args, "update_on_departure", default_update_on_departure, mapper_bool, predicate_true)

    print(f"Running {name} on seed {base_seed} with {num_jobs} jobs on {num_servers} servers for {num_experiments} experiments")

    experiment = Experiments(scheduler, base_seed, period, num_experiments, num_servers, num_jobs, update_on_arrival, update_on_departure, args)

    schedules = experiment.run_experiments()

    for i in range(len(schedules)):
        path = Path(f"./results/traces/seed_{base_seed}/{name}/experiment_{i}.csv")
        path.parent.mkdir(0o755, parents=True, exist_ok=True)
        with open(path, 'w+') as csvfile:
            writer = csv.writer(csvfile)
            schedules[i].stats(writer)


def read_parameter(sec, name: str, default: Any, mapper: Callable[[str], Any], predicate: Callable[[Any], bool]):
    assert predicate(default)

    try:
        v = mapper(sec[name])
        if not predicate(v):
            raise ValueError
        return v
    except KeyError:
        return default
    except ValueError:
        print(f"Invalid value for {name} : {sec[name]}")
        exit(1)

def mapper_int(v: str):
    return int(v)


def mapper_bool(v: str):
    if v == "1" or v.lower() == "true":
        return True
    if v == "0" or v.lower() == "false":
        return False
    raise ValueError


def gen_predicate_int_geq(v: int):
    return lambda _v: _v >= v


def predicate_true(_):
    return True


if len(sys.argv) > 1:
    config = ConfigParser()

    config.read(sys.argv[1])

    for section in config.sections():
        if section.upper() == "GLOBAL":
            default_seed = read_parameter(config[section], "seed", default_seed, mapper_int, gen_predicate_int_geq(0))
            default_period = read_parameter(config[section], "period", default_period, mapper_int, gen_predicate_int_geq(1))
            default_num_experiments = read_parameter(config[section], "num_experiments", default_num_experiments, mapper_int, gen_predicate_int_geq(1))
            default_num_servers = read_parameter(config[section], "num_servers", default_num_servers, mapper_int, gen_predicate_int_geq(1))
            default_num_jobs = read_parameter(config[section], "num_jobs", default_num_jobs, mapper_int, gen_predicate_int_geq(1))
            default_update_on_arrival = read_parameter(config[section], "update_on_arrival", default_update_on_arrival, mapper_bool, predicate_true)
            default_update_on_departure = read_parameter(config[section], "update_on_departure", default_update_on_departure, mapper_bool, predicate_true)

    for section in config.sections():
        if section.upper() != "GLOBAL":
            scheduler_name = config[section]["scheduler"].lower()
            if scheduler_name in schedulers:
                run_experiments(section, schedulers[scheduler_name], config[section])
            else:
                print(f"Unknown Scheduler: {scheduler_name}")
