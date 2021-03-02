from math import log, sqrt
from random import seed, uniform
from typing import List

import numpy
import scipy.stats

from simulator.scheduler import Scheduler
from simulator.job import Request as JobRequest, generate_factor_linear


class Experiments:
    def __init__(self, scheduler, base_seed: int, period: int, num_experiments: int, num_servers: int, num_jobs: int, update_on_arrival: bool, update_on_departure: bool, args):
        self.scheduler = scheduler
        self.seed = base_seed
        self.period = period
        self.num_experiments = num_experiments
        self.num_servers = num_servers
        self.num_jobs = num_jobs
        self.update_on_arrival = update_on_arrival
        self.update_on_departure = update_on_departure
        self.args = args

    def run_experiments(self):
        return [self.run_experiment(self.seed + i, self.num_servers, self.num_jobs, self.period, self.update_on_arrival, self.update_on_departure, self.args) for i in range(self.num_experiments)]

    def run_experiment(self, exp_seed: int, num_servers: int, num_jobs: int, period: int, update_on_arrival: bool, update_on_departure: bool, args):
        scheduler: Scheduler = self.scheduler(num_servers, args)
        jobs: List[JobRequest] = self.generate_jobs(num_jobs, num_servers, exp_seed)

        time = 0

        # run the scheduler while job requests are pending or jobs are executing
        while len(jobs) > 0 or scheduler.is_working():
            # schedule all jobs that arrived since the last update
            for job in filter(lambda request: time >= request.submission_time, list(jobs)):
                scheduler.schedule(job)
                jobs.remove(job)

            # update the schedule
            scheduler.update(time)

            # compute the time for the next update call
            next_time = time + period
            if update_on_arrival and len(jobs) > 0:
                next_time = min(next_time, jobs[0].submission_time)
            if update_on_departure and scheduler.time_next_departure is not None:
                next_time = min(next_time, scheduler.time_next_departure)
            time = next_time

        # stop the schedule at the next period after it ended
        scheduler.stop(time)

        return scheduler

    def generate_jobs(self, job_count: int, server_count: int, seed_num: int):
        jobs: List[JobRequest] = []
        previous_sub_time = 0

        for num in range(job_count):
            job: JobRequest = self.generate_job(previous_sub_time, server_count, num, seed_num)
            jobs.append(job)
            previous_sub_time = job.submission_time

        return jobs

    def generate_job(self, time_last_event: int, server_count: int, num: int, seed_num: int):
        seed(seed_num + num)
        numpy.random.seed(seed=seed_num + num)
        sub_time, mass = self.get_next_task(time_last_event, 500, 1700, 3.8)
        data = uniform(10, 500)
        min_num_servers = 1
        max_num_servers = server_count

        return JobRequest(
            "job" + str(num),
            sub_time,
            data,
            mass,
            min_num_servers,
            max_num_servers,
            generate_factor_linear(1, 0),
            generate_factor_linear(1, 0),
            )

    def get_next_task(self, time_last_event, dynamism, mass, disparity):
        arrival = scipy.stats.pareto.rvs(4, loc=-1) * 3 * dynamism
        new_time = time_last_event + arrival
        makespan = self.get_makespan(mass, disparity)

        return new_time, makespan

    def get_makespan(self, mass, disparity):
        mu = log(mass / disparity)
        sigma = sqrt(2 * (numpy.log(mass) - mu))

        return scipy.stats.lognorm.rvs(sigma, scale=mass / disparity)
