from dataclasses import astuple, dataclass
from random import uniform, random, sample

from simulator.job import JobWork, Request, JobSystemTurnOff
from simulator.scheduler import Scheduler


@dataclass
class SchedulerConfig:
    reconfig_scale: float = 0.331  #: The reconfiguration scaling factor in [0,1].
    reconfig_weight: float = 0.175  #: The reconfiguration weight in [0,1].
    alpha_weight: float = 0.742  #: The speedup factor's weight in [0,1].
    shutdown_scale: float = 0.760  #: The shutdown scaling factor in [0,1].
    shutdown_weight: float = 0.455  #: The shutdown's weight in [0,1].
    shutdown_time_short: float = 899  #: A short duration for shuting the servers.
    shutdown_time_long: float = 1406  #: A long duration for shuting the servers.
    shutdown_time_prob: float = 0.717  #: The probability of choosing shutdown_time_short.

    @classmethod
    def random(cls):
        c = SchedulerConfig()
        c.reconfig_scale = uniform(0.001, 1.0)
        c.reconfig_weight = uniform(0.01, 1.0)
        c.alpha_weight = uniform(0.001, 1.0)
        c.shutdown_scale = uniform(0.001, 1.0)
        c.shutdown_weight = uniform(0.01, 1.0)
        c.shutdown_time_short = uniform(370, 1200)
        c.shutdown_time_long = uniform(370, 4000)
        c.shutdown_time_prob = uniform(0.0001, 1.0)
        return c

    def to_dict(self):
        dict_obj = self.__dict__
        return dict_obj

    def to_list(self):
        return list(astuple(self))

class SchedulerPSO(Scheduler):
    NAME = "PSO"

    def __init__(self, server_count: int, args):
        super().__init__(server_count, args)

        self.conf = SchedulerConfig()

    def update(self, time: float):
        print(self.active_jobs)

        # removes the completed jobs
        for job in filter(lambda j: j.is_over(time), self.active_jobs):
            self.remove_job(time, job)

        # schedules new jobs on available servers by order of submission time
        while len(self.request_queue) > 0 and len(self.get_servers_available(time)) > 0:
            self.add_job(JobWork.from_request(time, self.request_queue.pop(0), [self.get_servers_available(time)[0]]))

        # Applies a reconfiguration to less cores
        for job in self.active_jobs:
            if isinstance(job, JobWork):
                if self._is_job_reconfigurable_decrease(job, self.get_servers_available(time), time):
                    av_servers = self._reconfigure_job_decrease(job, self.get_servers_available(time), time)

        # Applies a reconfiguration to more cores
        jobs_by_mass = sorted(self.active_jobs, key=lambda j: j.remaining_mass(time) if isinstance(j, JobWork) else 99999999)
        while jobs_by_mass and self.get_servers_available(time):
            job = jobs_by_mass[0]
            if isinstance(job, JobWork):
                if self._is_job_reconfigurable_increase(job, self.get_servers_available(time), time):
                    av_servers = self._reconfigure_job_increase(job, self.get_servers_available(time), time)
            jobs_by_mass.pop(0)

        # Applies power-offs
        for server in list(self.get_servers_available(time)):
            if not self._shutdown_server(self.get_servers_available(time)):
                break

            shutdown, duration = self._allow_shutdown(self.get_servers_available(time))
            if not shutdown:
                continue

            power_off = JobSystemTurnOff(time, server)
            self.add_job(power_off)

    def _allow_shutdown(self, av_servers: list):
        return False, None
        if (0.5 > ((len(av_servers) / len(self.servers)) ** self.conf.shutdown_weight) * self.conf.shutdown_scale):
            return False, 0

        if random() < self.conf.shutdown_time_prob:
            shutdown_duration = self.conf.shutdown_time_short
        else:
            shutdown_duration = self.conf.shutdown_time_long

        return True, shutdown_duration

    def _reconfigure_job_increase(self, job: JobWork, av_servers: list, time):
        job.interrupt(time)
        extra_srv_count = min(job.max_servers - len(job.servers), len(av_servers))
        extra_srvs = sample(av_servers, extra_srv_count)
        job_servers = job.servers + extra_srvs
        av_servers = [server for server in av_servers if server not in extra_srvs]

        reconfig_job, job_rest, _, _ = job.reconfigure_servers(time, job_servers)
        self.remove_job(time, job)
        self.add_job(reconfig_job)
        self.add_job(job_rest)

        return av_servers

    def _reconfigure_job_decrease(self, job: JobWork, av_servers: list, time):
        job.interrupt(time)

        dropped_server = sample(job.servers, 1)

        job_servers = job.servers.copy()
        for drop in dropped_server:
            job_servers.remove(drop)
            av_servers.append(drop)

        reconfig_job, job_rest, _, _ = job.reconfigure_servers(time, job_servers)
        self.remove_job(time, job)
        self.add_job(reconfig_job)
        self.add_job(job_rest)

        return av_servers

    def _is_job_reconfigurable_increase(self, job: JobWork, av_servers: list, time):
        if not job.is_executing(time) or len(job.servers) == job.max_servers:
            return False

        if time - job.time_beg < 100:
            return False

        return True

    def _is_job_reconfigurable_decrease(self, job: JobWork, av_servers: list, time):
        if not job.is_executing(time) or len(job.servers) == job.min_servers:
            return False

        if time - job.time_beg < 500:
            return False

        return True

    def _shutdown_server(self, av_servers: list):
        if not self.request_queue:
            return True

        required_servers = sum(req.min_server_count for req in self.request_queue)
        return len(av_servers) > required_servers

    def _allocate_servers(self, available_servers: list, job_req: Request):
        min_servers = min(job_req.max_server_count, len(available_servers))
        if min_servers < job_req.min_server_count:
            return []
        return sample(available_servers, k=min_servers)
