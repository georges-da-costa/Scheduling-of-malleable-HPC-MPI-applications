import abc

from typing import List, Dict, Callable

from simulator.job import Request, Job, JobWork, JobReconfiguration, JobSystemFrequency, JobSystemTurnOn, JobSystemTurnOff
from simulator.server import Server


class Scheduler(abc.ABC):
    def __init__(self, *servers: Server, args):
        self.request_queue: List[Request] = []

        self.jobs_active: List[Job] = []
        self.jobs_complete: Dict[str, List[Job]] = {}

        self.servers: List[Server] = list(servers)

    @abc.abstractmethod
    def update(self, time: float): ...

    # scheduler-level job operations

    def add_request(self, request: Request):
        # Adds the request to the scheduler.

        self.request_queue.append(request)

    def add_job(self, *jobs: Job):
        # Adds the jobs to the scheduler.

        for job in jobs:
            job.take_servers()

            self.jobs_active.append(job)

    def remove_job(self, *jobs: Job):
        # Removes a job from the schedule.

        for job in jobs:
            job.drop_servers()

            self.jobs_active.remove(job)

            if job.id not in self.jobs_complete.keys():
                self.jobs_complete[job.id] = []

            self.jobs_complete[job.id].append(job)

    # job-level operations

    def terminate_jobs(self, time: float, *jobs: Job):
        # Marks the jobs as completed or interrupted depending on the specified time.
        #
        # see `self.interrupt_job` and `self.complete_job`.

        for job in jobs:
            if job.is_over(time):
                self.complete_jobs(time, job)
            else:
                self.interrupt_jobs(time, job)

    def interrupt_jobs(self, time: float, *jobs: Job):
        # Marks the jobs as interrupted.

        for job in jobs:
            job.interrupt(time)

    def complete_jobs(self, time: float, *jobs: Job):
        # Marks the jobs as completed.

        for job in jobs:
            job.complete(time)

    # job-level reconfiguration operations

    def reconfigure_job(self, time: float, job: Job, servers: List[Server] = None, frequency: float = None, turn_on_before: bool = False):
        # Returns the jobs for a reconfiguration of the job.
        #
        # The first value returned is the continuation job.
        # The second value returned is the server reconfiguration job.
        # The third value returned is a list of frequency reconfiguration jobs.
        # The fourth value returned is a list of turn on jobs.
        #
        # ! is_job_reconfigurable must return True for the parameters

        assert isinstance(job, JobWork)

        if frequency is None:
            frequency = job.frequency

        if servers is None:
            servers = job.servers

        assert self.is_job_reconfigurable(time, job, *servers, frequency=frequency, turn_on_before=turn_on_before)

        job_continuation = None
        job_reconfiguration_servers = None
        jobs_reconfiguration_frequency = []
        jobs_turn_on = []

        # computes the turn_on jobs, if applicable, and computes the frequency reconfiguration job
        if turn_on_before:
            for server in servers:
                if server.is_turned_off(time):
                    job_turn_on = JobSystemTurnOn(time, server)
                    jobs_turn_on.append(job_turn_on)

                    # the frequency reconfiguration job begins at the end of the turn on job
                    if frequency != server.get_frequency(time):
                        jobs_reconfiguration_frequency.append(JobSystemFrequency(job_turn_on.time_end, server, frequency))

            time_end_turn_on = max(jobs_turn_on, key=lambda _j: _j.time_end).time_end if len(jobs_turn_on) > 0 else time

            # the frequency reconfiguration job begins at the maximum end time of the turn on jobs
            for server in servers:
                if server.is_turned_on(time):
                    jobs_reconfiguration_frequency.append(JobSystemFrequency(time_end_turn_on, server, frequency))
        else:
            for server in servers:
                jobs_reconfiguration_frequency.append(JobSystemFrequency(time, server, frequency))

        servers_old_sorted = sorted(job.servers, key=lambda _s: _s.id)
        servers_new_sorted = sorted(servers, key=lambda _s: _s.id)

        if servers_old_sorted != servers_new_sorted:
            # the beginning time of the servers reconfiguration job is the first value that exists in the following list
            #   · maximum end time of turn on jobs and frequency reconfiguration jobs
            #   · time of reconfiguration call
            time_beg_reconfiguration_servers = max(max(jobs_turn_on, key=lambda _j: _j.end_time).time_end if len(jobs_turn_on) > 0 else time, max(jobs_reconfiguration_frequency, key=lambda _j: _j.end_time).time_end if len(jobs_reconfiguration_frequency) > 0 else time)

            job_reconfiguration_servers = JobReconfiguration(job, servers, time_beg_reconfiguration_servers)

        if len(jobs_reconfiguration_frequency) != 0 or job_reconfiguration_servers is not None:
            # the beginning time of the continuation job is the first value that exists in the following list
            #   · end time of the servers reconfiguration job
            #   · maximum end time of the frequency reconfiguration jobs
            time_beg_continuation = job_reconfiguration_servers.time_end if job_reconfiguration_servers is not None else max(jobs_reconfiguration_frequency, key=lambda j: j.time_end).time_end

            # compute the continuation job
            job_continuation = JobWork(job.id, time_beg_continuation, job.remaining_mass(time), job.data, servers, frequency, job.min_servers, job.max_servers, job.malleability_factor, job.frequency_factor)

        return job_reconfiguration_servers, jobs_reconfiguration_frequency, job_continuation, jobs_turn_on

    def is_job_reconfigurable(self, time: float, job: Job, servers: List[Server] = None, frequency: float = None, turn_on_before: bool = False):
        # Returns true if a call to `self.reconfigure_job` with the same arguments is valid.
        #
        # If frequency is None, the job's current frequency is used.
        # If servers is None, the job's current servers are used.
        #
        # A job is reconfigurable to a list of servers if:
        #   · the servers are turned on or are turnable on if `turn_on_before` is True
        #   · the servers support the frequency
        #   · the new servers are not busy
        #   · the job supports the number of servers
        #   · the job is reconfigurable

        assert isinstance(job, JobWork)

        if frequency is None:
            frequency = job.frequency

        if servers is None:
            servers = job.servers

        if not turn_on_before:
            for server in servers:
                if not server.is_turned_on(time):
                    return False
        else:
            for server in servers:
                if not server.is_turnable_on(time):
                    return False

        for server in servers:
            if server in job.servers:
                continue

            if server.is_busy(time):
                return False

            if frequency not in server.frequencies:
                return False

        if not job.is_executable_on(len(servers)):
            return False

        return True

    # scheduler-level server operations

    def get_servers(self, predicate: Callable[[Server], bool] = lambda _s: True) -> List[Server]:
        # Returns the servers that validate the predicate.

        return [server for server in self.servers if predicate(server)]

    def get_servers_on(self) -> List[Server]:
        # Returns the servers that are turned on.

        return self.get_servers(lambda _s: _s.is_turned_on())

    def get_servers_off(self) -> List[Server]:
        # Returns the servers that are turned off.

        return self.get_servers(lambda _s: _s.is_turned_off())

    def get_servers_busy(self) -> List[Server]:
        # Returns the servers that are busy.

        return self.get_servers(lambda _s: _s.is_busy())

    def get_servers_available(self) -> List[Server]:
        # Returns the servers that are available.

        return self.get_servers(lambda _s: _s.is_available())

    # server-level status operations

    def turn_servers_on(self, time: float, *servers: Server):
        # Returns the jobs for the turning on of the servers.

        assert self.are_servers_turnable_on(time, *servers)

        jobs = []

        for server in servers:
            jobs.append(JobSystemTurnOn(time, server))

        return jobs

    def turn_servers_off(self, time: float, *servers: Server):
        # Returns the jobs for the turning off of the servers.

        assert self.are_servers_turnable_off(time, *servers)

        jobs = []

        for server in servers:
            jobs.append(JobSystemTurnOff(time, server))

        return jobs

    def are_servers_turnable_on(self, time: float, *servers: Server):
        # Returns true if the servers are turnable on at the specified time.

        for server in servers:
            if not server.is_turnable_on(time):
                return False

        return True

    def are_servers_turnable_off(self, time: float, *servers: Server):
        # Returns true if the servers are turnable off at the specified time.

        for server in servers:
            if not server.is_turnable_off(time):
                return False

        return True

    def are_servers_turned_on(self, time: float, *servers: Server):
        # Returns true if the servers are turned on at the specified time.

        for server in servers:
            if not server.is_turned_on(time):
                return False

        return True

    def are_servers_turned_off(self, time: float, *servers: Server):
        # Returns true if the servers are turned off at the specified time.

        for server in servers:
            if not server.is_turned_off(time):
                return False

        return True

    # server-level frequency operations

    def set_server_frequency(self, time: float, frequency: float, *servers: Server):
        # Returns the jobs for reconfiguring the servers to the specified frequency at the specified time.

        assert self.are_servers_frequency_settable(time, frequency, *servers)

        jobs = []

        for server in servers:
            if frequency == server.get_frequency(time):
                jobs.append(JobSystemFrequency(time, server, frequency))

        return jobs

    def set_server_frequency_max(self, time, server: Server):
        # Returns the job for reconfiguring the server to its maximum frequency.

        return self.set_server_frequency(time, server.max_frequency(), server)

    def set_server_frequency_min(self, time, server: Server):
        # Returns the job for reconfiguring the server to its minimum frequency.

        return self.set_server_frequency(time, server.min_frequency(), server)

    def are_servers_frequency_settable(self, time: float, frequency: float, *servers: Server):
        # Returns true if the servers' frequency can be set to the specified frequency at the specified time.

        for server in servers:
            if not server.is_frequency_settable(time, frequency):
                return False

        return True

    # misc operations

    def sample_servers(self, n: int, *servers: Server):
        # Returns n servers from the list.

        assert n <= len(servers)

        servers_sorted = list(servers)
        servers_sorted.sort(key=lambda _s: _s.id)

        return [servers_sorted[_i] for _i in range(n)]

    def sample_available_servers(self, n: int):
        # Returns n servers from the list of available servers.

        return self.sample_servers(n, *self.get_servers_available())
