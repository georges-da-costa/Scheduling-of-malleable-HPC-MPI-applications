from math import floor
from typing import List, Callable
from dataclasses import dataclass
from abc import ABC

from simulator.server import Server


@dataclass
class Request:
    id: str

    submission_time: int

    data: float
    mass: float

    min_server_count: int
    max_server_count: int

    malleability_factor: Callable[[int], float]  # Callable[[number of server from 1 to max_server_count], number greater or equal to 1]
    frequency_factor: Callable[[float], float]  # Callable[[target frequency], number lesser or equal to 1]


class Job(ABC):
    def __init__(self, job_id: str, time_beg: float, time_end: float, servers: List[Server]):
        assert time_beg < time_end
        assert len(servers) > 0

        # unique id of the job
        self.id = job_id

        # time at which the job starts and ends
        self.time_beg = time_beg
        self.time_end = time_end

        # time at which the job actually ended (different from self.time_end if the job is interrupted)
        # will stay None until the job actually ends (by calling either `self.complete` or `self.interrupt`)
        self.time_end_real = None

        # the servers on which the job is executing
        self.servers = servers

    def __repr__(self) -> str:
        return f"{self.id} ({type(self).__name__}) on {len(self.servers)}: {self.time_beg} -> {self.time_end}"

    def is_pending(self, time: float):
        # Returns true if the job is pending.
        #
        # A job is pending if the time is before `self.time_beg`.

        return self.time_beg > time

    def is_executing(self, time: float):
        # Returns true if the job is executing.
        #
        # A job is executing if it is not marked as interrupted or complete and if the time is between `self.time_beg` and `self.time_end`.

        return self.time_end_real is None and self.time_beg <= time < self.time_end

    def is_over(self, time: float):
        # Returns true if the job is over.
        #
        # A job is over if it is marked as completed or interrupted or if the time is past `self.time_end`.

        return time >= (self.time_end if self.time_end_real is None else self.time_end_real)

    def is_interruptable(self, time: float):
        # Returns true if the job in interruptable.

        # by default, all jobs are interruptable if they are executing

        return self.is_executing(time)

    def is_completed(self):
        # Returns true if the job is marked completed.
        #
        # Panics if the job is not over.
        #
        # An over job is considered completed if it has finished all of its work.

        # assert self.time_end_real is not None

        return self.time_end == self.time_end_real

    def is_interrupted(self):
        # Returns true if the job is marked interrupted.
        #
        # Panics if the job is not over.
        #
        # An over job is considered interrupted if it has not finished all of its work.

        # assert self.time_end_real is not None

        return self.time_end != self.time_end_real

    def take_servers(self):
        # Adds the job to its servers.
        #
        # The job must not be marked as terminated.
        #
        # ! This function should be called only once per job.

        for server in self.servers:
            server.add_job(self)

    def drop_servers(self):
        # Removes the job from its servers.
        #
        # The job must be marked as terminated.
        #
        # ! This function should be called only once per job.

        for server in self.servers:
            server.remove_job(self)

    def release_servers(self):
        # Removes the job from its servers.
        #
        # The job must be marked as terminated.
        #
        # ! This function should be called only once per job.

        for server in self.servers:
            server.remove_job(self)

    def terminate(self, time: float):
        # Marks the job as completed or interrupted.
        #
        # See `self.complete` and `self.interrupt`.

        if time < self.time_end:
            self.interrupt(time)
        else:
            self.complete(time)

    def complete(self, time: float):
        # Marks the job as completed.
        #
        # The job must not be marked completed or interrupted.
        # `time` must be no earlier than `self.time_end`.
        #
        # ! Releasing the servers that this job used is of the scheduler's responsibility.

        assert self.time_end_real is None
        assert self.time_end <= time

        # marks the job as complete
        self.time_end_real = self.time_end

    def interrupt(self, time: float):
        # Marks the job as interrupted.
        #
        # The job must not be marked as completed or interrupted.
        # `time` must be between `self.time_beg` and `self.time_end`.
        #
        # ! Releasing the servers that this job used is of the scheduler's responsibility.

        # assert self.time_end_real is None
        assert self.time_beg < time < self.time_end

        # marks the job as interrupted
        self.time_end_real = time

    def export_csv(self):
        return [[self.id, type(self).__name__, self.time_beg, self.time_end_real, server.id] for server in self.servers]


class JobWork(Job):
    def __init__(self, job_id: str, time_beg: float, mass: float, data: float, servers: List[Server], frequency: float, min_servers: int, max_servers: int, malleability_factor: Callable[[int], float], frequency_factor: Callable[[float], float]):
        assert mass > 0
        assert data >= 0
        assert len(servers) > 0

        time_end = time_beg + mass / (malleability_factor(len(servers)) * frequency_factor(frequency))

        super().__init__(job_id, time_beg, time_end, servers)

        # represents the amount of work required if the job is executed on a single server at frequency 1
        self.mass = mass
        # represents the amount of data transferred during a reconfiguration
        self.data = data

        # lower and upper bound to the number of servers
        self.min_servers = min_servers
        self.max_servers = max_servers

        # frequency at which the job is executing
        self.frequency = frequency

        # factors used to compute the impact of malleability and dvfs (multiplied with mass to obtain the duration of the job)
        self.malleability_factor = malleability_factor
        self.frequency_factor = frequency_factor

    @classmethod
    def from_request(cls, time: float, request: Request, servers: List[Server]):
        # Returns the JobWork created from the request.

        return cls(request.id, time, request.mass, request.data, servers, servers[0].get_frequency(time), request.min_server_count, request.max_server_count, request.malleability_factor, request.frequency_factor)

    def is_executable_on(self, n: int):
        return self.min_servers <= n <= self.max_servers

    def is_reconfigurable_servers(self, time: float, servers: List[Server], turn_on: bool = False, turn_off: bool = False):
        # Returns true if the job is reconfigurable to different servers.
        #
        # This function validates a call to `self.reconfigure_servers` with the exact same parameters.
        #
        # `turn_on` specify if turned off servers should be turned on or if they should be treated as an error.
        # `turn_off` is unused here, but kept to have a signature consistent with `self.reconfigure_servers`.

        # a non-executing job makes the job non-reconfigurable with these parameters
        if not self.is_executing(time):
            return False

        # an unsupported number of servers makes the job non-reconfigurable with these parameters
        if self.min_servers > len(servers) or len(servers) > self.max_servers:
            return False

        for server in servers:
            if server not in self.servers:
                # busy servers make the job non-reconfigurable with these parameters
                if server.is_busy(time):
                    return False
                # turned off servers make the job non-reconfigurable with these parameters
                if server.is_turned_off(time):
                    return False
                # non frequency harmonized servers make the job non-reconfigurable with these parameters
                if self.frequency != server.get_frequency(time):
                    return False

        # the job is reconfigurable with these parameters
        return True

    def is_reconfigurable_frequency(self, time: float, frequency: float):
        # Returns true if the job is reconfigurable to a different frequency.
        #
        # This functions validates a call to `self.reconfigure_frequency` with the exact same parameters.

        # a non-executing job makes the job non-reconfigurable with these parameters
        if not self.is_executing(time):
            return False

        # non-harmonizable frequencies make the job non-reconfigurable with these parameters
        for server in self.servers:
            if frequency not in server.frequencies:
                return False

        # the job is reconfigurable with these parameters
        return True

    def is_reconfigurable(self, time: float, servers: List[Server], frequency: float = None, turn_on: bool = False, turn_off: bool = False):
        # Returns true if the job is reconfigurable to different servers to a different frequency.
        #
        # This function validates a call to `self.reconfigure` with the exact same parameters.
        #
        # See `self.is_reconfigurable_servers` and `self.is_reconfigurable_frequency`.

        if frequency is None:
            frequency = self.frequency

        return self.is_reconfigurable_servers(time, servers, turn_on, turn_off) and self.is_reconfigurable_frequency(time, frequency)

    def reconfigure_servers(self, time: float, servers: List[Server], turn_on: bool = False, turn_off: bool = False):
        # Returns the jobs corresponding to a reconfiguration of the job to different servers.
        #
        # The first value returned is the server reconfiguration job.
        # The second value returned is the continuation job.
        # The third value is, if applicable, a list of turn_on jobs.
        # The fourth value is, if applicable, a list of turn_off jobs.
        #
        # See `self.is_reconfigurable_servers`.

        job_reconfiguration = None
        job_continuation = None
        jobs_turn_on = None
        jobs_turn_off = None

        # if applicable, compute the turn_on jobs
        if turn_on:
            jobs_turn_on = []

            time_beg_turn_on = time

            # turn on jobs target only the new servers are the old servers are by definition turned on
            for server in servers:
                if server.is_turned_off(time_beg_turn_on):
                    jobs_turn_on.append(JobSystemTurnOn(time_beg_turn_on, server))

        # the beginning time of the reconfiguration job is either the start of the reconfiguration process or the end of the latest turn on job
        time_beg_reconfiguration = time if jobs_turn_on is None else max(jobs_turn_on, key=lambda j: j.time_end).time_end
        # compute the reconfiguration job
        job_reconfiguration = JobReconfiguration(self, servers, time_beg_reconfiguration)

        # the beginning time of the continuation job is the end time of the reconfiguration job
        time_beg_continuation = job_reconfiguration.time_end
        # compute the continuation job
        job_continuation = JobWork(self.id, time_beg_continuation, self.remaining_mass(time), self.data, servers, self.frequency, self.min_servers, self.max_servers, self.malleability_factor, self.frequency_factor)

        # if applicable, compute the turn_off jobs
        if turn_off:
            jobs_turn_off = []

            # the beginning time of the turn_off jobs is the same as the continuation job
            time_beg_turn_off = time_beg_continuation

            for server in [server for server in self.servers if server not in servers]:
                jobs_turn_off.append(JobSystemTurnOff(time_beg_turn_off, server))

        return job_reconfiguration, job_continuation, jobs_turn_on, jobs_turn_off

    def reconfigure_frequency(self, time: float, frequency: float):
        # Returns the jobs corresponding to a reconfiguration of the job to a different frequency.
        #
        # The first value returned is the frequency reconfiguration job.
        # The second value returned is the continuation job.
        #
        # This functions returns two None if the frequency is the same as it currently is.
        #
        # See `self.is_reconfigurable_frequency`.

        # same frequency, no-op
        if self.frequency == frequency:
            return None, None

        jobs_reconfiguration = []
        job_continuation = None

        # the beginning time of the reconfiguration job is the start of the reconfiguration process
        time_beg_reconfiguration = time
        # compute the reconfiguration jobs
        for server in self.servers:
            jobs_reconfiguration.append(JobSystemFrequency(time_beg_reconfiguration, server, frequency))

        # the beginning time of the continuation job is the end of the latest reconfiguration job
        time_beg_continuation = max(jobs_reconfiguration, key=lambda j: j.time_end).time_end
        # compute the continuation job
        job_continuation = JobWork(self.id, time_beg_continuation, self.remaining_mass(time), self.data, self.servers, frequency, self.min_servers, self.max_servers, self.malleability_factor, self.frequency_factor)

        return jobs_reconfiguration, job_continuation

    def reconfigure(self, time: float, servers: List[Server], frequency: float = None, turn_on: bool = False, turn_off: bool = False):
        # Returns the jobs corresponding to a reconfiguration of the job to different servers to a different frequency.
        #
        # The first value returned is the server reconfiguration job.
        # The second value returned is the frequency reconfiguration job.
        # The third value returned is the continuation job.
        # The fourth value returned is, if applicable, a list of turn_on jobs.
        # The fifth value returned is, if applicable, a list of turn_off jobs.
        #
        # See `self.is_reconfigurable`.

        if frequency is None:
            frequency = self.frequency

        job_reconfiguration_servers = None
        jobs_reconfiguration_frequency = []
        job_continuation = None
        jobs_turn_on = None
        jobs_turn_off = None

        # if applicable, compute the turn_on jobs
        if turn_on:
            jobs_turn_on = []

            time_beg_turn_on = time

            # turn on jobs target only the new servers are the old servers are by definition turned on
            for server in servers:
                if server.is_turned_off(time_beg_turn_on):
                    jobs_turn_on.append(JobSystemTurnOn(time_beg_turn_on, server))

        # the beginning time of the servers reconfiguration job is either the start of the reconfiguration process or the end of the latest turn on job
        time_beg_reconfiguration_servers = time if jobs_turn_on is None else max(jobs_turn_on, key=lambda j: j.time_end).time_end
        # compute the servers reconfiguration job
        job_reconfiguration_servers = JobReconfiguration(self, servers, time_beg_reconfiguration_servers)

        # the beginning time of the frequency reconfiguration job is the end time of the servers reconfiguration job
        time_beg_reconfiguration_frequency = job_reconfiguration_servers.time_end
        # compute the frequency reconfiguration jobs
        for server in servers:
            jobs_reconfiguration_frequency.append(JobSystemFrequency(time_beg_reconfiguration_frequency, server, frequency))

        # the beginning time of the continuation job is the end of the latest frequency reconfiguration job
        time_beg_continuation = max(jobs_reconfiguration_frequency, key=lambda j: j.time_end).time_end
        # compute the continuation job
        job_continuation = JobWork(self.id, time_beg_continuation, self.remaining_mass(time), self.data, servers, frequency, self.min_servers, self.max_servers, self.malleability_factor, self.frequency_factor)

        # if applicable, compute the turn_off jobs
        if turn_off:
            jobs_turn_off = []

            # the beginning time of the turn_off jobs is the same as the frequency reconfiguration jobs
            time_beg_turn_off = time_beg_reconfiguration_frequency

            for server in [server for server in self.servers if server not in servers]:
                jobs_turn_off.append(JobSystemTurnOff(time_beg_turn_off, server))

        return job_reconfiguration_servers, jobs_reconfiguration_frequency, job_continuation, jobs_turn_on, jobs_turn_off

    def remaining_mass(self, time: float):
        # job has not started executing, full mass remains
        if self.is_pending(time):
            return self.mass

        if self.is_over(time):
            # job is marked as completed, remaining mass is 0
            if self.is_completed():
                return 0

            # job is marked as interrupted, compute remaining mass based on `self.time_end_real`
            if self.is_interrupted():
                return self.mass - ((self.mass * (self.time_end_real - self.time_beg)) / (self.time_end - self.time_beg))

            # should not reach this
            raise RuntimeError

        # job is still executing, compute remaining mass based on `time`
        return self.mass - ((self.mass * (time - self.time_beg)) / (self.time_end - self.time_beg))


class JobReconfiguration(Job):
    def __init__(self, job: JobWork, servers: List[Server], time_beg: float):
        duration = JobReconfiguration.duration(job.data, len(job.servers), len(servers))

        super().__init__(job.id, time_beg, time_beg + duration, list(set().union(servers, job.servers)))

        self.source = job

    @staticmethod
    def duration(data: float, ante: int, post: int):
        # Returns the duration of the reconfiguration.

        maxi = max(ante, post)
        mini = min(ante, post)

        return data / maxi * floor(maxi / mini)

    def reconfigure(self, time: float, servers: List[Server], turn_on: bool = False, turn_off: bool = False):
        # Returns the jobs corresponding to a new reconfiguration of the job.
        #
        # The first value returned is the server reconfiguration job.
        # The second value returned is the continuation job.
        # The third value is, if applicable, a list of turn_on jobs.
        # The fourth value is, if applicable, a list of turn_off jobs.

        job_reconfiguration = None
        job_continuation = None
        jobs_turn_on = None
        jobs_turn_off = None

        # if applicable, compute the turn_on jobs
        if turn_on:
            jobs_turn_on = []

            time_beg_turn_on = time

            # turn on jobs target only the new servers are the old servers are by definition turned on
            for server in servers:
                if server.is_turned_off(time_beg_turn_on):
                    jobs_turn_on.append(JobSystemTurnOn(time_beg_turn_on, server))

        # the beginning time of the reconfiguration job is either the start of the reconfiguration process or the end of the latest turn on job
        time_beg_reconfiguration = time if jobs_turn_on is None else max(jobs_turn_on, key=lambda j: j.time_end).time_end
        # compute the reconfiguration job
        job_reconfiguration = JobReconfiguration(self.source, servers, time_beg_reconfiguration)

        # the beginning time of the continuation job is the end time of the reconfiguration job
        time_beg_continuation = job_reconfiguration.time_end
        # compute the continuation job
        job_continuation = JobWork(self.id, time_beg_continuation, self.source.remaining_mass(time), self.source.data, servers, self.source.frequency, self.source.min_servers, self.source.max_servers, self.source.malleability_factor, self.source.frequency_factor)

        # if applicable, compute the turn_off jobs
        if turn_off:
            jobs_turn_off = []

            # the beginning time of the turn_off jobs is the same as the continuation job
            time_beg_turn_off = time_beg_continuation

            for server in [server for server in self.servers if server not in servers]:
                jobs_turn_off.append(JobSystemTurnOff(time_beg_turn_off, server))

        return job_reconfiguration, job_continuation, jobs_turn_on, jobs_turn_off


class JobSystem(Job, ABC):
    ID = "SYSTEM"

    def is_interruptable(self, time: float):
        # system jobs are not interruptable
        return False


class JobSystemFrequency(JobSystem):
    def __init__(self, time_beg: float, server: Server, frequency: float):
        super().__init__(JobSystem.ID, time_beg, time_beg + server.duration_frequency(), [server])

        self.frequency = frequency


class JobSystemTurnOn(JobSystem):
    def __init__(self, time_beg: float, server: Server):
        super().__init__(JobSystem.ID, time_beg, time_beg + server.duration_turn_on(), [server])


class JobSystemTurnOff(JobSystem):
    def __init__(self, time_beg: float, server: Server):
        super().__init__(JobSystem.ID, time_beg, time_beg + server.duration_turn_off(), [server])
