# from enum import Enum
# from math import floor
# from typing import List, Callable
# from dataclasses import dataclass
#
# from simulator.server import Server
#
#
# @dataclass
# class Request:
#     id: str
#
#     submission_time: int
#
#     data: int
#     mass: int
#
#     min_server_count: int
#     max_server_count: int
#
#     speed_up_factor: Callable[[int], float]  # Callable[[number of server from 1 to max_server_count], number greater or equal to 1]
#     slow_down_factor: Callable[[float], float]  # Callable[[target frequency], number lesser or equal to 1]
#
#
# class Job:
#     class IDs(Enum):
#         SYSTEM = "SYSTEM"
#
#     class Kind(Enum):
#         WORK = "WORK"  # work job
#         RECONFIGURATION = "RECONFIGURATION"  # reconfiguration job
#         TURN_ON = "TURN_ON"  # turn on job
#         TURN_OFF = "TURN_OFF"  # turn off job
#         DVFS = "DVFS"  # dvfs job
#
#     def __init__(self, job_id: str, kind: Kind, data: int, mass: float, submission_time: float, start_time: float, end_time: float, servers: List[Server], frequency: float, min_server_count: int, max_server_count: int, speed_up_factor: Callable[[int], float], slow_down_factor: Callable[[float], float]):
#         assert 0 <= submission_time <= start_time < end_time
#         assert 1 <= min_server_count <= len(servers) <= max_server_count
#
#         # id and kind of the job
#         # id represents the identifier of the job
#         # kind represents the origin of the job (work, reconfiguration, system...)
#         self.id: str = job_id
#         self.kind: Job.Kind = kind
#
#         # data represents the quantity if data to be transferred when a reconfiguration is issued
#         self.data: int = data
#         # mass represents the work to be done
#         # a mass of 0 is possible for non-work jobs
#         self.mass: float = mass
#
#         # original submission date of the job
#         self.submission_time: float = submission_time
#         # time at which the job has started (or restarted for continuations)
#         self.start_time: float = start_time
#         # time at which the job ends
#         self.end_time: float = end_time
#
#         # list of servers that are executing the job
#         self.servers: List[Server] = servers
#         # frequency at which the job is executed
#         self.frequency: float = frequency
#         # minimum number of servers executing the job
#         self.min_server_count: int = min_server_count
#         # maximum number of servers executing the job
#         self.max_server_count: int = max_server_count
#
#         # function to compute the speed up factor on n servers
#         self.speed_up_factor: Callable[[int], float] = speed_up_factor
#         # function to compute the slow down factor when run at frequency f
#         self.slow_down_factor: Callable[[float], float] = slow_down_factor
#
#     def __repr__(self):
#         return f"{self.id}: mass: {self.mass}, start: {self.start_time}, end: {self.end_time}"
#
#     @classmethod
#     def from_request(cls, time: int, request: Request, servers: List[Server], frequency: float = None):
#         assert 1 <= request.min_server_count <= len(servers) <= request.max_server_count
#
#         # frequency defaults to that of the first server in the list
#         if frequency is None:
#             frequency = servers[0].get_frequency()
#
#         harmonization_jobs = []
#         turn_on_jobs = []
#
#         # computes the startup and frequency harmonization jobs
#         for server in servers:
#             # creates the startup job
#             turn_on_job = server.add_startup(time)
#             if turn_on_job is not None:
#                 turn_on_jobs.append(turn_on_job)
#
#             time_harmonization = time + turn_on_job.end_time if turn_on_job is not None else 0
#
#             # creates the frequency harmonization job
#             harmonization_job = server.apply_dvfs(time_harmonization, frequency)
#             if harmonization_job is not None:
#                 harmonization_jobs.append(harmonization_job)
#
#         time_work = max(max(turn_on_jobs, key=lambda j: j.end_time).end_time if len(turn_on_jobs) > 0 else time, max(harmonization_jobs, key=lambda j: j.end_time).end_time if len(harmonization_jobs) > 0 else time)
#
#         # computes the work job
#         work_job = cls(request.id, Job.Kind.WORK, request.data, request.mass, request.submission_time, time_work, time_work + cls.execution_time(request.mass, len(servers), frequency, request.speed_up_factor, request.slow_down_factor), servers, frequency, request.min_server_count, request.max_server_count, request.speed_up_factor, request.slow_down_factor)
#
#         return turn_on_jobs, harmonization_jobs, work_job
#
#     @staticmethod
#     def execution_time(mass: float, servers: int, frequency: float, speed_up_factor: Callable[[int], float], slow_down_factor: Callable[[float], float]):
#         return mass / (slow_down_factor(frequency) * speed_up_factor(servers))
#
#     def interrupt(self, time):
#         assert time <= self.end_time
#         assert time >= self.submission_time
#
#         self.end_time = time
#
#         for server in self.servers:
#             server.remove_job(self)
#
#     def is_started(self, time):
#         return self.start_time <= time
#
#     def is_complete(self, time):
#         return self.end_time <= time
#
#     def is_running(self, time):
#         return self.is_started(time) and not self.is_complete(time)
#
#     def is_turning_off(self):
#         return self.kind == Job.Kind.SYSTEM and self.id == Job.IDs.TURNING_OFF
#
#     def is_turning_on(self):
#         return self.kind == Job.Kind.SYSTEM and self.id == Job.IDs.TURNING_ON
#
#     def is_reconfiguration(self):
#         return self.kind == Job.Kind.RECONFIGURATION
#
#     def is_work(self):
#         return self.kind == Job.Kind.WORK
#
#     def executed_mass(self, time):
#         return self.mass * ((time - self.start_time) / (self.end_time - self.start_time))
#
#     def remaining_mass(self, time):
#         return self.mass - self.executed_mass(time)
#
#     def reconfiguration_time(self, servers: List[Server]):
#         maxi = max(len(self.servers), len(servers))
#         mini = min(len(self.servers), len(servers))
#
#         return self.data / maxi * floor(maxi / mini)
#
#     def is_reconfigurable(self, time):
#         # a job is reconfigurable if it is a work job and it is currently running
#         return self.kind == Job.Kind.WORK and self.is_running(time)
#
#     def is_reconfigurable_to(self, time, servers: List[Server]):
#         # a job is reconfigurable to a number of servers within its allowed range
#         # ! the scheduler is responsible for checking whether or not the servers are turned on and available
#         return self.is_reconfigurable(time) and self.min_server_count <= len(servers) <= self.max_server_count
#
#     def build_reconfiguration(self, time, servers: List[Server], frequency: float = None):
#         # frequency defaults to the current one
#         if frequency is None:
#             frequency = self.frequency
#
#         # build the list of servers that will require a reconfiguration time
#         # this is the union of the old servers and the new servers
#         servers_under_reconfiguration = list(set().union(servers, self.servers))
#
#         turn_on_jobs = []
#         harmonization_jobs = []
#
#         # computes the startup and frequency harmonization jobs
#         for server in servers:
#             # creates the startup job
#             turn_on_job = server.add_startup(time)
#             if turn_on_job is not None:
#                 turn_on_jobs.append(turn_on_job)
#
#             time_harmonization = time + turn_on_job.end_time if turn_on_job is not None else 0
#
#             # creates the frequency harmonization job
#             harmonization_job = server.apply_dvfs(time_harmonization, frequency)
#             if harmonization_job is not None:
#                 harmonization_jobs.append(harmonization_job)
#
#         time_reconfiguration = max(max(turn_on_jobs, key=lambda j: j.end_time).end_time if len(turn_on_jobs) > 0 else time, max(harmonization_jobs, key=lambda j: j.end_time).end_time if len(harmonization_jobs) > 0 else time)
#
#         # computes the reconfiguration job
#         reconfiguration_job = Job(self.id, Job.Kind.RECONFIGURATION, 0, 0, self.submission_time, time_reconfiguration, time_reconfiguration + self.reconfiguration_time(servers), servers_under_reconfiguration, 1, len(servers_under_reconfiguration), len(servers_under_reconfiguration), self.speed_up_factor, self.slow_down_factor)
#
#         work_job_mass = self.remaining_mass(time)
#         work_job_duration = Job.execution_time(work_job_mass, len(servers), frequency, self.speed_up_factor, self.slow_down_factor)
#
#         # computes the work job
#         work_job = Job(self.id, Job.Kind.WORK, self.data, work_job_mass, self.submission_time, reconfiguration_job.end_time, reconfiguration_job.end_time + work_job_duration, servers, frequency, self.min_server_count, self.max_server_count, self.speed_up_factor, self.slow_down_factor)
#
#         return turn_on_jobs, harmonization_jobs, reconfiguration_job, work_job
#
#     def export_csv(self):
#         return [[self.id, self.kind.value, self.data, self.mass, self.submission_time, self.start_time, self.end_time, server.id] for server in self.servers]
#
#
# class _Job:
#     def __init__(self, job_id: str, time_beg: float, time_end: float, servers: List[Server]):
#         assert time_beg < time_end
#         assert len(servers) > 0
#
#         # unique id of the job
#         self.id = job_id
#
#         # time at which the job starts and ends
#         self.time_beg = time_beg
#         self.time_end = time_end
#
#         # time at which the job actually ended (different from self.time_end if the job is interrupted for some reason)
#         # will stay None until the job actually ends
#         self.time_end_real = None
#
#         # the servers on which the job is executing
#         self.servers = servers
#
#     def is_pending(self, time: float):
#         # Returns true if the job is pending.
#         #
#         # A job is pending if the time is before {self.time_beg}.
#
#         return self.time_beg > time
#
#     def is_executing(self, time: float):
#         # Returns true if the job is executing.
#         #
#         # A job is executing if it is not marked as interrupted or complete and if the time is between {self.time_beg} and {self.time_end}.
#
#         return self.time_end_real is None and self.time_beg <= time < self.time_end
#
#     def is_over(self, time: float):
#         # Returns true if the job is over.
#         #
#         # A job is over if it is marked as completed or interrupted or if the time is past {self.time_end}.
#
#         return time >= self.time_end if self.time_end_real is None else self.time_end_real
#
#     def is_completed(self):
#         # Returns true if the job is marked completed.
#         #
#         # Panics if the job is not over.
#         #
#         # An over job is considered completed if it has finished all its work.
#
#         assert self.time_end_real is not None
#
#         return self.time_end == self.time_end_real
#
#     def is_interrupted(self):
#         # Returns true if the job is marked interrupted.
#         #
#         # Panics if the job is not over.
#         #
#         # An over job is considered interrupted if it has not finished all its work.
#
#         assert self.time_end_real is not None
#
#         return self.time_end != self.time_end_real
#
#     def release_servers(self):
#         # Releases the servers that this job used.
#         #
#         # The job must be over.
#         #
#         # ! This function must not be called more than once per job.
#
#         assert self.time_end_real is not None
#
#         for server in self.servers:
#             server.remove_job(self)
#
#     def complete(self, time: float):
#         # Marks the job as completed.
#         #
#         # The job must not be marked completed or interrupted.
#         # `time` must be no earlier than `self.time_end`.
#         #
#         # ! Releasing the servers that this job used is of the scheduler's responsibility.
#
#         assert self.time_end_real is None
#         assert self.time_end <= time
#
#         # marks the job as complete
#         self.time_end_real = self.time_end
#
#     def interrupt(self, time: float):
#         # Marks the job as interrupted.
#         #
#         # The job must not be marked as completed or interrupted.
#         # {time} must be between {self.time_beg} and {self.time_end}.
#         #
#         # ! Releasing the servers that this job used is of the scheduler's responsibility.
#
#         assert self.time_end_real is None
#         assert self.time_beg < time < self.time_end
#
#         # marks the job as interrupted
#         self.time_end_real = time
#
#
# class JobWork(_Job):
#     def __init__(self, job_id: str, time_beg: float, mass: float, data: float, servers: List[Server], frequency: float, min_servers: int, max_servers: int, malleability_factor: Callable[[int], float], frequency_factor: Callable[[float], float]):
#         assert mass > 0
#         assert data >= 0
#         assert len(servers) > 0
#         assert frequency > 0
#
#         for server in servers:
#             assert server.get_frequency() == frequency
#
#         time_end = time_beg + mass * malleability_factor(len(servers)) * frequency_factor(frequency)
#
#         super().__init__(job_id, time_beg, time_end, servers)
#
#         # mass represents the amount of work required if the job is executed on a single server at frequency 1
#         self.mass = mass
#         # data represents the amount of data transferred during a reconfiguration
#         self.data = data
#
#         self.min_servers = min_servers
#         self.max_servers = max_servers
#
#         # these factors are used to compute the impact of malleability and dvfs
#         self.malleability_factor = malleability_factor
#         self.frequency_factor = frequency_factor
#
#     def reconfigure(self, time: float, servers: List[Server]):
#         # Build and returns a reconfiguration for this job.
#         #
#         # The job must be reconfigurable at the specified time on the specified servers.
#
#         assert self.is_reconfigurable(time, servers)
#
#         return JobReconfiguration(self, time, servers)
#
#     def is_reconfigurable(self, time: float, servers: List[Server]):
#         # Checks whether or not the job is reconfigurable at the specified time on the specified servers.
#         #
#         # For a job to be reconfigurable, the count of the new servers must be between `self.min_servers` and `self.max_servers`.
#
#         # the job has not started executing
#         if not self.is_executing(time):
#             return False
#
#         # new server count is out of bound
#         if not self.min_servers <= len(servers) <= self.max_servers:
#             return False
#
#         for server in servers:
#             # server is turned off
#             if server.is_turned_off():
#                 return False
#
#             # server is busy
#             if server not in self.servers and server.is_busy():
#                 return False
#
#         return True
#
#     def remaining_mass(self, time: float):
#         # job has not started executing, all mass remains
#         if self.is_pending(time):
#             return self.mass
#
#         if self.is_over(time):
#             # job is marked as completed, remaining mass is 0
#             if self.is_completed():
#                 return 0
#
#             # job is marked as interrupted, compute remaining mass based on `self.time_end_real`
#             if self.is_interrupted():
#                 return self.mass * (self.time_end_real - self.time_beg) / (self.time_end - self.time_beg)
#
#             # should not reach this
#             raise RuntimeError
#         else:
#             # job is still executing, compute remaining mass based on `time`
#             return self.mass * (time - self.time_beg) / (self.time_end - self.time_beg)
#
#
# class JobReconfiguration(_Job):
#     def __init__(self, job: JobWork, time_beg: float, servers: List[Server]):
#         for server in servers:
#             assert server.is_turned_on()
#             assert server in job.servers or not server.is_busy()
#
#         duration = JobReconfiguration.duration(job.data, len(job.servers), len(servers))
#
#         super().__init__(job.id, time_beg, time_beg + duration, servers)
#
#         self.origin = job
#         #                          (job_id: str, time_beg: float, mass: float,                  data: float, servers: List[Server], frequency: float, min_servers: int, max_servers: int, malleability_factor: Callable[[int], float], frequency_factor: Callable[[float], float])
#         self.continuation = JobWork(job.id,      self.time_end,   job.remaining_mass(time_beg), job.data,    servers,               )
#
#     @staticmethod
#     def duration(data: float, ante: int, post: int):
#         maxi = max(ante, post)
#         mini = min(ante, post)
#
#         return data / maxi * floor(maxi / mini)
