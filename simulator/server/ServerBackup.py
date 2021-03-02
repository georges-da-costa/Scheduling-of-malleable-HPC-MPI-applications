# from typing import List
#
#
# def dummy_factor(_):
#     return 1
#
#
# class Server:
#     DURATION_FREQUENCY_CHANGE = 10
#     DURATION_SHUTDOWN = 500
#     DURATION_STARTUP = 250
#
#     @classmethod
#     def generate_n(cls, n: int, frequencies: List[float] = None, begin_on: bool = True, begin_freq_index: int = 0):
#         """Generates {n} identical servers."""
#         return [cls(str(i), frequencies, begin_on, begin_freq_index) for i in range(n)]
#
#     def __init__(self, server_id: str, frequencies: List[float] = None, begin_on: bool = True, begin_freq_index: int = 0):
#         from simulator.job import Job
#
#         self.id = server_id
#
#         # a server begins without assigned jobs
#         self.jobs: List[Job] = []
#
#         # defaults to a single frequency
#         if frequencies is None:
#             self.frequencies = [1.0]
#         else:
#             self.frequencies = frequencies
#
#         # frequencies are sorted in ascending order to simplify computations
#         self.frequencies.sort()
#
#         assert 0 <= begin_freq_index < len(self.frequencies)
#
#         # a server has a starting frequency, which is by default the lowest available
#         self.time_frequency = [(0, self.frequencies[begin_freq_index])]
#
#         self.begin_on = begin_on
#
#         # self.time_startup contains the times at which the server is considered on (start_time of the turn_on jobs)
#         # self.time_shutdown contains the times at which the server is considered off (end_time of the turn_off jobs)
#         # ! these times do not reflect self.begin_on to avoid confusion if a turn_on job is issued on a server at time 0
#         self.time_startup = []
#         self.time_shutdown = []
#
#     def __repr__(self):
#         return f"Server-{self.id}{self.frequencies}: {self.jobs}"
#
#     def get_frequency(self):
#         """Returns the frequency at which the server is operating, or will operate if a dvfs_job is running."""
#         return self.time_frequency[-1][1]
#
#     def apply_dvfs(self, time: int, frequency: float):
#         """
#         Changes the frequency of the server and returns the corresponding dvfs_job.
#         The frequency must be supported by the server.
#         If the current and chosen frequencies are the same, the server will not create a dvfs_job and will return {None}.
#         """
#         from simulator.job import Job
#
#         assert frequency in self.frequencies
#
#         # frequency is the same, no-op
#         if self.time_frequency[-1][1] == frequency:
#             return None
#
#         self.time_frequency.append((time + Server.DURATION_FREQUENCY_CHANGE, frequency))
#
#         dvfs_job = Job(Job.IDs.DVFS.value, Job.Kind.SYSTEM, 0, 0, time, time, time + Server.DURATION_FREQUENCY_CHANGE, [self], frequency, 1, 1, dummy_factor, dummy_factor)
#
#         return dvfs_job
#
#     def add_startup(self, time: int):
#         """
#         Turns on the server and returns the corresponding turn_on_job.
#         If the server is already on, the server will not create a turn_on_job and will return {None}.
#         """
#         from simulator.job import Job
#
#         # server is already on, no-op
#         if self.is_turned_on():
#             return None
#
#         self.time_startup.append(time)
#
#         startup_job = Job(Job.IDs.TURNING_ON.value, Job.Kind.SYSTEM, 0, 0, time, time, time + Server.DURATION_STARTUP, [self], self.time_frequency[-1][1], 1, 1, dummy_factor, dummy_factor)
#
#         return startup_job
#
#     def add_shutdown(self, time: int):
#         """
#         Turns off the server and returns the turn_off_job.
#         If the server is already off, the server will not create a turn_off_job and will return {None}.
#         """
#         from simulator.job import Job
#
#         # server is already off, no-op
#         if self.is_turned_off():
#             return None
#
#         self.time_shutdown.append(time + Server.DURATION_SHUTDOWN)
#
#         shutdown_job = Job(Job.IDs.TURNING_OFF.value, Job.Kind.SYSTEM, 0, 0, time, time, time + Server.DURATION_SHUTDOWN, [self], self.time_frequency[-1][1], 1, 1, dummy_factor, dummy_factor)
#
#         return shutdown_job
#
#     def add_job(self, job):
#         self.jobs.append(job)
#
#     def remove_job(self, job):
#         self.jobs.remove(job)
#
#     def is_busy(self):
#         return len(self.jobs) > 0
#
#     def is_turned_off(self):
#         if len(self.time_startup) == 0 and len(self.time_shutdown) == 0:
#             return not self.begin_on
#
#         last_startup = self.time_startup[-1] if len(self.time_startup) > 0 else 0
#         last_shutdown = self.time_shutdown[-1] if len(self.time_shutdown) > 0 else 0
#
#         return last_startup < last_shutdown
#
#     def is_turned_on(self):
#         if len(self.time_startup) == 0 and len(self.time_shutdown) == 0:
#             return self.begin_on
#
#         last_startup = self.time_startup[-1] if len(self.time_startup) > 0 else 0
#         last_shutdown = self.time_shutdown[-1] if len(self.time_shutdown) > 0 else 0
#
#         return last_startup > last_shutdown
#
#     def duration_turn_on(self):
#         pass
#
#     def duration_turn_off(self):
#         pass
#
#     def duration_frequency(self):
#         pass
