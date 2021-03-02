from typing import List


def dummy_factor(_):
    return 1


class Server:
    DURATION_FREQUENCY_CHANGE = 10
    DURATION_SHUTDOWN = 500
    DURATION_STARTUP = 250

    @classmethod
    def generate_n(cls, n: int, frequencies: List[float] = None, begin_on: bool = True, begin_freq_index: int = 0):
        # Generates `n` identical servers.

        return [cls(str(i), frequencies, begin_on, begin_freq_index) for i in range(n)]

    def __init__(self, server_id: str, frequencies: List[float] = None, begin_on: bool = True, begin_freq_index: int = 0):
        from simulator.job import Job, JobSystemTurnOn, JobSystemTurnOff, JobSystemFrequency

        self.id = server_id

        # a server begins without any jobs assigned
        self.jobs: List[Job] = []

        # defaults to a single frequency
        if frequencies is None:
            self.frequencies = [1.0]
        else:
            self.frequencies = frequencies

        # frequencies are sorted in ascending order to simplify computations
        self.frequencies.sort()

        assert 0 <= begin_freq_index < len(self.frequencies)

        # stores the beginning state of the server
        self.begin_on = begin_on
        self.begin_frequency = self.frequencies[begin_freq_index]

        # stores the turn on, turn off and frequency jobs to keep track of the server's status
        self.jobs_turn_on: List[JobSystemTurnOn] = []
        self.jobs_turn_off: List[JobSystemTurnOff] = []
        self.jobs_frequency: List[JobSystemFrequency] = []

    def __repr__(self):
        return f"Server-{self.id}"

    def get_frequency(self, time: float):
        # Returns the frequency at which the server is operating at during the specified time.

        # if frequency changes occurred, the frequency is that of the latest to be over at time `time`
        for job in reversed(self.jobs_frequency):
            if job.is_over(time):
                return job.frequency

        # if no frequency change occurred, the frequency is the beginning frequency
        return self.begin_frequency

    def time_turn_on(self, time: float):
        # Returns the time of the latest turn on job before `time`.

        for job in reversed(self.jobs_turn_on):
            if job.time_beg <= time:
                return job.time_beg

        return None

    def time_turn_off(self, time: float):
        # Returns the time of the latest turn off job before `time`.

        for job in reversed(self.jobs_turn_off):
            if job.time_beg <= time:
                return job.time_beg

        return None

    def is_turned_on(self, time):
        # Returns True if the server is turned on at the specified time.

        on = self.time_turn_on(time)
        off = self.time_turn_off(time)

        if on is None:
            on = -1 if self.begin_on else -2

        if off is None:
            off = -2 if self.begin_on else -1

        return on > off

    def is_turned_off(self, time):
        # Returns True if the server is turned off at the specified time.

        on = self.time_turn_on(time)
        off = self.time_turn_off(time)

        if on is None:
            on = -1 if self.begin_on else -2

        if off is None:
            off = -2 if self.begin_on else -1

        return on < off

    def create_dvfs(self, time: float, frequency: float):
        # Returns a system frequency change.
        # None is returned if the frequency is the same
        #
        # ! This function does not call `self.add_job`

        from simulator.job import JobSystemFrequency

        if frequency == self.get_frequency(time):
            return None

        job = JobSystemFrequency(time, self, frequency)

        self.jobs_frequency.append(job)

        return job

    def create_turn_on(self, time: float):
        # Create a turn on job.
        # None is returned if the server is on at the time.
        #
        # ! This function does not call `self.add_job`

        from simulator.job import JobSystemTurnOn

        if self.is_turned_on(time):
            return None

        job = JobSystemTurnOn(time, self)

        self.jobs_turn_on.append(job)

        return job

    def create_turn_off(self, time: float):
        # Create a turn off job.
        # None is returned if the server is off at the time.
        #
        # ! This function does not call `self.add_job`

        from simulator.job import JobSystemTurnOff

        if self.is_turned_off(time):
            return None

        job = JobSystemTurnOff(time, self)

        self.jobs_turn_off.append(job)

        return job

    def add_job(self, job):
        # Adds a job to the server's internal list.

        for j in self.jobs:
            assert job.time_beg >= j.time_end or job.time_end <= j.time_beg

        self.jobs.append(job)
        self.jobs.sort(key=lambda _j: _j.time_beg)

    def remove_job(self, job):
        # Removes a job from the server's internal list.

        self.jobs.remove(job)

    def is_busy(self, time: float):
        # Returns true if the server is executing a job at the specified time.

        for job in self.jobs:
            if not job.is_over(time):
                return True

        return False

    def is_frequency_settable(self, time: float, frequency: float):
        if self.is_turned_off(time):
            return False

        if self.is_busy(time):
            return False

        if frequency not in self.frequencies:
            return False

        return True

    def max_frequency(self):
        # Returns the maximum frequency of the server.

        return max(self.frequencies)

    def min_frequency(self):
        # Returns the minimum frequency of the server.

        return min(self.frequencies)

    def is_turnable_on(self, time: float):
        # Returns true if the server is turnable on.

        return self.is_turned_off(time)

    def is_turnable_off(self, time: float):
        # Returns true if the server is turnable off.

        return self.is_turnable_on(time) and len(self.jobs) == 0

    def duration_turn_on(self):
        # Returns the duration of a turn on job for the server.

        return Server.DURATION_STARTUP

    def duration_turn_off(self):
        # Returns the duration of a turn off job for the server.

        return Server.DURATION_SHUTDOWN

    def duration_frequency(self):
        # Returns the duration of a frequency change job for the server.

        return Server.DURATION_FREQUENCY_CHANGE
