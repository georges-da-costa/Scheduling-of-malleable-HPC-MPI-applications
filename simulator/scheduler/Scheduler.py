import abc
import math

from typing import List, Dict
from csv import DictWriter as CSVWriter

from simulator.job import Job, Request as JobRequest, JobSystem, JobSystemTurnOn, JobSystemTurnOff
from simulator.server import Server


class Scheduler(abc.ABC):
    def __init__(self, server_count: int, args):
        self.servers: List[Server] = [Server(str(i), [1.0, 3.0]) for i in range(server_count)]

        self.turned_on_servers: List[Server] = [server for server in self.servers if server.is_turned_on(0)]
        self.turned_off_servers: List[Server] = [server for server in self.servers if server.is_turned_off(0)]

        self.request_queue: List[JobRequest] = []

        self.active_jobs: List[Job] = []
        self.complete_jobs: Dict[str, List[Job]] = {JobSystem.ID: []}

        self.time_next_departure = None

    @abc.abstractmethod
    def update(self, time): ...

    def schedule(self, job_request: JobRequest):
        """Schedules a JobRequest."""

        # adds the request to the queue
        if len(self.request_queue) == 0:
            # queue is empty, insert is valid
            self.request_queue.append(job_request)
        else:
            # queue is not empty, using insertion sort
            index_beg = 0
            index_end = len(self.request_queue)

            while index_beg != index_end:
                index = math.floor((index_beg + index_end) / 2)

                if self.request_queue[index].submission_time < job_request.submission_time:
                    index_beg = index + 1
                elif self.request_queue[index].submission_time > job_request.submission_time:
                    index_beg = index - 1
                else:
                    break

            self.request_queue.insert(math.floor((index_beg + index_end) / 2), job_request)

        # adds future job id to completed job list
        self.complete_jobs[job_request.id] = []

    def stop(self, time):
        """Interrupts all the work jobs and scraps the pending jobs."""

        # scraps pending jobs
        self.request_queue.clear()

        # interrupts work jobs
        # work jobs are always "last" in their respective servers, so there is no need to change the starting time of non-work jobs
        for server in self.servers:
            for job in server.jobs:
                if job.is_work():
                    self.remove_job(time, job)

        # update self.time_next_departure
        self.time_next_departure = None

    def add_jobs(self, jobs: List[Job]):
        # Adds a list of jobs.
        #
        # See `self.add_job`.

        for job in jobs:
            self.add_job(job)

    def add_job(self, job: Job):
        # Adds a job to the scheduler.

        # adds the job to its servers
        for server in job.servers:
            server.add_job(job)

        # adds the job to the active jobs list
        self.active_jobs.append(job)

        # update self.time_next_departure
        if self.time_next_departure is None or self.time_next_departure > job.time_end:
            self.time_next_departure = job.time_end

    def remove_jobs(self, time: float, jobs: List[Job]):
        # Removes a list of jobs.
        #
        # See `self.remove_job`.

        for job in jobs:
            self.remove_job(time, job)

    def remove_job(self, time: float, job: Job):
        # Removes a job from the scheduler.
        #
        # This functions terminates the job and releases its servers.

        job.terminate(time)
        job.release_servers()

        # remove job from active jobs list
        self.active_jobs.remove(job)

        # append job to the corresponding completed jobs list
        self.complete_jobs[job.id].append(job)

        # update self.time_next_departure
        if job.time_end == self.time_next_departure:
            if len(self.active_jobs) == 0:
                self.time_next_departure = None
            else:
                self.time_next_departure = min(self.active_jobs, key=lambda j: j.time_end).time_end

    def get_servers_turned_on(self, time: float):
        # Returns the list of turned on servers.

        return [server for server in self.servers if server.is_turned_on(time)]

    def get_servers_turned_off(self, time: float):
        # Returns the list of turned off servers.

        return [server for server in self.servers if server.is_turned_off(time)]

    def get_servers_available(self, time: float):
        # Returns the list of available servers.

        return [server for server in self.turned_on_servers if not server.is_busy(time)]

    def is_working(self):
        # Returns true if the scheduler is working.
        #
        # A scheduler is working if it has active jobs or pending requests.

        return len(self.request_queue) > 0 or len(self.active_jobs) > 0

    def turn_on_servers(self, time: float, servers: List[Server]):
        # Turns on the servers.

        for server in servers:
            self.turn_on_server(time, server)

    def turn_on_server(self, time: float, server: Server):
        # Turns on the server.

        assert server.is_turnable_on(time)

        self.add_job(JobSystemTurnOn(time, server))

    def turn_off_servers(self, time: float, servers: List[Server]):
        # Turns off the servers.

        for server in servers:
            self.turn_off_server(time, server)

    def turn_off_server(self, time: float, server: Server):
        # Turns off the server.

        assert server.is_turnable_off(time)

        self.add_job(JobSystemTurnOff(time, server))

    def are_servers_turnable_on(self, time: float, servers: List[Server]):
        # Returns true if the servers are turnable on.

        for server in servers:
            if not server.is_turnable_on(time):
                return False

        return True

    def are_servers_turnable_off(self, time: float, servers: List[Server]):
        # Returns true if the servers are turnable off.

        for server in servers:
            if not server.is_turnable_off(time):
                return False

        return True

    def stats(self, writer: CSVWriter):
        for jobs in self.complete_jobs.values():
            for job in jobs:
                writer.writerows(job.export_csv())
