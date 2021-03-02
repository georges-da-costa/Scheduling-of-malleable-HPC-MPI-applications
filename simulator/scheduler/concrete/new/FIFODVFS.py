from simulator.job import JobWork
from simulator.scheduler.SchedulerNew import Scheduler


class SchedulerFIFODVFS(Scheduler):
    # Schedules the job on the minimum number of servers based on a FIFO order at the lowest frequency except for the long jobs that are scheduled at the highest frequency..

    NAME = "FIFODVFS"

    MASS_THRESHOLD = 500

    def update(self, time: float):
        # removes over jobs
        for job in filter(lambda j: j.is_over(time), self.jobs_active):
            self.complete_jobs(time, job)
            self.remove_job(job)

        # schedules new jobs on available servers by order of submission time
        servers_available = self.get_servers_available()
        while len(self.request_queue) > 0 and len(servers_available) > 0:
            request = self.request_queue[0]

            if not (request.min_server_count <= len(servers_available) <= request.max_server_count):
                break

            servers = self.sample_servers(min(request.max_server_count, len(servers_available)), *servers_available)

            jobs_frequency = None

            if request.mass > SchedulerFIFODVFS.MASS_THRESHOLD:
                jobs_frequency = self.set_server_frequency_max(time, servers)
            else:
                jobs_frequency = self.set_server_frequency_min(time, servers)

            self.add_job(JobWork.from_request(max(jobs_frequency, key=lambda _j: _j.time_end) if len(jobs_frequency) > 0 else time, self.request_queue.pop(0), servers))

            servers_available = self.get_servers_available()
