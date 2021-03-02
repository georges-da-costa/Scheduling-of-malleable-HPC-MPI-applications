from simulator.job import JobWork
from simulator.scheduler.SchedulerNew import Scheduler


class SchedulerFIFOMAX(Scheduler):
    # Schedules the job on the maximum number of servers based on a FIFO order.

    NAME = "FIFOMAX"

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

            self.add_job(JobWork.from_request(time, self.request_queue.pop(0), self.sample_servers(min(request.max_server_count, len(servers_available)), *servers_available)))
            servers_available = self.get_servers_available()
