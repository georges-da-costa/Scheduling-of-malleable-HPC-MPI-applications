from random import sample

from simulator.job import Job
from simulator.scheduler.concrete import SchedulerFIFO


class SchedulerFIFOMax(SchedulerFIFO):
    NAME = "FIFOMAX"

    def schedule_pending_jobs(self, time):
        while len(self.request_queue) > 0 and len(self.available_servers()) > 0:
            request = self.request_queue.pop(0)

            turn_on_jobs, harmonization_jobs, work_job = Job.from_request(time, request, sample(self.available_servers(), min(len(self.available_servers()), request.max_server_count)))

            self.add_jobs(turn_on_jobs)
            self.add_jobs(harmonization_jobs)
            self.add_job(work_job)
