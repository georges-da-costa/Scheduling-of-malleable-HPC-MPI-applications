from simulator.job import Job
from simulator.scheduler import Scheduler


class SchedulerFIFO(Scheduler):
    NAME = "FIFO"

    def update(self, time):
        pass

    def remove_completed_jobs(self, time):
        for job in filter(lambda j: j.is_complete(time), self.active_jobs):
            self.remove_job(time, job)

    def schedule_pending_jobs(self, time):
        while len(self.request_queue) > 0 and len(self.available_servers()) > 0:
            turn_on_jobs, harmonization_jobs, work_job = Job.from_request(time, self.request_queue.pop(), [self.available_servers()[-1]])

            self.add_jobs(turn_on_jobs)
            self.add_jobs(harmonization_jobs)
            self.add_job(work_job)
