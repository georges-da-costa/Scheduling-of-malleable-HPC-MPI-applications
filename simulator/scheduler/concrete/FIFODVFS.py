from random import sample

from simulator.job import Job
from simulator.scheduler import SchedulerDVFS
from simulator.scheduler.concrete import SchedulerFIFO


class SchedulerFIFODVFS(SchedulerDVFS, SchedulerFIFO):
    NAME = "FIFODVFS"

    def update(self, time):
        self.remove_completed_jobs(time)
        self.schedule_pending_jobs(time)

    def schedule_pending_jobs(self, time):
        while len(self.request_queue) > 0 and len(self.available_servers()) > 0:
            servers = [self.available_servers()[-1]]

            turn_on_jobs, harmonization_jobs, work_job = Job.from_request(time, self.request_queue.pop(), servers, sample(servers[0].frequencies, 1)[0])

            self.add_jobs(turn_on_jobs)
            self.add_jobs(harmonization_jobs)
            self.add_job(work_job)
