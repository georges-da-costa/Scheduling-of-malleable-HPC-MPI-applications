from simulator.job import Job
from simulator.scheduler import SchedulerReboot


class SchedulerFIFOReboot(SchedulerReboot):
    NAME = "FIFOREBOOT"

    def __init__(self, server_count: int, args):
        super().__init__(server_count, args)

    def remove_completed_jobs(self, time):
        for job in filter(lambda j: j.is_complete(time), self.active_jobs):
            self.remove_job(time, job)
            if job.kind != Job.Kind.SYSTEM and job.kind != Job.Kind.RECONFIGURATION:
                self.shutdown_servers(time, job.servers)
                self.startup_servers(time + 2000, job.servers)

    def schedule_pending_jobs(self, time):
        while len(self.request_queue) > 0 and len(self.available_servers()) > 0:
            turn_on_jobs, harmonization_jobs, work_job = Job.from_request(time, self.request_queue.pop(), [self.available_servers()[-1]])

            self.add_jobs(turn_on_jobs)
            self.add_jobs(harmonization_jobs)
            self.add_job(work_job)

    def update(self, time):
        self.remove_completed_jobs(time)
        self.schedule_pending_jobs(time)
