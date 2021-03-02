from random import sample

from simulator.scheduler import SchedulerReconfig
from simulator.scheduler.concrete import SchedulerFIFOMax


class SchedulerFIFOReconfigDown(SchedulerReconfig, SchedulerFIFOMax):
    NAME = "FIFORECONFDOWN"

    def update(self, time):
        self.remove_completed_jobs(time)
        self.reconfigure_down_jobs(time)
        self.schedule_pending_jobs(time)

    def reconfigure_down_jobs(self, time):
        if len(self.available_servers()) < len(self.request_queue):
            for job in filter(lambda j: j.is_reconfigurable(time) and time - j.start_time > 100, self.active_jobs):
                new_servers = sample(job.servers, len(job.servers) - 1)
                if job.is_reconfigurable_to(time, new_servers):
                    self.reconfigure_job(time, job, new_servers)
