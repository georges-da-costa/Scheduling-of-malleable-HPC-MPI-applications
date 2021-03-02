from random import sample

from simulator.scheduler import SchedulerReconfig
from simulator.scheduler.concrete import SchedulerFIFO


class SchedulerFIFOReconfigUp(SchedulerReconfig, SchedulerFIFO):
    NAME = "FIFORECONFUP"

    def update(self, time):
        self.remove_completed_jobs(time)
        self.schedule_pending_jobs(time)
        self.reconfigure_up_jobs(time)

    def reconfigure_up_jobs(self, time):
        if len(self.available_servers()) > 0:
            for job in filter(lambda j: j.is_reconfigurable(time) and time - j.start_time > 100, self.active_jobs):
                new_servers = job.servers.copy()
                new_servers.extend(sample(self.available_servers(), 1))
                if job.is_reconfigurable_to(time, new_servers):
                    self.reconfigure_job(time, job, new_servers)
                    if len(self.available_servers()) == 0:
                        break
