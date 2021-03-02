from simulator.job import JobWork
from simulator.scheduler import Scheduler


class SchedulerFIFO(Scheduler):
    NAME = "FIFO"

    def update(self, time):
        # removes the completed jobs
        for job in filter(lambda j: j.is_over(time), self.active_jobs):
            self.remove_job(time, job)

        # schedules new jobs on available servers by order of submission time
        while len(self.request_queue) > 0 and len(self.get_servers_available(time)) > 0:
            self.add_job(JobWork.from_request(time, self.request_queue.pop(0), [self.get_servers_available(time)[0]]))
