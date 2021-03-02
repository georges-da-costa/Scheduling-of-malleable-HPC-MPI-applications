from simulator.job import JobWork
from simulator.scheduler.SchedulerNew import Scheduler


class SchedulerFIFOMIN(Scheduler):
    # Schedules the job on the minimum number of servers based on a FIFO order.
    # Servers that have no assigned jobs after the schedule are turned off.
    # If requests are pending and not enough turned on servers are available, turned off servers are turned on.

    NAME = "FIFOMIN"

    def update(self, time: float):
        # removes over jobs
        for job in filter(lambda j: j.is_over(time), self.jobs_active):
            self.complete_jobs(time, job)
            self.remove_job(job)

        # schedules new jobs on available servers by order of submission time
        servers_available = self.get_servers_available()
        while len(self.request_queue) > 0 and len(servers_available) > 0:
            request = self.request_queue[0]

            if request.min_server_count > len(servers_available):
                break

            self.add_job(JobWork.from_request(time, self.request_queue.pop(0), self.sample_servers(request.min_server_count, *servers_available)))
            servers_available = self.get_servers_available()

        if len(self.request_queue) > 0:
            # + turn on servers if they can be used to schedule a pending request
            pass
        else:
            self.turn_servers_off(time, *self.get_servers_available())
