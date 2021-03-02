from simulator.scheduler.concrete import SchedulerFIFOReconfigDown, SchedulerFIFOReconfigUp


class SchedulerFIFOReconfig(SchedulerFIFOReconfigDown, SchedulerFIFOReconfigUp):
    NAME = "FIFORECONF"

    def update(self, time):
        self.remove_completed_jobs(time)
        self.reconfigure_down_jobs(time)
        self.schedule_pending_jobs(time)
        self.reconfigure_up_jobs(time)
