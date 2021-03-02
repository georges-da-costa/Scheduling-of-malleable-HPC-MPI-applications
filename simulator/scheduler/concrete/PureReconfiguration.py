import math

from simulator.job import JobWork, JobReconfiguration, Request
from simulator.scheduler import Scheduler


class SchedulerPureReconfiguration(Scheduler):
    NAME = "PURE"

    def update(self, time: float):
        # removes the completed jobs
        for job in filter(lambda _j: _j.is_over(time), self.active_jobs):
            self.remove_job(time, job)

        # list of all servers that are to be used by the scheduler
        servers = self.servers
        servers_utilisation = [False] * len(servers)

        # list of jobs that are to be executed or are executing before the reconfiguration
        jobs_executing = [job for job in self.active_jobs if isinstance(job, JobWork)]
        jobs_executing.sort(key=lambda _j: int(_j.id[3:]))

        max_reconfiguration_end = time

        # list of jobs to be added during this reconfiguration
        jobs_pending = []
        for i in range(0, (len(servers) - len(jobs_executing))):
            if len(self.request_queue) == 0:
                break
            jobs_pending.append(self.request_queue.pop())
        jobs_pending.sort(key=lambda _j: int(_j.id[3:]))

        # schedule the jobs
        for i in range(0, len(jobs_executing)):
            # computes number of servers to execute on
            number_of_servers = math.floor(len(servers) / (len(jobs_executing) + len(jobs_pending)))
            if len(servers) % (len(jobs_executing) + len(jobs_pending)) > i:
                number_of_servers += 1

            # number of server is the same
            if len(jobs_executing[i].servers) == number_of_servers:
                for _i in range(len(servers)):
                    if servers[_i] in jobs_executing[i].servers:
                        servers_utilisation[_i] = True
                continue

            servers_to_use = None

            if len(jobs_executing[i].servers) > number_of_servers:
                # computes the servers to keep
                servers_to_use = jobs_executing[i].servers[:number_of_servers]
            else:
                # computes the servers to recruit
                servers_to_use = [server for server in jobs_executing[i].servers]
                # find the lowest id server available
                for _i in range(0, len(servers)):
                    # enough servers are selected
                    if len(servers_to_use) == number_of_servers:
                        break
                    if not servers_utilisation[_i] and not servers[_i].is_busy(time):
                        servers_to_use.append(servers[_i])

            assert len(servers_to_use) == number_of_servers

            for _i in range(len(servers)):
                if servers[_i] in servers_to_use:
                    servers_utilisation[_i] = True

            if isinstance(jobs_executing[i].servers[0].jobs[0], JobReconfiguration):
                # jobs is a reconfiguration, needs interrupt and dropping

                # reconfiguration needs to be interrupted and created with the new servers
                jor: JobReconfiguration = jobs_executing[i].servers[0].jobs[0]
                assert isinstance(jor, JobReconfiguration)
                # continuation needs to be dropped and created with the new reconfiguration
                joc: JobWork = jobs_executing[i].servers[0].jobs[1]
                assert isinstance(joc, JobWork)

                # interrupts reconfiguration
                self.remove_job(time, jor)

                # drops execution
                self.active_jobs.remove(joc)
                joc.release_servers()
                # update self.time_next_departure
                if joc.time_end == self.time_next_departure:
                    if len(self.active_jobs) == 0:
                        self.time_next_departure = None
                    else:
                        self.time_next_departure = min(self.active_jobs, key=lambda _j: _j.time_end).time_end

                # build new reconfiguration
                jnr, jnc, _, _ = jor.reconfigure(time, servers_to_use)

                # adds the new jobs
                self.add_job(jnr)
                self.add_job(jnc)

                max_reconfiguration_end = max(max_reconfiguration_end, jnr.time_end)
            else:
                # job is work, needs reconfiguration

                if jobs_executing[i].is_executing(time):
                    # job is executing

                    # interrupt execution
                    self.remove_job(time, jobs_executing[i])

                    # build reconfiguration
                    jr, jc, _, _ = jobs_executing[i].reconfigure_servers(time, servers_to_use)

                    # adds the new jobs
                    self.add_job(jr)
                    self.add_job(jc)

                    max_reconfiguration_end = max(max_reconfiguration_end, jr.time_end)
                else:
                    # job has not started execution

                    # rebuilt the request
                    request = Request(jobs_executing[i].id, 0, jobs_executing[i].data, jobs_executing[i].mass, jobs_executing[i].min_servers, jobs_executing[i].max_servers, jobs_executing[i].malleability_factor, jobs_executing[i].frequency_factor)

                    # drop execution
                    assert isinstance(jobs_executing[i], JobWork)
                    self.active_jobs.remove(jobs_executing[i])
                    jobs_executing[i].release_servers()
                    # update self.time_next_departure
                    if jobs_executing[i].time_end == self.time_next_departure:
                        if len(self.active_jobs) == 0:
                            self.time_next_departure = None
                        else:
                            self.time_next_departure = min(self.active_jobs, key=lambda j: j.time_end).time_end

                    # build work
                    jw = JobWork.from_request(time, request, servers_to_use)

                    self.add_job(jw)

        # schedule new jobs
        for i in range(len(jobs_executing), len(jobs_executing) + len(jobs_pending)):
            # computes number of servers to execute on
            number_of_servers = math.floor(len(servers) / (len(jobs_executing) + len(jobs_pending)))
            if len(servers) % (len(jobs_executing) + len(jobs_pending)) > i:
                number_of_servers += 1

            servers_to_use = []
            for _i in range(len(servers)):
                if not servers_utilisation[_i] and not servers[_i].is_busy(max_reconfiguration_end):
                    servers_to_use.append(servers[_i])
                    servers_utilisation[_i] = True
                    if len(servers_to_use) == number_of_servers:
                        break

            assert len(servers_to_use) == number_of_servers

            jw = JobWork.from_request(max_reconfiguration_end, jobs_pending[i - len(jobs_executing)], servers_to_use)

            self.add_job(jw)
