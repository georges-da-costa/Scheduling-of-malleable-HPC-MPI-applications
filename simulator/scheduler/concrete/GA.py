from dataclasses import astuple, dataclass
from random import uniform, random, sample

from simulator.job import JobWork, Request, JobSystemTurnOff
from simulator.scheduler import Scheduler

from deap import creator, tools, base
import numpy as np


@dataclass
class SchedulerConfig:
    reconfig_scale: float = 0.331  #: The reconfiguration scaling factor in [0,1].
    reconfig_weight: float = 0.175  #: The reconfiguration weight in [0,1].
    alpha_weight: float = 0.742  #: The speedup factor's weight in [0,1].
    shutdown_scale: float = 0.760  #: The shutdown scaling factor in [0,1].
    shutdown_weight: float = 0.455  #: The shutdown's weight in [0,1].
    shutdown_time_short: float = 899  #: A short duration for shuting the servers.
    shutdown_time_long: float = 1406  #: A long duration for shuting the servers.
    shutdown_time_prob: float = 0.717  #: The probability of choosing shutdown_time_short.

    @classmethod
    def random(cls):
        c = SchedulerConfig()
        c.reconfig_scale = uniform(0.001, 1.0)
        c.reconfig_weight = uniform(0.01, 1.0)
        c.alpha_weight = uniform(0.001, 1.0)
        c.shutdown_scale = uniform(0.001, 1.0)
        c.shutdown_weight = uniform(0.01, 1.0)
        c.shutdown_time_short = uniform(370, 1200)
        c.shutdown_time_long = uniform(370, 4000)
        c.shutdown_time_prob = uniform(0.0001, 1.0)
        return c

    def to_dict(self):
        dict_obj = self.__dict__
        return dict_obj

    def to_list(self):
        return list(astuple(self))

class SchedulerGA(Scheduler):
    NAME = "GA"

    def __init__(self, server_count: int, args):
        super().__init__(server_count, args)

        self.conf = SchedulerConfig()

    def evaluate(self, individual):
        return sum(individual),

    def get_min_max_range(self, input_values):
        rearranged_list = []

        length = len(input_values[0]) #Using the length of the values in the first list as a criterion in order to pick min and max of related parameters
        for i in range(length):
            x = [input_value[i] for input_value in input_values]
            rearranged_list.append(x)

        min_range = [min(x) for x in rearranged_list]
        max_range = [max(x) for x in rearranged_list]

        return min_range, max_range

    def create_toolbox(self, input_values):
        creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMin)

        toolbox = base.Toolbox()

        def load_individuals(creator):
            individuals = []
            for i in range(len(input_values)):
                individuals.append(creator(input_values[i]))
            return individuals

        min_range, max_range = self.get_min_max_range(input_values)

        toolbox.register("population",load_individuals, creator.Individual)
        toolbox.register("evaluate", self.evaluate)
        toolbox.register("mate", tools.cxSimulatedBinaryBounded,eta=0.5, low=min_range, up=max_range)
        toolbox.register("mutate", tools.mutPolynomialBounded, eta=0.5, low=min_range, up=max_range,indpb=0.5)
        toolbox.register("select", tools.selTournament, tournsize=2)

        population = toolbox.population()
        NGEN = 100
    
        # Evaluate the entire population
        fitnesses = list(map(toolbox.evaluate, population))
        for individual, fit in zip(population, fitnesses):
            individual.fitness.values = fit
        
        for g in range(NGEN):
            offspring = toolbox.select(population, len(population))
            offspring = list(map(toolbox.clone, offspring))
    
            for child1, child2 in zip(offspring[::2], offspring[1::2]):
                if abs(sum(child1) - sum(min_range)) < abs(sum(child1) - sum(max_range)) and abs(sum(child2) - sum(min_range)) < abs(sum(child2) - sum(max_range)):
                    #The condition is to only cross parents whose value are closer to the min_range
                    toolbox.mate(child1, child2)
                    del child1.fitness.values
                    del child2.fitness.values
            for mutant in offspring:
                if abs(sum(mutant) - sum(min_range)) < abs(sum(mutant) - sum(max_range)):
                    #The condition is to only mutate a child whose value is closer to the min_range
                    try:
                        toolbox.mutate(mutant)
                    except ZeroDivisionError:
                        continue #This occurs should in case the up an low are the same which will result in an error, thus , do not mutate
                                 #It continues since the fitness values are still related
                    del mutant.fitness.values
        
            invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
            fitnesses = map(toolbox.evaluate, invalid_ind)
        
            for individual, fit in zip(invalid_ind, fitnesses):
                individual.fitness.values = fit  
        

            population[:] = offspring

        best_ind = tools.selBest(population, 1)[0] 
        best_ind_sum = sum(best_ind)

        return best_ind_sum

    def update(self, time: float):
        print(self.active_jobs)

        # removes the completed jobs
        for job in filter(lambda j: j.is_over(time), self.active_jobs):
            self.remove_job(time, job)

        # schedules new jobs on available servers by picking best job through ga
        while len(self.request_queue) > 0 and len(self.get_servers_available(time)) > 0:
            if len(self.request_queue) > 1:  #only allows for jobs more than 1 to be analized using ga
                request_queue_selected_parameters = []
                request_queue_selected_parameters_sum = []
                
                for req in self.request_queue:
                    parameters = [req.submission_time, req.data, req.mass, req.min_server_count, req.max_server_count]
                    request_queue_selected_parameters.append(parameters)
                    
                    sum_of_parameters = sum(parameters)
                    request_queue_selected_parameters_sum.append(sum_of_parameters)
                
                best_job_parameters_sum = self.create_toolbox(request_queue_selected_parameters)

                closest_parameter_difference = abs(request_queue_selected_parameters_sum[0] - best_job_parameters_sum)
                position = 0
                for i in range(1, len(request_queue_selected_parameters_sum)):
                    if abs(request_queue_selected_parameters_sum[i] - best_job_parameters_sum) < closest_parameter_difference:
                        closest_parameter_difference = request_queue_selected_parameters_sum[i]
                        position = i

                self.add_job(JobWork.from_request(time, self.request_queue.pop(position), [self.get_servers_available(time)[0]]))

            else:
                self.add_job(JobWork.from_request(time, self.request_queue.pop(0), [self.get_servers_available(time)[0]]))

        # Applies a reconfiguration to less cores
        for job in self.active_jobs:
            if isinstance(job, JobWork):
                if self._is_job_reconfigurable_decrease(job, self.get_servers_available(time), time):
                    av_servers = self._reconfigure_job_decrease(job, self.get_servers_available(time), time)

        # Applies a reconfiguration to more cores
        jobs_by_mass = sorted(self.active_jobs, key=lambda j: j.remaining_mass(time) if isinstance(j, JobWork) else 99999999)
        while jobs_by_mass and self.get_servers_available(time):
            job = jobs_by_mass[0]
            if isinstance(job, JobWork):
                if self._is_job_reconfigurable_increase(job, self.get_servers_available(time), time):
                    av_servers = self._reconfigure_job_increase(job, self.get_servers_available(time), time)
            jobs_by_mass.pop(0)

        # Applies power-offs
        for server in list(self.get_servers_available(time)):
            if not self._shutdown_server(self.get_servers_available(time)):
                break

            shutdown, duration = self._allow_shutdown(self.get_servers_available(time))
            if not shutdown:
                continue

            power_off = JobSystemTurnOff(time, server)
            self.add_job(power_off)

    def _allow_shutdown(self, av_servers: list):
        return False, None
        if (0.5 > ((len(av_servers) / len(self.servers)) ** self.conf.shutdown_weight) * self.conf.shutdown_scale):
            return False, 0

        if random() < self.conf.shutdown_time_prob:
            shutdown_duration = self.conf.shutdown_time_short
        else:
            shutdown_duration = self.conf.shutdown_time_long

        return True, shutdown_duration

    def _reconfigure_job_increase(self, job: JobWork, av_servers: list, time):
        job.interrupt(time)
        extra_srv_count = min(job.max_servers - len(job.servers), len(av_servers))
        extra_srvs = sample(av_servers, extra_srv_count)
        job_servers = job.servers + extra_srvs
        av_servers = [server for server in av_servers if server not in extra_srvs]

        reconfig_job, job_rest, _, _ = job.reconfigure_servers(time, job_servers)
        self.remove_job(time, job)
        self.add_job(reconfig_job)
        self.add_job(job_rest)

        return av_servers

    def _reconfigure_job_decrease(self, job: JobWork, av_servers: list, time):
        job.interrupt(time)

        dropped_server = sample(job.servers, 1)

        job_servers = job.servers.copy()
        for drop in dropped_server:
            job_servers.remove(drop)
            av_servers.append(drop)

        reconfig_job, job_rest, _, _ = job.reconfigure_servers(time, job_servers)
        self.remove_job(time, job)
        self.add_job(reconfig_job)
        self.add_job(job_rest)

        return av_servers

    def _is_job_reconfigurable_increase(self, job: JobWork, av_servers: list, time):
        if not job.is_executing(time) or len(job.servers) == job.max_servers:
            return False

        if time - job.time_beg < 100:
            return False

        return True

    def _is_job_reconfigurable_decrease(self, job: JobWork, av_servers: list, time):
        if not job.is_executing(time) or len(job.servers) == job.min_servers:
            return False

        if time - job.time_beg < 500:
            return False

        return True

    def _shutdown_server(self, av_servers: list):
        if not self.request_queue:
            return True

        required_servers = sum(req.min_server_count for req in self.request_queue)
        return len(av_servers) > required_servers

    def _allocate_servers(self, available_servers: list, job_req: Request):
        min_servers = min(job_req.max_server_count, len(available_servers))
        if min_servers < job_req.min_server_count:
            return []
        return sample(available_servers, k=min_servers)