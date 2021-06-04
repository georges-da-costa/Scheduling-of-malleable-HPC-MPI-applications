from dataclasses import astuple, dataclass
from pathlib import Path
from random import uniform
import sys

import matplotlib.pyplot as plt
import pandas as pd
import structlog

@route('/test', method='GET')
def test():
    return sys.version


@dataclass
class Vector2i:
    """A 2-dimensional vector of int."""

    x = 0  #: value on the x axis.
    y = 0  #: value on the y axis.


@dataclass
class Color:
    """A color container object.

    Default color is black.
    """

    r = 0.0  #: Normalized red channel value.
    g = 0.0  #: Normalized green channel value.
    b = 0.0  #: Normalized blue channel value.
    a = 1.0  #: Normalized alpha value (influences the transparency).


logger = structlog.getLogger(__name__)


class Visualizer:
    """A class that creates different types of visualizations.
    """

    def __init__(self):
        """if not Path('last_completed_jobs.txt').exists():
            raise FileNotFoundError('last_completed_jobs.txt not found')
        with open('last_completed_jobs.txt', 'r') as lcj:
            self.last_completed_jobs = lcj.read()"""

        all_jobs = {'SYSTEM': [], 'job0': '[job0 (JobWork) on 1: 220 -> 340.61705846760304, job0 (JobReconfiguration) on 5: 320 -> 395.8384796150766, job0 (JobWork) on 5: 395.8384796150766 -> 399.9618913085972]', 'job1': '[job1 (JobWork) on 1: 450 -> 931.9373659754765, job1 (JobReconfiguration) on 5: 550 -> 1028.4567932257323, job1 (JobWork) on 5: 1028.4567932257323 -> 1104.8442664208276]', 'job2': '[job2 (JobWork) on 1: 1110 -> 1193.197021015362]', 'job3': '[job3 (JobWork) on 1: 2800 -> 3733.816099032648, job3 (JobReconfiguration) on 4: 2900 -> 3025.663563971343, job3 (JobWork) on 4: 3025.663563971343 -> 3234.117588729505, job3 (JobReconfiguration) on 5: 3180 -> 3205.1327127942686, job3 (JobWork) on 5: 3205.1327127942686 -> 3248.4267837778725]', 'job4': '[job4 (JobWork) on 1: 2900 -> 3176.7315754075175]', 'job5': '[job5 (JobWork) on 1: 4020 -> 6330.990385184537, job5 (JobReconfiguration) on 4: 4120 -> 4518.736641043215, job5 (JobWork) on 4: 4518.736641043215 -> 5071.484237339349, job5 (JobReconfiguration) on 5: 4620 -> 4699.747328208643, job5 (JobWork) on 5: 4699.747328208643 -> 5060.934718080122]', 'job6': '[job6 (JobWork) on 1: 4050 -> 4314.942340391629]', 'job7': '[job7 (JobWork) on 1: 5070 -> 5104.732825266564]', 'job8': '[job8 (JobWork) on 1: 5070 -> 5070.696498461082]', 'job9': '[job9 (JobWork) on 1: 5740 -> 6215.850366381, job9 (JobReconfiguration) on 4: 5840 -> 6129.987271398058, job9 (JobWork) on 4: 6129.987271398058 -> 6223.949862993308]', 'job10': '[job10 (JobWork) on 1: 5820 -> 6246.464890904328, job10 (JobReconfiguration) on 4: 6230 -> 6461.665981219811, job10 (JobWork) on 4: 6461.665981219811 -> 6465.782203945893]', 'job11': '[job11 (JobWork) on 1: 6230 -> 6335.931471685353]', 'job12': '[job12 (JobWork) on 1: 6570 -> 7888.65145919138, job12 (JobReconfiguration) on 5: 6670 -> 6806.914160940582, job12 (JobWork) on 5: 6806.914160940582 -> 7050.644452778858]', 'job13': '[job13 (JobWork) on 1: 7060 -> 8013.687870044841, job13 (JobReconfiguration) on 5: 7160 -> 7222.345983473811, job13 (JobWork) on 5: 7222.345983473811 -> 7393.08355748278]', 'job14': '[job14 (JobWork) on 1: 7770 -> 7818.386340792669]', 'job15': '[job15 (JobWork) on 1: 7870 -> 44410.697398448734, job15 (JobReconfiguration) on 5: 7970 -> 8157.146159707897, job15 (JobWork) on 5: 8157.146159707897 -> 15445.285639397644, job15 (JobReconfiguration) on 5: 8660 -> 8697.42923194158, job15 (JobWork) on 4: 8697.42923194158 -> 17179.036281188637, job15 (JobReconfiguration) on 4: 9200 -> 9246.786539926974, job15 (JobWork) on 3: 9246.786539926974 -> 19885.50158151182, job15 (JobReconfiguration) on 3: 9750 -> 9812.382053235966, job15 (JobWork) on 2: 9812.382053235966 -> 25015.634425503697, job15 (JobReconfiguration) on 2: 10320 -> 10507.146159707896, job15 (JobWork) on 1: 10507.146159707896 -> 39898.4150107153, job15 (JobReconfiguration) on 5: 11050 -> 11237.146159707896, job15 (JobWork) on 5: 11237.146159707896 -> 17006.829161850954, job15 (JobReconfiguration) on 5: 11740 -> 11777.42923194158, job15 (JobWork) on 4: 11777.42923194158 -> 18360.965684255272, job15 (JobReconfiguration) on 5: 11950 -> 11987.42923194158, job15 (JobWork) on 5: 11987.42923194158 -> 17116.2017793458, job15 (JobReconfiguration) on 5: 12490 -> 12527.42923194158, job15 (JobWork) on 4: 12527.42923194158 -> 18310.181456123828, job15 (JobReconfiguration) on 4: 13030 -> 13076.786539926974, job15 (JobWork) on 3: 13076.786539926974 -> 20117.028481425412, job15 (JobReconfiguration) on 3: 13580 -> 13642.382053235966, job15 (JobWork) on 2: 13642.382053235966 -> 23447.924775374086, job15 (JobReconfiguration) on 2: 14150 -> 14337.146159707896, job15 (JobWork) on 1: 14337.146159707896 -> 32932.99571045606, job15 (JobReconfiguration) on 2: 18170 -> 18357.146159707896, job15 (JobWork) on 2: 18357.146159707896 -> 25738.644014935926, job15 (JobReconfiguration) on 2: 18860 -> 19047.146159707896, job15 (JobWork) on 1: 19047.146159707896 -> 32804.434189579755, job15 (JobReconfiguration) on 4: 20640 -> 20827.146159707896, job15 (JobWork) on 4: 20827.146159707896 -> 23868.254707102835, job15 (JobReconfiguration) on 4: 21330 -> 21376.786539926976, job15 (JobWork) on 3: 21376.786539926976 -> 24761.12614939742, job15 (JobReconfiguration) on 5: 21480 -> 21517.42923194158, job15 (JobWork) on 5: 21517.42923194158 -> 23486.104921580034, job15 (JobReconfiguration) on 5: 22020 -> 22057.42923194158, job15 (JobWork) on 4: 22057.42923194158 -> 23890.06038391662, job15 (JobReconfiguration) on 5: 22160 -> 22197.42923194158, job15 (JobWork) on 5: 22197.42923194158 -> 23581.477539074876, job15 (JobReconfiguration) on 5: 22700 -> 22737.42923194158, job15 (JobWork) on 4: 22737.42923194158 -> 23839.276155785177, job15 (JobReconfiguration) on 5: 22840 -> 22877.42923194158, job15 (JobWork) on 5: 22877.42923194158 -> 23676.850156569722, job15 (JobReconfiguration) on 5: 23380 -> 23417.42923194158, job15 (JobWork) on 4: 23417.42923194158 -> 23788.491927653733, job15 (JobReconfiguration) on 5: 23520 -> 23557.42923194158, job15 (JobWork) on 5: 23557.42923194158 -> 23772.222774064565]', 'job16': '[job16 (JobWork) on 1: 10020 -> 10067.220483626168]', 'job17': '[job17 (JobWork) on 1: 8830 -> 11335.78520442783, job17 (JobReconfiguration) on 2: 10070 -> 10168.81978303328, job17 (JobWork) on 2: 10168.81978303328 -> 10801.712385247194, job17 (JobReconfiguration) on 3: 10510 -> 10542.93992767776, job17 (JobWork) on 3: 10542.93992767776 -> 10737.41485117589]', 'job18': '[job18 (JobWork) on 1: 8700 -> 8824.053884004226]', 'job19': '[job19 (JobWork) on 1: 9920 -> 10014.665061506852]', 'job20': '[job20 (JobWork) on 1: 9260 -> 11611.980415994964, job20 (JobReconfiguration) on 4: 10740 -> 10830.825245118267, job20 (JobWork) on 4: 10830.825245118267 -> 11048.820349117008]', 'job21': '[job21 (JobWork) on 1: 9250 -> 9256.1411327063]', 'job22': '[job22 (JobWork) on 1: 9820 -> 9918.287286535102]', 'job23': '[job23 (JobWork) on 1: 11780 -> 11945.232193248065]', 'job24': '[job24 (JobWork) on 1: 13650 -> 13681.475239866464]', 'job25': '[job25 (JobWork) on 1: 13170 -> 18993.990241296706, job25 (JobReconfiguration) on 3: 14030 -> 14406.17272044941, job25 (JobWork) on 3: 14406.17272044941 -> 16060.836134214976, job25 (JobReconfiguration) on 3: 14910 -> 15035.39090681647, job25 (JobWork) on 2: 15035.39090681647 -> 16761.645108138935, job25 (JobReconfiguration) on 3: 15150 -> 15275.39090681647, job25 (JobWork) on 3: 15275.39090681647 -> 16349.820978909094, job25 (JobReconfiguration) on 3: 15780 -> 15905.39090681647, job25 (JobWork) on 2: 15905.39090681647 -> 16760.12237518011, job25 (JobReconfiguration) on 2: 16410 -> 16786.17272044941, job25 (JobWork) on 1: 16786.17272044941 -> 17486.417470809633]', 'job26': '[job26 (JobWork) on 1: 12530 -> 13699.867665674934, job26 (JobReconfiguration) on 2: 13690 -> 14017.763637789652, job26 (JobWork) on 2: 14017.763637789652 -> 14022.69747062712]', 'job27': '[job27 (JobWork) on 1: 13080 -> 13162.578338490794]', 'job28': '[job28 (JobWork) on 1: 14340 -> 14580.49702860409]', 'job29': '[job29 (JobWork) on 1: 14590 -> 20101.48791926252, job29 (JobReconfiguration) on 2: 17880 -> 18154.14996665685, job29 (JobWork) on 2: 18154.14996665685 -> 19264.89392628811, job29 (JobReconfiguration) on 3: 18260 -> 18351.38332221895, job29 (JobWork) on 3: 18351.38332221895 -> 19021.312606411022, job29 (JobReconfiguration) on 3: 18870 -> 18961.38332221895, job29 (JobWork) on 2: 18961.38332221895 -> 19188.352231835484]', 'job30': '[job30 (JobWork) on 1: 15040 -> 15147.944650293766]', 'job31': '[job31 (JobWork) on 1: 15910 -> 17857.044607542623]', 'job32': '[job32 (JobWork) on 1: 17860 -> 17872.336751953913]', 'job33': '[job33 (JobWork) on 1: 17580 -> 17915.430083783027, job33 (JobReconfiguration) on 2: 17870 -> 18139.178337640802, job33 (JobWork) on 2: 18139.178337640802 -> 18161.893379532314]', 'job34': '[job34 (JobWork) on 1: 17490 -> 17570.48523088262]', 'job35': '[job35 (JobWork) on 1: 16790 -> 17866.83165800255]', 'job36': '[job36 (JobWork) on 1: 18170 -> 18217.172178455083]', 'job37': '[job37 (JobWork) on 1: 19270 -> 21331.43198464637, job37 (JobReconfiguration) on 4: 19970 -> 20293.342110412956, job37 (JobWork) on 4: 20293.342110412956 -> 20633.700106574546]', 'job38': '[job38 (JobWork) on 1: 19280 -> 20352.778310931255, job38 (JobReconfiguration) on 3: 19600 -> 19712.827109822894, job38 (JobWork) on 3: 19712.827109822894 -> 19963.753213466647]', 'job39': '[job39 (JobWork) on 1: 19050 -> 19277.535715513983]', 'job40': '[job40 (JobWork) on 1: 18970 -> 19511.833409098334, job40 (JobReconfiguration) on 2: 19280 -> 19476.700138097927, job40 (JobWork) on 2: 19476.700138097927 -> 19592.616842647094]', 'job41': '[job41 (JobWork) on 1: 19190 -> 19262.715506371067]', 'job42': '[job42 (JobWork) on 1: 19190 -> 19278.418532352065]', 'job43': '[job43 (JobWork) on 1: 20640 -> 21352.057051972908]', 'job44': '[job44 (JobWork) on 1: 23420 -> 23445.744132670225]'}
        show_range = False
        epoch = True
        
        all_jobs_sectioned = {}
        all_individual_job_servers = {}
        all_job_numbers = []
        
        #plt.figure()
        #plt.subplot()
        for jobs in all_jobs.values():
            if not jobs:
                continue
            
            elif ',' in jobs:
                jobs = jobs.split(',')
                for job in jobs:
                    job_number_position = job.index('job') + 3
                    job_number_position_end = job.index('(')
                    job_number = job[job_number_position:job_number_position_end]
                    job_number = int(job_number)
                    
                    job_server_position = job.index('on ') + 3   #The addition is to account for the index which chooses the first position of 'o' in 'on '
                    job_server_position_end = job.index(':')
                    job_server = job[job_server_position:job_server_position_end]
                    job_server = int(job_server)
                    
                    job_color = Color(uniform(0.25, 0.9), uniform(0.25, 0.9), uniform(0.25, 0.9))
                    job_color.a = 0.5 if 'JobReconfiguration' in job else 1

                    job_start_position = job_server_position_end + 1
                    job_start_position_end = job.index('->')
                    job_start_time = job[job_start_position:job_start_position_end]
                    job_start_time = float(job_start_time)

                    job_end_time = job[job_start_position_end+2:]
                    for x in job_end_time:
                        if not x.isdigit() and x != '.':
                            job_end_time = job_end_time.replace(x, '')
                            
                    job_end_time = float(job_end_time)

                    job_duration = job_end_time - job_start_time

                    """tl = Vector2i(job_start_time, job_server)
                    size = Vector2i(job_duration, 1)
                    rectangle = plt.Rectangle((tl.x, tl.y), size.x, size.y, fc=astuple(job_color))
                    plt.gca().add_patch(rectangle)"""

                    if job_number not in all_job_numbers:
                        all_job_numbers.append(job_number)

                    #The job duration is also the stretch_time
                    if job_number not in all_jobs_sectioned.keys():
                    	all_jobs_sectioned[job_number] = [job_duration]
                    else:
                    	all_jobs_sectioned[job_number].append(job_duration)

                    if job_server not in all_individual_job_servers.keys():
                    	all_individual_job_servers[job_server] = [job_duration]
                    else:
                    	all_individual_job_servers[job_server].append(job_duration)
                    
                    
            else:
                job_number_position = jobs.index('job') + 3
                job_number_position_end = jobs.index('(')
                job_number = jobs[job_number_position:job_number_position_end]
                job_number = int(job_number)
                
                job_server_position = jobs.index('on ') + 3   #The addition is to account for the index which chooses the first position of 'o' in 'on '
                job_server_position_end = jobs.index(':')
                job_server = jobs[job_server_position:job_server_position_end]
                job_server = int(job_server)
        
                job_color = Color(uniform(0.25, 0.9), uniform(0.25, 0.9), uniform(0.25, 0.9))
                job_color.a = 0.5 if 'JobReconfiguration' in jobs else 1

                job_start_position = job_server_position_end + 1
                job_start_position_end = jobs.index('->')
                job_start_time = jobs[job_start_position:job_start_position_end]
                job_start_time = float(job_start_time)

                job_end_time = jobs[job_start_position_end+2:]
                for x in job_end_time:
                    if not x.isdigit() and x != '.':
                        job_end_time = job_end_time.replace(x, '')
                        
                job_end_time = float(job_end_time)
                
                job_duration = job_end_time - job_start_time

                """tl = Vector2i(job_start_time, job_server)
                size = Vector2i(job_duration, 1)
                rectangle = plt.Rectangle((tl.x, tl.y), size.x, size.y, fc=astuple(job_color))
                plt.gca().add_patch(rectangle)"""

                if job_number not in all_job_numbers:
                    all_job_numbers.append(job_number)

                #The job duration is also the stretch_time
                if job_number not in all_jobs_sectioned.keys():
                    all_jobs_sectioned[job_number] = [job_duration]
                else:
                    all_jobs_sectioned[job_number].append(job_duration)

                if job_server not in all_individual_job_servers.keys():
                    all_individual_job_servers[job_server] = [job_duration]
                else:
                    all_individual_job_servers[job_server].append(job_duration)


        DURATION_FREQUENCY_CHANGE = 10
        DURATION_SHUTDOWN = 500
        DURATION_STARTUP = 250
        #cost = mean(stretch_time) ** stretch_time_weight * self._normalized_average_power() ** energy_weight

        fig, ax = plt.subplots(1)

        all_total_costs = []
        all_mean_costs = []
        all_min_costs = []
        all_max_costs = []
        
        servers_count = 5
        for job_number, jobs in enumerate(all_jobs_sectioned.values()):
            total_energy = 0
            area = 0
            
            for job_duration in jobs:   #The job duration equals the time
                srv_count = 0
                
                for job_servers in all_individual_job_servers.values():
                    for job_duration_in_server in job_servers:
                        if job_duration_in_server == job_duration:
                            srv_count += 1
                            
                startup_energy = DURATION_STARTUP * job_duration
                
                total_energy += startup_energy * srv_count
                area += job_duration * srv_count

            #adding shutdown_time
            work_duration = sum(jobs)
            total_shutdown_energy = work_duration * servers_count - area

            total_energy += total_shutdown_energy
            

            total_cost = sum(jobs) * total_energy
            mean_cost = total_cost / len(jobs)
            min_cost = min(jobs) * total_energy
            max_cost = max(jobs) * total_energy

            all_total_costs.append(total_cost)
            all_mean_costs.append(mean_cost)
            all_min_costs.append(min_cost)
            all_max_costs.append(max_cost)

            #print([total_cost, mean_cost, min_cost, max_cost])
    

        rearranged_list = [['Total Cost', 'Mean Cost','Min Cost', 'Max Cost']]
        all_cost_values = [all_total_costs, all_mean_costs, all_min_costs, all_max_costs]

        length = len(all_cost_values[0])
        for i in range(length):
            x = [cost_value[i] for cost_value in all_cost_values]
            rearranged_list.append(x)

        self.to_csv(rearranged_list, 'ga_stats.csv')    #Saves the data to the csv file
    
        if epoch:
            xlabel = "Epoch"
            ylabel = "Mean cost"
            ax.plot(all_job_numbers, all_mean_costs, lw=2, color="blue")
            #plt.plot(all_job_numbers, all_mean_costs, lw=2, color="blue")
            if show_range:
                ax.fill_between(
                    all_job_numbers,
                    all_min_costs,
                    all_max_costs,
                    facecolor="blue",
                    alpha=0.1)
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            
        else:
            # Drawing results of bench experiments.
            ax.plot(all_job_numbers, all_total_costs, lw=2, color="blue")
            xlabel = "Experiments count"
            ylabel = "Cost"
            ax.legend(loc="upper right")
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            
        plt.show()
        #path = Path('gantt.jpg')
        #path.parent.mkdir(0o755, parents=True, exist_ok=True)


        #plt.ylabel("servers")
        #plt.xlabel("time")
        #plt.axis("auto")
        #plt.show()
        #plt.savefig('gantt.jpg', dpi=200)
        #plt.close('gantt.jpg')"""

    def draw_gantt(self, stats, filepath=str):
        """Draws a Gantt chart.

        Directories referenced by filepath are created similar to mkdir -p.

        Args:
            stats (SchedulerStats): A container object for the scheduler's \
            output statistics.
            filepath (str): The location for writing the resulting Gantt chart.

        """
        path = Path(filepath)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)

        plt.figure(filepath)
        plt.subplot()

        power_off_color = Color(0, 0, 0)
        for jobs in stats.values():
            job_color = Color(
                uniform(0.25, 0.9), uniform(0.25, 0.9), uniform(0.25, 0.9)
            )
            for job in jobs:
                job_color.a = 0.5 if job.is_reconfiguration() else 1
                #if job.is_power_off():
                #    job_color = power_off_color
                for server in job.servers:
                    tl = Vector2i(job.start_time, server.index)
                    size = Vector2i(job.duration, 1)
                    self._draw_rectangle(tl=tl, size=size, color=job_color)

        plt.ylabel("servers")
        plt.xlabel("time")
        plt.axis("auto")
        plt.savefig(filepath, dpi=200)
        plt.close(filepath)

    def draw_graph(self, stats, filepath=str, show_range=False):
        """Draws a 2D graph of the mean cost against the epoch count.

        Directories referenced by filepath are created similar to mkdir -p.

        Args:
            stats: A container object for the epoch's or experiment's statistics.
            filepath: Location for writing the resulting chart.
            show_range: A flag for enabling showing the min-max range as a filled\
            up area on the graph. Defaults to False.

        """
        path = Path(filepath)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)
        fig, ax = plt.subplots(1)
        if "epoch" in stats:
            # Drawing results of swarm training.
            xlabel = "Epoch"
            ylabel = "Mean cost"
            ax.plot(stats["epoch"], stats["mean"], lw=2, color="blue")
            if show_range:
                ax.fill_between(
                    stats["epoch"],
                    stats["min"],
                    stats["max"],
                    facecolor="blue",
                    alpha=0.1,
                )
        else:
            # Drawing results of bench experiments.
            ax.plot(stats.index.values.tolist(), stats["cost"], lw=2, color="blue")
            xlabel = "Experiments count"
            ylabel = "Cost"
        ax.legend(loc="upper right")
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.savefig(filepath, dpi=200)

    def to_csv(self, table=list, path=str):
        """Converts a list into a csv file.

        Directories referenced by filepath are created similar to mkdir -p.

        Args:
            table (list): A python list.
            path (str): The location for writing the csv file.

        """
        path = Path(path)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)
        df_table = pd.DataFrame(table)
        logger.debug(
		"Obtained costs, stored in %s:\n %s" % (path.name, df_table)
	)
        df_table.to_csv(path)

    def _draw_rectangle(self, tl, size, color):
        rectangle = plt.Rectangle((tl.x, tl.y), size.x, size.y, fc=astuple(color))
        plt.gca().add_patch(rectangle)



if __name__ == '__main__':
    Visualizer()
    #SchedulerStats.generae
    #Visualizer.draw_gantt(__file__, SchedulerStats.gen_stats(__file__, None, None), filepath = 'results/GA/gantt.csv')
    #Visualizer.draw_graph(__file__, SchedulerStats.gen_stats(__file__, None, None), filepath = 'results/GA/gantt.csv')
