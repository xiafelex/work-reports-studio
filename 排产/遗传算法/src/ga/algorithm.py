"""
遗传算法主流程
"""
import random
from typing import List, Tuple, Optional, Dict
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import os

from ..models import WorkerGroup, Schedule, ProjectData
from ..config import WORKER_CONFIG, GA_CONFIG, ENABLE_SPATIAL_CONSTRAINT
from .population import initialize_population
from .operators import tournament_selection, order_crossover, mutate
from .fitness import calculate_fitness
from .decoder_with_constraint import decode_chromosome_with_package_priority
from .spatial_decoder import decode_with_spatial_constraint
from .progressive_spatial_decoder import decode_with_progressive_spatial_constraint


class GeneticAlgorithm:
    """
    遗传算法类
    """
    
    def __init__(self,
                 project_data: ProjectData,
                 population_size: int = None,
                 generations: int = None,
                 crossover_rate: float = None,
                 mutation_rate: float = None,
                 elite_size: int = None,
                 tournament_size: int = None,
                 use_multiprocessing: bool = True,
                 n_processes: int = None,
                 worker_group: WorkerGroup = None):
        """
        初始化遗传算法
        
        参数:
            project_data: 项目数据对象（ProjectData）
            population_size: 种群大小
            generations: 迭代代数
            crossover_rate: 交叉概率
            mutation_rate: 变异概率
            elite_size: 精英数量
            tournament_size: 锦标赛大小
            use_multiprocessing: 是否使用多进程加速（默认True）
            n_processes: 进程数，默认为CPU核心数-1
            worker_group: 焊工组对象（如果不传入则使用默认配置创建）
        """
        self.project_data = project_data
        
        # 多进程配置
        self.use_multiprocessing = use_multiprocessing
        if n_processes is None:
            self.n_processes = max(1, cpu_count() - 1)  # 留一个核心给系统
        else:
            self.n_processes = n_processes
        
        # 参数（使用配置文件默认值）
        self.population_size = population_size or GA_CONFIG['population_size']
        self.generations = generations or GA_CONFIG['generations']
        self.crossover_rate = crossover_rate or GA_CONFIG['crossover_rate']
        self.mutation_rate = mutation_rate or GA_CONFIG['mutation_rate']
        self.elite_size = elite_size or GA_CONFIG['elite_size']
        self.tournament_size = tournament_size or GA_CONFIG['tournament_size']
        
        # 初始化焊工组（必须传入worker_group）
        if worker_group is None:
            raise ValueError("必须传入 worker_group 参数")
        self.worker_group = worker_group
        
        # 判断是否使用拓扑段（优先级最高）
        self.use_topology_segments = (ENABLE_SPATIAL_CONSTRAINT and 
                                      len(project_data.topology_segments) > 0)
        
        # 判断是否使用工区段（次优先级）
        self.use_segments = (ENABLE_SPATIAL_CONSTRAINT and 
                             len(project_data.segments) > 0 and 
                             not self.use_topology_segments)
        
        # 选择解码器
        if self.use_topology_segments:
            # 使用拓扑段解码器（新增）
            from .topology_segment_decoder import decode_with_topology_segments
            self.decoder_func = decode_with_topology_segments
        elif self.use_segments:
            # 使用渐进式空间约束解码器（基于管线拓扑）
            self.decoder_func = decode_with_progressive_spatial_constraint
        else:
            self.decoder_func = decode_chromosome_with_package_priority
        
        # 运行时数据
        self.population = []
        self.fitnesses = []
        self.best_solution = None
        self.best_fitness = 0.0
        self.history = {
            'best_fitness': [],
            'avg_fitness': [],
            'best_makespan': []
        }
    
    def run(self, verbose: bool = True) -> Tuple[List[str], Schedule]:
        """
        运行遗传算法
        
        参数:
            verbose: 是否显示详细信息
        
        返回:
            (最优染色体, 最优调度方案)
        """
        if verbose:
            print("\n" + "=" * 60)
            print("开始运行遗传算法")
            print("=" * 60)
            print(f"种群大小: {self.population_size}")
            print(f"迭代代数: {self.generations}")
            print(f"交叉率: {self.crossover_rate}")
            print(f"变异率: {self.mutation_rate}")
            print(f"精英数量: {self.elite_size}")
            if self.use_topology_segments:
                print(f"拓扑段数量: {len(self.project_data.topology_segments)} (拓扑段模式)")
            elif self.use_segments:
                print(f"工区段数量: {len(self.project_data.segments)} (工区段模式)")
            else:
                print(f"管线数量: {self.project_data.pipeline_count} (管线模式)")
            print(f"焊工组数: {self.worker_group.total_count}")
            if self.use_multiprocessing:
                print(f"多进程加速: 开启 ({self.n_processes}个进程)")
            else:
                print(f"多进程加速: 关闭")
            print("=" * 60)
        
        # 1. 初始化种群
        if verbose:
            print("初始化种群...")
        self.population = initialize_population(
            self.population_size, 
            self.project_data, 
            self.use_segments,
            self.use_topology_segments
        )
        
        # 2. 评估初始种群
        if verbose:
            print("评估初始种群...")
        self._evaluate_population()
        
        # 记录第0代历史（用于显示）
        self._record_history()
        if verbose:
            initial_makespan = self.history['best_makespan'][0]
            print(f"第0代: 最优工期={initial_makespan:.2f}天, "
                  f"平均适应度={sum(self.fitnesses)/len(self.fitnesses):.6f}")
        
        # 3. 迭代优化
        iterator = tqdm(range(self.generations), desc="GA进化") if verbose else range(self.generations)
        
        for generation in iterator:
            # 3.1 精英保留
            elites = self._select_elites()
            
            # 3.2 生成新种群
            new_population = elites.copy()
            
            while len(new_population) < self.population_size:
                # 选择
                parent1 = tournament_selection(self.population, self.fitnesses, self.tournament_size)
                parent2 = tournament_selection(self.population, self.fitnesses, self.tournament_size)
                
                # 交叉
                if random.random() < self.crossover_rate:
                    child = order_crossover(parent1, parent2)
                else:
                    child = parent1.copy()
                
                # 变异
                if random.random() < self.mutation_rate:
                    child = mutate(child, mutation_type='auto', project_data=self.project_data)
                
                new_population.append(child)
            
            # 3.3 更新种群
            self.population = new_population
            self._evaluate_population()
            
            # 3.4 记录历史
            self._record_history()
            
            # 3.5 显示进度
            if verbose and generation > 0 and generation % 50 == 0:
                # 获取真实的最优工期（从历史记录中）
                current_best_makespan = self.history['best_makespan'][-1] if self.history['best_makespan'] else 0
                tqdm.write(f"代{generation}: 最优工期={current_best_makespan:.2f}天, "
                          f"平均适应度={sum(self.fitnesses)/len(self.fitnesses):.6f}")
        
        # 4. 返回最优解（使用当前选定的解码器，并导出Excel）
        if hasattr(self.decoder_func, '__name__') and 'progressive_spatial' in self.decoder_func.__name__:
            # 如果是渐进式空间约束解码器，启用Excel导出
            best_schedule = self.decoder_func(self.best_solution, 
                                              self.project_data, 
                                              self.worker_group,
                                              export_excel=True)
        else:
            # 其他解码器不支持Excel导出参数
            best_schedule = self.decoder_func(self.best_solution, 
                                              self.project_data, 
                                              self.worker_group)
        
        if verbose:
            print("\n" + "=" * 60)
            print("遗传算法完成！")
            print("=" * 60)
            self._print_final_result(best_schedule)
        
        return self.best_solution, best_schedule
    
    def _evaluate_population(self):
        """评估种群中所有个体的适应度（支持多进程加速）"""
        if self.use_multiprocessing and len(self.population) > 10:
            # 使用多进程并行计算
            self.fitnesses = self._evaluate_population_parallel()
        else:
            # 单进程计算
            self.fitnesses = []
            for chromosome in self.population:
                fitness = self._calculate_fitness_single(chromosome)
                self.fitnesses.append(fitness)
        
        # 更新最优解
        max_fitness = max(self.fitnesses)
        if max_fitness > self.best_fitness:
            self.best_fitness = max_fitness
            best_idx = self.fitnesses.index(max_fitness)
            self.best_solution = self.population[best_idx].copy()
    
    def _calculate_fitness_single(self, chromosome: List[str]) -> float:
        """计算单个染色体的适应度"""
        # 使用选定的解码器
        schedule = self.decoder_func(chromosome, self.project_data, self.worker_group)
        
        # 计算适应度（纯粹基于makespan）
        makespan = schedule.get_makespan()
        
        # 防止除零错误
        if makespan <= 0:
            return 1e-10
        
        fitness = 1.0 / makespan
        return fitness
    
    def _evaluate_population_parallel(self):
        """使用多进程并行评估种群"""
        # 准备参数（包括use_segments标志）
        args_list = [
            (chromosome, self.project_data, self.worker_group, self.use_segments)
            for chromosome in self.population
        ]
        
        # 多进程计算
        with Pool(processes=self.n_processes) as pool:
            fitnesses = pool.starmap(_calculate_fitness_wrapper, args_list)
        
        return fitnesses
    
    def _select_elites(self) -> List[List[str]]:
        """选择精英个体"""
        # 按适应度排序
        sorted_indices = sorted(range(len(self.fitnesses)), 
                               key=lambda i: self.fitnesses[i], 
                               reverse=True)
        
        # 选择前elite_size个
        elite_indices = sorted_indices[:self.elite_size]
        elites = [self.population[i].copy() for i in elite_indices]
        
        return elites
    
    def _record_history(self):
        """记录历史数据"""
        self.history['best_fitness'].append(self.best_fitness)
        self.history['avg_fitness'].append(sum(self.fitnesses) / len(self.fitnesses))
        
        # 计算最优工期（使用当前选定的解码器，不导出Excel）
        if hasattr(self.decoder_func, '__name__') and 'progressive_spatial' in self.decoder_func.__name__:
            # 如果是渐进式空间约束解码器，不导出Excel
            best_schedule = self.decoder_func(self.best_solution, 
                                              self.project_data, 
                                              self.worker_group,
                                              export_excel=False)
        else:
            # 其他解码器不支持Excel导出参数
            best_schedule = self.decoder_func(self.best_solution, 
                                              self.project_data, 
                                              self.worker_group)
        self.history['best_makespan'].append(best_schedule.get_makespan())
    
    def _print_final_result(self, schedule: Schedule):
        """打印最终结果"""
        # 构建队伍名称映射（从WORKER_CONFIG读取）
        team_name_map = self._get_team_name_map()
        
        stats = schedule.get_statistics(team_name_map)
        
        print(f"\n最优调度方案统计:")
        print(f"  总工期: {stats['总工期']} 天")
        
        # 动态打印各队完工时间（从worker_group获取队伍列表）
        for team_id in sorted(self.worker_group.teams.keys()):
            team_name = team_name_map.get(team_id, f'队伍{team_id}')
            key = f'{team_name}完工时间'
            if key in stats:
                print(f"  {key}: {stats[key]} 天")
        
        print(f"  负载不均衡度: {stats['负载不均衡度']} 天")
        print(f"  管线总数: {stats['管线总数']}")
        print(f"  总寸径: {stats['总寸径']:.2f}")
    
    @staticmethod
    def _get_team_name_map() -> Dict[int, str]:
        """从配置读取队伍名称映射"""
        team_name_map = {}
        
        # 尝试从WORKER_CONFIG读取
        if isinstance(WORKER_CONFIG, list):
            for config in WORKER_CONFIG:
                team_id = config.get('team_id')
                team_name = config.get('name')
                if team_id and team_name:
                    team_name_map[team_id] = team_name
        
        return team_name_map


# 全局函数（用于多进程）
def _calculate_fitness_wrapper(chromosome, project_data, worker_group, use_segments):
    """
    适应度计算包装函数（用于多进程）
    
    注意：每个进程需要独立的 WorkerGroup 实例
    
    参数:
        use_segments: 是否使用空间约束（segments）
    """
    # 为每个进程创建独立的 WorkerGroup（包含单元限制和daily_capacity）
    from ..models import WorkerGroup as WG
    local_worker_group = WG(
        worker_group.teams, 
        worker_group.team_units,
        worker_group.daily_capacity
    )
    
    # 选择解码器
    if use_segments:
        decoder_func = decode_with_progressive_spatial_constraint
        # 解码并计算适应度（不导出Excel）
        schedule = decoder_func(chromosome, project_data, local_worker_group, export_excel=False)
    else:
        decoder_func = decode_chromosome_with_package_priority
        # 解码并计算适应度
        schedule = decoder_func(chromosome, project_data, local_worker_group)
    makespan = schedule.get_makespan()
    
    if makespan <= 0:
        return 1e-10
    
    return 1.0 / makespan

