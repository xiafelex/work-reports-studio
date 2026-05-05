"""
遗传算子模块
职责：选择、交叉、变异操作
"""
import random
from typing import List, Tuple
import pandas as pd


def tournament_selection(population: List[List[str]],
                        fitnesses: List[float],
                        tournament_size: int = 3) -> List[str]:
    """
    锦标赛选择
    
    从种群中随机选择tournament_size个个体，返回适应度最高的
    
    参数:
        population: 种群
        fitnesses: 适应度列表
        tournament_size: 锦标赛大小
    
    返回:
        选中的染色体
    """
    # 随机选择tournament_size个索引
    tournament_indices = random.sample(range(len(population)), tournament_size)
    
    # 找到适应度最高的
    best_idx = max(tournament_indices, key=lambda i: fitnesses[i])
    
    return population[best_idx].copy()


def order_crossover(parent1: List[str], parent2: List[str]) -> List[str]:
    """
    顺序交叉（OX - Order Crossover）
    
    经典的排列编码交叉算子，保证不会产生重复基因
    
    算法：
    1. 随机选择两个切点
    2. 子代继承parent1的中间段
    3. 从parent2中按顺序填充剩余基因
    
    参数:
        parent1: 父代1
        parent2: 父代2
    
    返回:
        子代染色体
    """
    size = len(parent1)
    
    # 随机选择两个切点
    start, end = sorted(random.sample(range(size), 2))
    
    # 子代初始化
    child = [None] * size
    
    # 继承parent1的中间段
    child[start:end] = parent1[start:end]
    
    # 从parent2中按顺序填充剩余位置
    pointer = end  # 从end位置开始填充
    
    # 遍历parent2的顺序（从end开始循环）
    for gene in parent2[end:] + parent2[:end]:
        if gene not in child:
            # 如果指针到末尾，回到开头
            if pointer >= size:
                pointer = 0
            
            child[pointer] = gene
            pointer += 1
    
    return child


def mutate(chromosome: List[str],
          mutation_type: str = 'auto',
          project_data = None) -> List[str]:
    """
    变异操作（增强版：执行多次变异以增强搜索能力）
    
    支持三种变异策略：
    1. swap: 随机交换两个位置
    2. inversion: 随机选一段逆序
    3. package_shuffle: 随机选一个试压包，重排其内部管线（需要project_data）
    
    对于大规模问题（>1000个基因），会执行多次变异以增强效果
    
    参数:
        chromosome: 染色体
        mutation_type: 'auto', 'swap', 'inversion', 'package_shuffle'
        project_data: 项目数据对象（ProjectData，仅package_shuffle需要）
    
    返回:
        变异后的染色体
    """
    # 复制以避免修改原始数据
    mutated = chromosome.copy()
    
    # 根据染色体长度决定变异次数（增强搜索能力）
    chromosome_size = len(chromosome)
    if chromosome_size > 3000:
        num_mutations = random.randint(3, 6)  # 大规模问题：3-6次变异
    elif chromosome_size > 1000:
        num_mutations = random.randint(2, 4)  # 中等规模：2-4次
    else:
        num_mutations = 1  # 小规模：1次
    
    # 执行多次变异
    for _ in range(num_mutations):
        # 自动选择变异类型
        if mutation_type == 'auto':
            if project_data is not None:
                current_type = random.choice(['swap', 'inversion', 'package_shuffle'])
            else:
                current_type = random.choice(['swap', 'inversion'])
        else:
            current_type = mutation_type
        
        # 执行变异
        if current_type == 'swap':
            _swap_mutation(mutated)
        elif current_type == 'inversion':
            _inversion_mutation(mutated)
        elif current_type == 'package_shuffle':
            if project_data is not None:
                _package_shuffle_mutation(mutated, project_data)
            else:
                _swap_mutation(mutated)  # 降级为swap
    
    return mutated


def _swap_mutation(chromosome: List[str]) -> None:
    """
    交换变异（原地修改）
    随机交换两个位置的基因
    """
    if len(chromosome) < 2:
        return
    
    i, j = random.sample(range(len(chromosome)), 2)
    chromosome[i], chromosome[j] = chromosome[j], chromosome[i]


def _inversion_mutation(chromosome: List[str]) -> None:
    """
    逆序变异（原地修改）
    随机选择一段区间，将其逆序
    """
    if len(chromosome) < 2:
        return
    
    start, end = sorted(random.sample(range(len(chromosome)), 2))
    chromosome[start:end] = reversed(chromosome[start:end])


def _package_shuffle_mutation(chromosome: List[str], 
                              project_data) -> None:
    """
    试压包内重排变异（原地修改）
    
    随机选择一个试压包，重新排列其内部的管线
    保持试压包优先的领域知识
    
    参数:
        chromosome: 染色体
        project_data: 项目数据对象（ProjectData）
    """
    # 获取所有试压包
    packages = project_data.packages
    
    if len(packages) == 0:
        return
    
    # 随机选择一个试压包
    selected_package = random.choice(packages)
    
    # 获取该试压包内的管线号列表
    pipelines_in_package = selected_package.get_pipeline_ids()
    
    if len(pipelines_in_package) < 2:
        return
    
    # 找到这些管线在染色体中的位置
    indices = [i for i, pipeline in enumerate(chromosome) 
               if pipeline in pipelines_in_package]
    
    if len(indices) < 2:
        return
    
    # 提取这些位置的值并重新排列
    values = [chromosome[i] for i in indices]
    random.shuffle(values)
    
    # 重新赋值
    for i, val in zip(indices, values):
        chromosome[i] = val

