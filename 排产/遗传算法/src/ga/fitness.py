"""
适应度函数模块
职责：计算染色体的适应度
"""
from typing import List
from ..models import Schedule, WorkerGroup, ProjectData
from .decoder_with_constraint import decode_chromosome_with_package_priority
from ..config import GA_CONFIG


def calculate_fitness(chromosome: List[str],
                     project_data: ProjectData,
                     worker_group: WorkerGroup,
                     package_weight: float = None,
                     load_balance_weight: float = None,
                     continuity_weight: float = None,
                     diameter_weight: float = None) -> float:
    """
    计算染色体的适应度
    
    目标：最小化总工期（各队伍独立优化，不考虑均衡）
    
    适应度 = 1 / (总工期 + 试压包分散惩罚 + 负载均衡惩罚 + 连续性惩罚 + 管径惩罚)
    
    参数:
        chromosome: 染色体
        project_data: 项目数据对象（ProjectData）
        worker_group: 焊工组
        package_weight: 试压包分散惩罚权重（None=从配置读取）
        load_balance_weight: 负载均衡惩罚权重（None=从配置读取）
        continuity_weight: 连续性惩罚权重（None=从配置读取）
        diameter_weight: 管径优先惩罚权重（None=从配置读取）
    
    返回:
        适应度值（越大越好）
    """
    # 从配置文件读取权重（如果未指定）
    if package_weight is None:
        package_weight = GA_CONFIG.get('package_weight', 0.0)
    if load_balance_weight is None:
        load_balance_weight = GA_CONFIG.get('load_balance_weight', 0.0)
    if continuity_weight is None:
        continuity_weight = GA_CONFIG.get('continuity_weight', 0.5)  # 默认0.5
    if diameter_weight is None:
        diameter_weight = GA_CONFIG.get('diameter_weight', 0.1)  # 默认0.1
    
    # 解码染色体为调度方案（使用带约束的版本）
    schedule = decode_chromosome_with_package_priority(chromosome, project_data, worker_group)
    
    # 1. 主要目标：总工期（makespan）
    makespan = schedule.get_makespan()
    
    # 防止除零
    if makespan <= 0:
        return 1e-10
    
    # 2. 惩罚项1：试压包分散惩罚
    package_penalty = calculate_package_penalty(schedule, project_data)
    
    # 3. 惩罚项2：负载不均惩罚
    load_balance_penalty = calculate_load_balance_penalty(schedule)
    
    # 4. 惩罚项3：包内连续性惩罚
    continuity_penalty = calculate_continuity_penalty(schedule)
    
    # 5. 惩罚项4：管径优先惩罚（新增）
    diameter_penalty = calculate_diameter_penalty(chromosome, project_data)
    
    # 6. 综合评分
    total_cost = (makespan + 
                  package_weight * package_penalty + 
                  load_balance_weight * load_balance_penalty +
                  continuity_weight * continuity_penalty +
                  diameter_weight * diameter_penalty)
    
    # 6. 适应度（越大越好）
    fitness = 1.0 / max(total_cost, 1e-10)
    
    return fitness


def calculate_package_penalty(schedule: Schedule, 
                              project_data: ProjectData) -> float:
    """
    计算试压包分散惩罚
    
    逻辑：
    同一试压包内的管线，完工时间应该尽量接近（便于后续统一试压）
    如果时间跨度太大，增加惩罚
    
    参数:
        schedule: 调度方案
        project_data: 项目数据对象（ProjectData）
    
    返回:
        惩罚值（越大越差）
    """
    penalty = 0.0
    
    # 遍历所有试压包
    for package in project_data.packages:
        # 获取该试压包内所有管线的完工时间
        completion_times = schedule.get_package_completion_times(package.package_no)
        
        if len(completion_times) > 1:
            # 计算时间跨度
            time_span = max(completion_times) - min(completion_times)
            penalty += time_span
    
    return penalty


def calculate_load_balance_penalty(schedule: Schedule) -> float:
    """
    计算负载不均惩罚
    
    逻辑：
    各队伍的完工时间应该尽量接近（避免某队早早完成，另一队还在加班）
    
    参数:
        schedule: 调度方案
    
    返回:
        惩罚值（越大越差）
    """
    if not schedule.tasks:
        return 0.0
    
    # 从任务中提取所有队伍ID
    team_ids = set(task.team_id for task in schedule.tasks)
    
    # 获取所有队伍的完工时间
    team_times = []
    for team_id in team_ids:
        team_time = schedule.get_team_completion_time(team_id)
        team_times.append(team_time)
    
    if not team_times:
        return 0.0
    
    # 计算各队完工时间的差异
    max_time = max(team_times)
    min_time = min(team_times)
    
    penalty = max_time - min_time
    
    return penalty


def calculate_continuity_penalty(schedule: Schedule) -> float:
    """
    计算包内连续性惩罚（混合方案的惩罚机制部分）
    
    逻辑：
    焊工应该尽量连续焊接同一试压包内的管线/段，减少跨包跳跃
    统计每个焊工的"跨包跳跃次数"，累加作为惩罚
    
    参数:
        schedule: 调度方案
    
    返回:
        惩罚值（越大越差）
    """
    if not schedule.tasks:
        return 0.0
    
    # 按焊工分组任务
    from collections import defaultdict
    worker_tasks = defaultdict(list)
    
    for task in schedule.tasks:
        worker_tasks[task.worker_id].append(task)
    
    # 计算每个焊工的跨包跳跃次数
    total_switches = 0
    
    for worker_id, tasks in worker_tasks.items():
        # 按开始时间排序
        sorted_tasks = sorted(tasks, key=lambda t: t.start_time)
        
        # 统计跨包跳跃
        prev_package = None
        for task in sorted_tasks:
            current_package = task.package_no
            
            # 跳过Unknown包（不计入连续性）
            if current_package == 'Unknown' or current_package is None:
                continue
            
            # 如果从一个包跳到另一个包，计为一次跳跃
            if prev_package is not None and prev_package != current_package:
                total_switches += 1
            
            prev_package = current_package
    
    # 返回跳跃次数作为惩罚（可以根据需要调整权重）
    return float(total_switches)


def calculate_diameter_penalty(chromosome: List[str], project_data: ProjectData) -> float:
    """
    计算管径优先惩罚
    
    逻辑：
    大管径段应该优先安排，后安排的大管径段会产生惩罚
    惩罚 = Σ(位置索引 × 1/管径) for 大管径段
    
    Args:
        chromosome: 染色体（拓扑段ID列表或管线ID列表）
        project_data: 项目数据
    
    Returns:
        管径惩罚值（越小越好）
    """
    if not chromosome:
        return 0.0
    
    penalty = 0.0
    
    # 检查染色体类型：拓扑段 vs 管线/工区段
    if hasattr(project_data, 'topology_segments') and len(project_data.topology_segments) > 0:
        # 拓扑段模式
        for i, segment_id in enumerate(chromosome):
            segment = project_data.get_topology_segment(segment_id)
            if segment and not segment.is_empty:
                diameter = segment.diameter
                if diameter > 600:  # 大管径段
                    # 位置越靠后，惩罚越大；管径越大，惩罚权重越高
                    position_penalty = (i + 1) / len(chromosome)  # 位置惩罚：0.x ~ 1.0
                    diameter_factor = diameter / 1000.0  # 管径因子：0.6+ 
                    penalty += position_penalty * diameter_factor
    
    elif hasattr(project_data, 'segments') and len(project_data.segments) > 0:
        # 工区段模式
        for i, segment_id in enumerate(chromosome):
            segment = project_data.get_segment(segment_id)
            if segment:
                # 计算段的最大管径
                max_diameter = max((wp.diameter for wp in segment.weld_points), default=0)
                if max_diameter > 600:  # 大管径段
                    position_penalty = (i + 1) / len(chromosome)
                    diameter_factor = max_diameter / 1000.0
                    penalty += position_penalty * diameter_factor
    
    else:
        # 管线模式
        for i, pipeline_no in enumerate(chromosome):
            pipeline = project_data.get_pipeline(pipeline_no)
            if pipeline:
                # 计算管线的最大管径
                max_diameter = max((wp.diameter for wp in pipeline.weld_points), default=0)
                if max_diameter > 600:  # 大管径管线
                    position_penalty = (i + 1) / len(chromosome)
                    diameter_factor = max_diameter / 1000.0
                    penalty += position_penalty * diameter_factor
    
    return penalty

