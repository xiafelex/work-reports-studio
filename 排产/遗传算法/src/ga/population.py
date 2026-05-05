"""
种群初始化模块
职责：生成初始种群（融入领域知识）
"""
import random
from typing import List
from ..models.data_model import ProjectData


def initialize_population(population_size: int,
                          project_data: ProjectData,
                          use_segments: bool = False,
                          use_topology_segments: bool = False) -> List[List[str]]:
    """
    初始化种群
    
    策略：
    - 20%: 试压包优先 + 包内按寸径降序
    - 20%: 全局寸径降序
    - 60%: 随机排列（保证多样性）
    
    参数:
        population_size: 种群大小
        project_data: 项目数据对象（ProjectData）
        use_segments: 是否使用工区管线段（True=段，False=管线）
        use_topology_segments: 是否使用拓扑管线段（优先级最高）
    
    返回:
        种群（染色体列表）
    """
    population = []
    
    # 根据优先级决定使用哪种编码方式
    if use_topology_segments and len(project_data.topology_segments) > 0:
        all_items = project_data.get_topology_segment_ids()
        encoding_type = "topology_segments"
        print(f"种群初始化: 使用拓扑段编码，共{len(all_items)}个段")
    elif use_segments and len(project_data.segments) > 0:
        all_items = project_data.get_segment_ids()
        encoding_type = "segments"
        print(f"种群初始化: 使用工区段编码，共{len(all_items)}个段")
    else:
        all_items = project_data.get_pipeline_ids()
        encoding_type = "pipelines"
        print(f"种群初始化: 使用管线编码，共{len(all_items)}条管线")
    
    # 策略1: 试压包优先 + 包内按寸径降序 (20%)
    package_priority_count = int(population_size * 0.2)
    for _ in range(package_priority_count):
        chromosome = _generate_package_priority_chromosome(project_data, encoding_type)
        population.append(chromosome)
    
    # 策略2: 全局寸径降序 (20%)
    global_sorted_count = int(population_size * 0.2)
    for _ in range(global_sorted_count):
        chromosome = _generate_global_sorted_chromosome(project_data, encoding_type)
        population.append(chromosome)
    
    # 策略3: 完全随机 (60%)
    remaining = population_size - len(population)
    for _ in range(remaining):
        chromosome = all_items.copy()
        random.shuffle(chromosome)
        population.append(chromosome)
    
    return population


def _generate_package_priority_chromosome(project_data: ProjectData, encoding_type: str) -> List[str]:
    """
    生成试压包优先的染色体
    
    逻辑：
    1. 按试压包分组（排除Unknown）
    2. 每个包内按寸径降序排列（先焊长的）
    3. 试压包之间随机排列
    4. Unknown管线/段放到最后
    """
    chromosome = []
    
    if encoding_type == "topology_segments":
        # 使用拓扑管线段
        from collections import defaultdict
        package_segments = defaultdict(list)
        unknown_segments = []
        
        for segment in project_data.topology_segments:
            if segment.is_empty:  # 跳过空段
                continue
            if segment.package_no == 'Unknown' or segment.package_no is None:
                unknown_segments.append(segment)
            else:
                package_segments[segment.package_no].append(segment)
        
        # 随机排序试压包
        package_nos = list(package_segments.keys())
        random.shuffle(package_nos)
        
        # 按试压包处理，包内按管径降序（大管径优先）
        for package_no in package_nos:
            segments = package_segments[package_no]
            # 按管径降序排列
            segments.sort(key=lambda s: s.diameter, reverse=True)
            chromosome.extend([s.segment_id for s in segments])
        
        # Unknown段放到最后，也按管径降序
        unknown_segments.sort(key=lambda s: s.diameter, reverse=True)
        chromosome.extend([s.segment_id for s in unknown_segments])
        
    elif encoding_type == "segments":
        # 使用管线段
        # 按试压包分组segment
        from collections import defaultdict
        package_segments = defaultdict(list)
        unknown_segments = []
        
        for segment in project_data.segments:
            if segment.package_no == 'Unknown' or segment.package_no is None:
                unknown_segments.append(segment)
            else:
                package_segments[segment.package_no].append(segment)
        
        # 随机排序试压包
        package_nos = list(package_segments.keys())
        random.shuffle(package_nos)
        
        # 遍历每个试压包
        for package_no in package_nos:
            # 包内段按寸径降序排列
            sorted_segments = sorted(package_segments[package_no],
                                    key=lambda s: s.total_inches,
                                    reverse=True)
            chromosome.extend([s.segment_id for s in sorted_segments])
        
        # Unknown段按寸径降序排列，放到最后
        if unknown_segments:
            sorted_unknown = sorted(unknown_segments,
                                   key=lambda s: s.total_inches,
                                   reverse=True)
            chromosome.extend([s.segment_id for s in sorted_unknown])
    else:
        # 使用管线（原逻辑）
        normal_packages = []
        unknown_pipelines = []
        
        for package in project_data.packages:
            if package.package_no == 'Unknown' or package.package_no is None:
                unknown_pipelines.extend(package.pipelines)
            else:
                normal_packages.append(package)
        
        # 随机排序正常试压包
        random.shuffle(normal_packages)
        
        # 遍历每个正常试压包
        for package in normal_packages:
            # 包内管线按寸径降序排列
            sorted_pipelines = sorted(package.pipelines, 
                                     key=lambda p: p.total_inches, 
                                     reverse=True)
            
            # 添加到染色体
            chromosome.extend([p.pipeline_no for p in sorted_pipelines])
        
        # Unknown管线按寸径降序排列，放到最后
        if unknown_pipelines:
            sorted_unknown = sorted(unknown_pipelines,
                                   key=lambda p: p.total_inches,
                                   reverse=True)
            chromosome.extend([p.pipeline_no for p in sorted_unknown])
    
    return chromosome


def _generate_global_sorted_chromosome(project_data: ProjectData, encoding_type: str) -> List[str]:
    """
    生成全局寸径降序的染色体
    
    逻辑：
    所有管线/段按寸径从大到小排列，但Unknown管线/段放到最后
    """
    if encoding_type == "topology_segments":
        # 使用拓扑管线段
        normal_segments = []
        unknown_segments = []
        
        for segment in project_data.topology_segments:
            if segment.is_empty:  # 跳过空段
                continue
            if segment.package_no == 'Unknown' or segment.package_no is None:
                unknown_segments.append(segment)
            else:
                normal_segments.append(segment)
        
        # 正常段按管径降序排列
        normal_segments.sort(key=lambda s: s.diameter, reverse=True)
        unknown_segments.sort(key=lambda s: s.diameter, reverse=True)
        
        chromosome = ([s.segment_id for s in normal_segments] + 
                     [s.segment_id for s in unknown_segments])
        
    elif encoding_type == "segments":
        # 使用管线段
        normal_segments = []
        unknown_segments = []
        
        for segment in project_data.segments:
            if segment.package_no == 'Unknown' or segment.package_no is None:
                unknown_segments.append(segment)
            else:
                normal_segments.append(segment)
        
        # 分别按寸径降序排列
        sorted_normal = sorted(normal_segments,
                              key=lambda s: s.total_inches,
                              reverse=True)
        sorted_unknown = sorted(unknown_segments,
                               key=lambda s: s.total_inches,
                               reverse=True)
        
        # 拼接：正常段在前，Unknown在后
        result = [s.segment_id for s in sorted_normal]
        result.extend([s.segment_id for s in sorted_unknown])
    else:
        # 使用管线（原逻辑）
        normal_pipelines = []
        unknown_pipelines = []
        
        for pipeline in project_data.pipelines:
            if pipeline.package_no == 'Unknown' or pipeline.package_no is None:
                unknown_pipelines.append(pipeline)
            else:
                normal_pipelines.append(pipeline)
        
        # 分别按寸径降序排列
        sorted_normal = sorted(normal_pipelines, 
                              key=lambda p: p.total_inches, 
                              reverse=True)
        sorted_unknown = sorted(unknown_pipelines,
                               key=lambda p: p.total_inches,
                               reverse=True)
        
        # 拼接：正常管线在前，Unknown在后
        result = [p.pipeline_no for p in sorted_normal]
        result.extend([p.pipeline_no for p in sorted_unknown])
    
    return result

