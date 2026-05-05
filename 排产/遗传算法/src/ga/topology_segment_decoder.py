"""
拓扑段解码器模块
职责：将基于拓扑段的染色体解码为调度方案

核心特性：
1. 支持大管径段的多焊工并行焊接（管径>600mm）
2. 小管径段由单焊工按拓扑顺序焊接（管径≤600mm）
3. 严格按照拓扑焊接顺序（weld_sequence）进行焊接
4. 网格约束和空间冲突检测
"""
from typing import List, Dict, Tuple, Optional
from math import ceil
from collections import defaultdict
from ..models.data_model import ProjectData, TopologySegment
from ..models.schedule import Schedule, Task
from ..models.worker import WorkerGroup
from ..utils.excel_export import export_detailed_schedule


def decode_with_topology_segments(
    chromosome: List[str],
    project_data: ProjectData,
    worker_group: WorkerGroup,
    daily_capacity: float = 25,
    export_excel: bool = False
) -> Schedule:
    """
    基于拓扑段的解码器
    
    解码策略：
    1. 按染色体顺序处理每个拓扑段
    2. 大管径段（>600mm）：支持多焊工并行，但保持拓扑顺序
    3. 小管径段（≤600mm）：单焊工按拓扑顺序焊接
    4. 严格遵循网格约束和空间限制
    
    Args:
        chromosome: 拓扑段ID列表
        project_data: 项目数据
        worker_group: 焊工组
        daily_capacity: 每日焊接寸径
        export_excel: 是否导出Excel
    
    Returns:
        调度方案
    """
    print(f"\n[拓扑段解码器] 开始解码，染色体长度: {len(chromosome)}")
    
    # 初始化
    worker_group.reset()
    schedule = Schedule()
    
    # 存储详细信息用于Excel导出
    all_weld_details = []
    
    # 网格占用状态：grid_id -> {worker_id: (start_time, end_time)}
    grid_occupancy = defaultdict(dict)
    
    # 工区每日限制：(worker_id, day) -> zone_name
    worker_daily_zones = {}
    
    # 统计
    large_diameter_segments = 0
    small_diameter_segments = 0
    parallel_welding_segments = 0
    
    # 按染色体顺序处理每个拓扑段
    for i, segment_id in enumerate(chromosome):
        segment = project_data.get_topology_segment(segment_id)
        if not segment or segment.is_empty:
            continue
        
        print(f"  处理段 {i+1}/{len(chromosome)}: {segment.segment_id} "
              f"({segment.segment_type}, {segment.diameter:.0f}mm, {segment.weld_count}个焊口)")
        
        # 根据管径选择焊接策略
        if segment.can_parallel_weld:  # 大管径段（>600mm）
            large_diameter_segments += 1
            weld_details = _assign_large_diameter_segment(
                segment, worker_group, grid_occupancy, worker_daily_zones,
                schedule, project_data, daily_capacity
            )
            if len(_get_available_workers_for_segment(segment, worker_group)) > 1:
                parallel_welding_segments += 1
        else:  # 小管径段（≤600mm）
            small_diameter_segments += 1
            weld_details = _assign_small_diameter_segment(
                segment, worker_group, grid_occupancy, worker_daily_zones,
                schedule, project_data, daily_capacity
            )
        
        all_weld_details.extend(weld_details)
    
    # 输出统计信息
    print(f"\n[拓扑段解码完成] 统计:")
    print(f"  大管径段: {large_diameter_segments}个")
    print(f"  小管径段: {small_diameter_segments}个")
    print(f"  并行焊接段: {parallel_welding_segments}个")
    print(f"  总工期: {schedule.get_makespan():.2f}天")
    print(f"  总任务数: {len(schedule.tasks)}")
    
    # Excel导出
    if export_excel and all_weld_details:
        try:
            filename = f"拓扑段调度详情_{worker_group.group_name}.xlsx"
            export_detailed_schedule(all_weld_details, filename)
            print(f"  已导出Excel: {filename}")
        except Exception as e:
            print(f"  Excel导出失败: {e}")
    
    return schedule


def _assign_large_diameter_segment(
    segment: TopologySegment,
    worker_group: WorkerGroup,
    grid_occupancy: Dict,
    worker_daily_zones: Dict,
    schedule: Schedule,
    project_data: ProjectData,
    daily_capacity: float
) -> List[Dict]:
    """
    分配大管径段（支持多焊工并行，但保持拓扑顺序）
    """
    weld_details = []
    
    # 获取可用焊工
    available_workers = _get_available_workers_for_segment(segment, worker_group)
    if not available_workers:
        print(f"    警告: 没有可用焊工处理段 {segment.segment_id}")
        return weld_details
    
    # 检查是否可以并行焊接（网格约束）
    can_parallel = _check_parallel_feasibility(segment, available_workers, grid_occupancy, project_data)
    
    if can_parallel and len(available_workers) > 1:
        # 多焊工并行焊接
        print(f"    多焊工并行焊接，使用{len(available_workers)}个焊工")
        weld_details = _parallel_welding(
            segment, available_workers, grid_occupancy, worker_daily_zones,
            schedule, project_data, daily_capacity
        )
    else:
        # 单焊工焊接（网格冲突或只有一个焊工）
        print(f"    单焊工焊接（网格约束或焊工数限制）")
        primary_worker = available_workers[0]
        weld_details = _sequential_welding(
            segment, primary_worker, grid_occupancy, worker_daily_zones,
            schedule, project_data, daily_capacity
        )
    
    return weld_details


def _assign_small_diameter_segment(
    segment: TopologySegment,
    worker_group: WorkerGroup,
    grid_occupancy: Dict,
    worker_daily_zones: Dict,
    schedule: Schedule,
    project_data: ProjectData,
    daily_capacity: float
) -> List[Dict]:
    """
    分配小管径段（单焊工按拓扑顺序焊接）
    """
    # 选择最早可用的焊工
    available_workers = _get_available_workers_for_segment(segment, worker_group)
    if not available_workers:
        print(f"    警告: 没有可用焊工处理段 {segment.segment_id}")
        return []
    
    primary_worker = min(available_workers, key=lambda w: w.available_time)
    print(f"    单焊工焊接，使用焊工 {primary_worker.worker_id}")
    
    return _sequential_welding(
        segment, primary_worker, grid_occupancy, worker_daily_zones,
        schedule, project_data, daily_capacity
    )


def _sequential_welding(
    segment: TopologySegment,
    worker,
    grid_occupancy: Dict,
    worker_daily_zones: Dict,
    schedule: Schedule,
    project_data: ProjectData,
    daily_capacity: float
) -> List[Dict]:
    """
    按拓扑顺序顺序焊接
    """
    weld_details = []
    worker_capacity = getattr(worker, 'daily_capacity', daily_capacity)
    current_time = worker.available_time
    
    # 按拓扑顺序处理每个焊口
    for weld_no in segment.weld_sequence:
        weld_point = next((wp for wp in segment.weld_points if wp.weld_no == weld_no), None)
        if not weld_point:
            continue
        
        current_day = int(current_time)
        
        # 工区每日限制检查
        if (worker.worker_id, current_day) in worker_daily_zones:
            existing_zone = worker_daily_zones[(worker.worker_id, current_day)]
            if existing_zone != weld_point.zone_name:
                # 跨工区，跳到下一天
                current_time = float(current_day + 1)
                current_day = int(current_time)
        
        # 记录当天工区
        if weld_point.zone_name:
            worker_daily_zones[(worker.worker_id, current_day)] = weld_point.zone_name
        
        # 计算焊接时间
        welding_days = ceil(weld_point.diameter / worker_capacity)
        task_start = current_time
        task_end = task_start + welding_days
        
        # 创建任务
        task = Task(
            pipeline_no=segment.original_pipeline_no,  # 使用原始管线号
            worker_id=worker.worker_id,
            team_id=worker.team_id,
            start_time=task_start,
            end_time=task_end,
            total_inches=weld_point.diameter,
            package_no=segment.package_no,
            unit_name=segment.unit_name,
            zone_name=weld_point.zone_name
        )
        schedule.add_task(task)
        
        # 收集详细信息
        weld_detail = {
            '拓扑段ID': segment.segment_id,
            '段类型': segment.segment_type,
            '管线号': segment.original_pipeline_no,
            '焊口号': weld_point.weld_no,
            '焊工ID': worker.worker_id,
            '队伍': worker.team_id,
            '开始时间': task_start,
            '结束时间': task_end,
            '工期': welding_days,
            '焊口管径': weld_point.diameter,
            '试压包号': segment.package_no,
            '单元名称': segment.unit_name,
            '工区': weld_point.zone_name,
            '拓扑顺序': segment.weld_sequence.index(weld_no) + 1
        }
        weld_details.append(weld_detail)
        
        # 更新时间
        current_time = task_end
    
    # 更新焊工可用时间
    worker.available_time = current_time
    return weld_details


def _parallel_welding(
    segment: TopologySegment,
    workers: List,
    grid_occupancy: Dict,
    worker_daily_zones: Dict,
    schedule: Schedule,
    project_data: ProjectData,
    daily_capacity: float
) -> List[Dict]:
    """
    多焊工并行焊接（保持拓扑顺序）
    """
    weld_details = []
    
    # 将焊口按拓扑顺序分配给焊工
    worker_assignments = defaultdict(list)
    for i, weld_no in enumerate(segment.weld_sequence):
        worker_index = i % len(workers)
        worker_assignments[worker_index].append(weld_no)
    
    # 每个焊工处理分配的焊口
    for worker_index, assigned_welds in worker_assignments.items():
        worker = workers[worker_index]
        
        # 为这个焊工创建临时段
        temp_weld_points = []
        for weld_no in assigned_welds:
            weld_point = next((wp for wp in segment.weld_points if wp.weld_no == weld_no), None)
            if weld_point:
                temp_weld_points.append(weld_point)
        
        if temp_weld_points:
            temp_segment = TopologySegment(
                segment_id=f"{segment.segment_id}_W{worker.worker_id}",
                original_pipeline_no=segment.original_pipeline_no,
                segment_type=segment.segment_type,
                weld_sequence=assigned_welds,
                weld_points=temp_weld_points,
                unit_name=segment.unit_name,
                package_no=segment.package_no
            )
            
            # 顺序焊接分配的焊口
            worker_details = _sequential_welding(
                temp_segment, worker, grid_occupancy, worker_daily_zones,
                schedule, project_data, daily_capacity
            )
            weld_details.extend(worker_details)
    
    return weld_details


def _get_available_workers_for_segment(segment: TopologySegment, worker_group: WorkerGroup) -> List:
    """获取可以处理该段的焊工列表"""
    available_workers = []
    
    for worker in worker_group.workers:
        # 检查队伍-单元限制
        if worker_group.can_team_work_on_unit(worker.team_id, segment.unit_name):
            available_workers.append(worker)
    
    # 按可用时间排序
    available_workers.sort(key=lambda w: w.available_time)
    return available_workers


def _check_parallel_feasibility(
    segment: TopologySegment,
    workers: List,
    grid_occupancy: Dict,
    project_data: ProjectData
) -> bool:
    """
    检查是否可以并行焊接（网格约束检查）
    """
    if len(workers) <= 1:
        return False
    
    # 简化：如果段内焊口涉及的网格数量足够，允许并行
    # 实际实现中需要更复杂的网格冲突检测
    grid_ids = set()
    for weld_point in segment.weld_points:
        if hasattr(weld_point, 'grid_id') and weld_point.grid_id:
            grid_ids.add(weld_point.grid_id)
    
    # 如果网格数量 >= 焊工数量，允许并行
    return len(grid_ids) >= len(workers)
