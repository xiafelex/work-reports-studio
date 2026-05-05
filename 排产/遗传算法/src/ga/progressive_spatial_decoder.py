"""
渐进式空间约束解码器
基于焊接顺序的渐进式网格占用策略
"""
from typing import List, Set, Dict, Tuple, Optional
from collections import defaultdict
from math import ceil
import pandas as pd
import os
from ..models import ProjectData, WorkerGroup, Schedule, Task, Pipeline
from ..processing.pipeline_topology import PipelineTopologyBuilder


def decode_with_progressive_spatial_constraint(
    chromosome: List[str],
    project_data: ProjectData,
    worker_group: WorkerGroup,
    daily_capacity: float = 25,
    export_excel: bool = False
) -> Schedule:
    """
    渐进式空间约束解码器
    
    核心逻辑：
    1. 为每条管线构建焊接顺序（基于拓扑）
    2. 焊工驱动调度，选择最早可用的焊工
    3. 渐进式网格占用：只占用当前焊接位置的网格
    4. 跨工区管线必须连续完成
    
    Args:
        chromosome: 管线ID列表（决定管线优先级）
        project_data: 项目数据
        worker_group: 焊工组
        daily_capacity: 默认每日焊接容量
    
    Returns:
        Schedule: 调度方案
    """
    worker_group.reset()
    schedule = Schedule()
    
    # 创建拓扑构建器（按需构建，不预先构建所有管线）
    topology_builder = PipelineTopologyBuilder(silent_mode=True)  # 使用静默模式提升性能
    
    # 跟踪状态
    grid_occupancy: Dict[Tuple, Dict[int, Tuple[float, float]]] = defaultdict(dict)
    pipeline_locks: Dict[str, Tuple[int, float]] = {}  # 管线锁定（跨工区管线）
    worker_daily_zones: Dict[Tuple[int, int], str] = {}  # 焊工每日工区限制
    
    # 从管线段重构管线对象（现在小管径管线已经不分段了）
    reconstructed_pipelines = {}
    
    for segment_id in chromosome:
        segment = project_data.get_segment(segment_id)
        if segment:
            # 直接将段转换为管线对象
            pipeline = Pipeline(
                pipeline_no=segment.segment_id,  # 内部使用段ID作为管线ID
                total_inches=segment.total_inches,
                weld_count=segment.weld_count,
                package_no=segment.package_no or "Unknown",
                unit_name=segment.unit_name,
                weld_points=segment.weld_points.copy()
            )
            # 保存原始管线号用于Excel导出
            pipeline.original_pipeline_no = segment.original_pipeline_no
            reconstructed_pipelines[segment.segment_id] = pipeline
    
    # 管线优先级索引（基于段ID在染色体中的位置）
    pipeline_priority = {segment_id: idx for idx, segment_id in enumerate(chromosome)}
    
    unassigned_pipelines = set(reconstructed_pipelines.keys())
    
    # 收集焊口详细信息用于Excel导出
    weld_details = []
    
    # 初始化焊工状态
    for worker in worker_group.workers:
        worker.available_time = 0.0
        worker.current_pipeline = None
        worker.current_weld_index = 0
        worker.current_weld_remaining = 0.0  # 当前焊口剩余工作量
        worker.current_weld_start_day = None  # 当前焊口开始天数
    
    # 主循环：按天驱动
    current_day = 0
    max_days = 1000  # 防止死循环
    
    while unassigned_pipelines and current_day < max_days:
        
        # 每天开始时，为空闲焊工分配新管线
        for worker in worker_group.workers:
            if worker.current_pipeline is None:
                # 焊工空闲，尝试分配新管线
                selected_pipeline = _find_best_pipeline_for_worker(
                    worker=worker,
                    unassigned_pipelines=unassigned_pipelines,
                    pipeline_priority=pipeline_priority,
                    reconstructed_pipelines=reconstructed_pipelines,
                    project_data=project_data,
                    worker_group=worker_group,
                    grid_occupancy=grid_occupancy,
                    pipeline_locks=pipeline_locks,
                    worker_daily_zones=worker_daily_zones,
                    current_time=float(current_day)
                )
                
                if selected_pipeline:
                    # 分配管线给焊工
                    _start_pipeline_for_worker(
                        worker=worker,
                        pipeline=selected_pipeline,
                        topology_builder=topology_builder,
                        pipeline_locks=pipeline_locks,
                        worker_daily_zones=worker_daily_zones,
                        current_day=current_day,
                        project_data=project_data
                    )
                    unassigned_pipelines.discard(selected_pipeline.pipeline_no)
        
        # 执行一天的工作
        day_weld_details = _execute_daily_work(
            worker_group=worker_group,
            current_day=current_day,
            schedule=schedule,
            grid_occupancy=grid_occupancy,
            pipeline_locks=pipeline_locks,
            worker_daily_zones=worker_daily_zones,
            project_data=project_data,
            daily_capacity=daily_capacity
        )
        
        # 收集焊口详细信息
        weld_details.extend(day_weld_details)
        
        current_day += 1
    
    # 只在需要时导出焊口详细信息到Excel
    if export_excel:
        _export_weld_details_to_excel(weld_details)
    
    return schedule




def _find_best_pipeline_for_worker(
    worker,
    unassigned_pipelines: set,
    pipeline_priority: Dict[str, int],
    reconstructed_pipelines: Dict[str, Pipeline],
    project_data: ProjectData,
    worker_group: WorkerGroup,
    grid_occupancy: Dict,
    pipeline_locks: Dict,
    worker_daily_zones: Dict,
    current_time: float
):
    """
    为焊工从未分配管线池中按优先级选择最合适的管线
    
    优先级策略：
    1. 队伍-单元限制
    2. 跨工区管线的连续性要求
    3. 工区每日限制
    4. 起始网格可用性
    5. 染色体顺序
    """
    candidates = []
    
    for pipeline_id in unassigned_pipelines:
        pipeline = reconstructed_pipelines.get(pipeline_id)
        if not pipeline or not pipeline.weld_points:
            continue
        
        # 约束1：队伍-单元限制
        if not worker_group.can_team_work_on_unit(worker.team_id, pipeline.unit_name):
            continue
        
        # 约束2：跨工区管线锁定检查
        if pipeline.pipeline_no in pipeline_locks:
            locked_worker, lock_end = pipeline_locks[pipeline.pipeline_no]
            if locked_worker != worker.worker_id and lock_end > current_time:
                continue  # 被其他焊工锁定
        
        # 获取起始焊口（焊接顺序的第一个）
        start_weld_point = pipeline.weld_points[0]
        
        # 约束3：工区每日限制
        start_day = int(current_time)
        if (worker.worker_id, start_day) in worker_daily_zones:
            if worker_daily_zones[(worker.worker_id, start_day)] != start_weld_point.zone_name:
                continue  # 当天已在其他工区工作
        
        # 约束4：起始网格可用性检查
        if not _can_start_pipeline_at_time(
            pipeline, worker, current_time, grid_occupancy, project_data
        ):
            continue
        
        # 计算优先级得分
        priority_score = 0.0
        
        # 跨工区管线优先级更高（避免被打断）
        if pipeline.is_cross_zone:
            priority_score += 1000.0
        
        # 管线规模优先级（大管线优先）
        priority_score += pipeline.total_inches
        
        # 获取管线优先级
        priority_pos = pipeline_priority.get(pipeline.pipeline_no, 999999)
        
        # 加入候选（优先级、染色体位置、管线对象）
        candidates.append((priority_score, priority_pos, pipeline))
    
    if not candidates:
        return None
    
    # 排序：优先级降序 > 染色体位置升序
    candidates.sort(key=lambda x: (-x[0], x[1]))
    
    return candidates[0][2]


def _can_start_pipeline_at_time(
    pipeline: Pipeline,
    worker,
    current_time: float,
    grid_occupancy: Dict,
    project_data: ProjectData
) -> bool:
    """
    检查是否可以在指定时间开始焊接管线
    
    只检查起始焊口的网格可用性（渐进式占用）
    """
    if not pipeline.weld_points:
        return True
    
    start_weld_point = pipeline.weld_points[0]
    
    # 检查起始焊口的网格
    if not start_weld_point.grid_id:
        return True  # 无网格约束
    
    zone = project_data.get_zone(start_weld_point.zone_name)
    if not zone or not zone.grid_enabled:
        return True  # 网格未启用
    
    max_workers_allowed = zone.max_workers_per_grid
    if max_workers_allowed is None or max_workers_allowed <= 0:
        return True  # 无容量限制
    
    # 检查起始网格在当前时间的可用性
    grid_id = start_weld_point.grid_id
    start_time = current_time
    end_time = start_time + 1.0  # 检查第一天
    
    return _check_grid_available_at_time(
        grid_id, worker.worker_id, start_time, end_time, grid_occupancy, max_workers_allowed
    )


def _check_grid_available_at_time(
    grid_id: Tuple,
    worker_id: int,
    start_time: float,
    end_time: float,
    grid_occupancy: Dict,
    max_workers_per_grid: int
) -> bool:
    """检查网格在指定时间段的可用性"""
    workers_in_grid = grid_occupancy[grid_id]
    
    occupied_count = 0
    for wid, (occ_start, occ_end) in workers_in_grid.items():
        # 检查时间是否重叠
        if occ_start < end_time and occ_end > start_time:
            if wid == worker_id:
                continue  # 同一个焊工，不算占用
            occupied_count += 1
    
    return occupied_count < max_workers_per_grid


def _assign_pipeline_to_worker_progressive(
    worker,
    pipeline: Pipeline,
    topology_builder: PipelineTopologyBuilder,
    grid_occupancy: Dict,
    pipeline_locks: Dict,
    worker_daily_zones: Dict,
    schedule: Schedule,
    project_data: ProjectData,
    daily_capacity: float
):
    """
    渐进式为焊工分配管线
    
    核心逻辑：
    1. 按需构建管线拓扑（只在需要时构建）
    2. 按焊接顺序逐个焊接焊口
    3. 只占用当前焊接位置的网格
    4. 跨工区管线全程锁定
    5. 动态释放已完成的网格
    
    Returns:
        List[Dict]: 焊口详细信息列表，用于Excel导出
    """
    
    # 收集焊口详细信息
    weld_details = []
    
    # 按需构建拓扑（只在实际分配时构建）
    if not hasattr(pipeline, '_topology_built') or not pipeline._topology_built:
        # 构建拓扑并更新焊接顺序
        ordered_weld_points = topology_builder.build_pipeline_topology(pipeline)
        pipeline.weld_points = ordered_weld_points
        
        # 为焊口添加序号
        for i, wp in enumerate(ordered_weld_points):
            wp.sequence_in_pipeline = i + 1
        
        # 检查是否跨工区
        zones_involved = set()
        for wp in ordered_weld_points:
            if wp.zone_name:
                zones_involved.add(wp.zone_name)
        
        pipeline.is_cross_zone = len(zones_involved) > 1
        pipeline.zones_involved = list(zones_involved)
        
        # 标记已构建拓扑
        pipeline._topology_built = True
    
    # 跨工区管线全程锁定
    if pipeline.is_cross_zone:
        pipeline_locks[pipeline.pipeline_no] = (worker.worker_id, float('inf'))
    
    worker_capacity = getattr(worker, 'daily_capacity', daily_capacity)
    current_time = worker.available_time
    
    # 按焊接顺序逐个处理焊口
    for i, weld_point in enumerate(pipeline.weld_points):
        current_day = int(current_time)
        
        # 工区每日限制检查
        if (worker.worker_id, current_day) in worker_daily_zones:
            existing_zone = worker_daily_zones[(worker.worker_id, current_day)]
            if existing_zone != weld_point.zone_name:
                # 跨工区，跳到下一天
                current_time = float(current_day + 1)
                current_day = int(current_time)
        
        # 记录当天工区
        worker_daily_zones[(worker.worker_id, current_day)] = weld_point.zone_name
        
        # 计算焊接时间（按天计算）
        welding_days = ceil(weld_point.diameter / worker_capacity)  # 向上取整到整天
        task_start = current_time
        task_end = task_start + welding_days
        
        # 创建任务
        task = Task(
            pipeline_no=pipeline.pipeline_no,
            worker_id=worker.worker_id,
            team_id=worker.team_id,
            start_time=task_start,
            end_time=task_end,
            total_inches=weld_point.diameter,
            package_no=pipeline.package_no,
            unit_name=pipeline.unit_name,
            zone_name=weld_point.zone_name
        )
        schedule.add_task(task)
        
        # 收集焊口详细信息
        weld_detail = {
            '管线号': pipeline.pipeline_no,
            '焊口号': weld_point.weld_no,
            '焊工ID': worker.worker_id,
            '队伍': worker.team_id,
            '开始时间': task_start,
            '结束时间': task_end,
            '工期': welding_days,
            '管线寸径': weld_point.diameter,
            '试压包号': pipeline.package_no,
            '单元名称': pipeline.unit_name,
            '工区': weld_point.zone_name
        }
        weld_details.append(weld_detail)
        
        # 渐进式网格占用
        if weld_point.grid_id:
            zone = project_data.get_zone(weld_point.zone_name)
            if zone and zone.grid_enabled:
                # 占用当前网格
                grid_occupancy[weld_point.grid_id][worker.worker_id] = (task_start, task_end)
                
                # 释放前一个网格（如果不同）
                if i > 0:
                    prev_weld_point = pipeline.weld_points[i-1]
                    if (prev_weld_point.grid_id and 
                        prev_weld_point.grid_id != weld_point.grid_id and
                        worker.worker_id in grid_occupancy[prev_weld_point.grid_id]):
                        del grid_occupancy[prev_weld_point.grid_id][worker.worker_id]
        
        # 更新时间
        current_time = task_end
    
    # 更新焊工可用时间
    worker.available_time = current_time
    
    # 释放管线锁定
    if pipeline.pipeline_no in pipeline_locks:
        del pipeline_locks[pipeline.pipeline_no]
    
    # 释放最后一个网格
    if pipeline.weld_points:
        last_weld_point = pipeline.weld_points[-1]
        if (last_weld_point.grid_id and 
            worker.worker_id in grid_occupancy[last_weld_point.grid_id]):
            del grid_occupancy[last_weld_point.grid_id][worker.worker_id]
    
    return weld_details


def _start_pipeline_for_worker(
    worker,
    pipeline: Pipeline,
    topology_builder: PipelineTopologyBuilder,
    pipeline_locks: Dict,
    worker_daily_zones: Dict,
    current_day: int,
    project_data: ProjectData
):
    """
    为焊工开始一条新管线
    """
    # 按需构建拓扑（使用原始完整管线数据）
    if not hasattr(pipeline, '_topology_built') or not pipeline._topology_built:
        # 从项目数据中获取原始完整管线（包含已焊接焊口）
        original_pipeline_no = getattr(pipeline, 'original_pipeline_no', pipeline.pipeline_no)
        original_pipeline = None
        
        # 查找原始管线
        for orig_pipeline in project_data.pipelines:
            if orig_pipeline.pipeline_no == original_pipeline_no:
                original_pipeline = orig_pipeline
                break
        
        if original_pipeline:
            # 使用完整管线构建拓扑
            ordered_weld_points = topology_builder.build_pipeline_topology(original_pipeline)
            # 只保留未焊接的焊口用于调度
            unwelded_ordered_points = [wp for wp in ordered_weld_points 
                                     if not getattr(wp, 'is_welded', False)]
            pipeline.weld_points = unwelded_ordered_points
        else:
            # 回退到原有逻辑
            ordered_weld_points = topology_builder.build_pipeline_topology(pipeline)
            pipeline.weld_points = ordered_weld_points
        
        # 为未焊接焊口分配序号
        for i, wp in enumerate(pipeline.weld_points):
            wp.sequence_in_pipeline = i + 1
        
        zones_involved = set()
        for wp in pipeline.weld_points:
            if wp.zone_name:
                zones_involved.add(wp.zone_name)
        
        pipeline.is_cross_zone = len(zones_involved) > 1
        pipeline.zones_involved = list(zones_involved)
        pipeline._topology_built = True
    
    # 分配管线给焊工
    worker.current_pipeline = pipeline
    worker.current_weld_index = 0
    worker.current_weld_remaining = 0.0
    worker.current_weld_start_day = None
    
    # 跨工区管线全程锁定
    if pipeline.is_cross_zone:
        pipeline_locks[pipeline.pipeline_no] = (worker.worker_id, float('inf'))


def _execute_daily_work(
    worker_group,
    current_day: int,
    schedule: Schedule,
    grid_occupancy: Dict,
    pipeline_locks: Dict,
    worker_daily_zones: Dict,
    project_data: ProjectData,
    daily_capacity: float
) -> List[Dict]:
    """
    执行一天的工作，返回当天的焊口详细信息
    """
    day_weld_details = []
    
    for worker in worker_group.workers:
        if worker.current_pipeline is None:
            continue
            
        pipeline = worker.current_pipeline
        worker_capacity = getattr(worker, 'daily_capacity', daily_capacity)
        remaining_capacity = worker_capacity
        
        # 处理当天的焊接工作
        while (remaining_capacity > 0 and 
               worker.current_weld_index < len(pipeline.weld_points)):
            
            weld_point = pipeline.weld_points[worker.current_weld_index]
            
            # 跳过已焊接的焊口
            if getattr(weld_point, 'is_welded', False):
                worker.current_weld_index += 1
                continue
            
            # 检查工区限制
            if (worker.worker_id, current_day) in worker_daily_zones:
                existing_zone = worker_daily_zones[(worker.worker_id, current_day)]
                if existing_zone != weld_point.zone_name:
                    # 跨工区，今天不能继续
                    break
            
            # 记录当天工区
            worker_daily_zones[(worker.worker_id, current_day)] = weld_point.zone_name
            
            # 如果是新焊口，初始化
            if worker.current_weld_remaining == 0.0:
                worker.current_weld_remaining = weld_point.diameter
                worker.current_weld_start_day = current_day
            
            # 计算今天可以完成的工作量
            work_done = min(remaining_capacity, worker.current_weld_remaining)
            
            # 创建任务记录
            task = Task(
                pipeline_no=getattr(pipeline, 'original_pipeline_no', pipeline.pipeline_no),
                worker_id=worker.worker_id,
                team_id=worker.team_id,
                start_time=float(current_day),
                end_time=float(current_day + 1),
                total_inches=work_done,
                package_no=pipeline.package_no,
                unit_name=pipeline.unit_name,
                zone_name=weld_point.zone_name
            )
            schedule.add_task(task)
            
            # 计算当天工作的开始和结束时间（小数天）
            day_start_time = current_day + (worker_capacity - remaining_capacity) / worker_capacity
            day_end_time = day_start_time + work_done / worker_capacity
            
            # 记录每天的工作详细信息
            weld_detail = {
                '管线号': getattr(pipeline, 'original_pipeline_no', pipeline.pipeline_no),
                '焊口号': weld_point.weld_no,
                '焊工ID': worker.worker_id,
                '队伍': worker.team_id,
                '开始时间': round(day_start_time, 2),
                '结束时间': round(day_end_time, 2),
                '工期': round(work_done / worker_capacity, 2),
                '管线寸径': round(work_done, 1),  # 当天完成的寸径
                '试压包号': pipeline.package_no,
                '单元名称': pipeline.unit_name,
                '工区': weld_point.zone_name
            }
            day_weld_details.append(weld_detail)
            
            # 更新工作进度
            worker.current_weld_remaining -= work_done
            remaining_capacity -= work_done
            
            # 如果焊口完成，移动到下一个焊口
            if worker.current_weld_remaining <= 0:
                worker.current_weld_index += 1
                worker.current_weld_remaining = 0.0
                worker.current_weld_start_day = None
        
        # 检查管线是否完成
        if worker.current_weld_index >= len(pipeline.weld_points):
            # 管线完成，释放焊工
            worker.current_pipeline = None
            worker.current_weld_index = 0
            worker.current_weld_remaining = 0.0
            worker.current_weld_start_day = None
            
            # 释放管线锁定
            if pipeline.pipeline_no in pipeline_locks:
                del pipeline_locks[pipeline.pipeline_no]
    
    return day_weld_details


def _export_weld_details_to_excel(weld_details: List[Dict]):
    """
    将焊口详细信息导出到Excel文件
    
    Args:
        weld_details: 焊口详细信息列表
    """
    if not weld_details:
        return
    
    # 创建DataFrame
    df = pd.DataFrame(weld_details)
    
    # 确保列顺序
    columns_order = [
        '管线号', '焊口号', '焊工ID', '队伍', '开始时间', 
        '结束时间', '工期', '管线寸径', '试压包号', '单元名称', '工区'
    ]
    df = df[columns_order]
    
    # 格式化时间列（保留小数）
    df['开始时间'] = df['开始时间'].round(2)
    df['结束时间'] = df['结束时间'].round(2)
    df['工期'] = df['工期'].round(2)
    df['管线寸径'] = df['管线寸径'].round(1)
    
    # 确保输出目录存在
    output_dir = "output_files"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成文件名（包含时间戳）
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{output_dir}/焊口详细信息_{timestamp}.xlsx"
    
    # 导出到Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='焊口详细信息', index=False)
        
        # 获取工作表并设置列宽
        worksheet = writer.sheets['焊口详细信息']
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"✅ 焊口详细信息已导出到: {filename}")
    print(f"   总焊口数: {len(weld_details)}")
    
    # 统计信息
    total_welders = df['焊工ID'].nunique()
    total_teams = df['队伍'].nunique()
    total_zones = df['工区'].nunique()
    total_duration = df['工期'].sum()
    
    print(f"   焊工数: {total_welders}, 队伍数: {total_teams}, 工区数: {total_zones}")
    print(f"   总工期: {total_duration}天")
