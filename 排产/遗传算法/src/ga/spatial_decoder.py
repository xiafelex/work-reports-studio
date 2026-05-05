"""
空间约束解码器（焊工驱动 + 优先级选段 + 惩罚机制）
核心逻辑：
- 焊工驱动调度：选择最早可用的焊工
- 优先级选段：为焊工从待分配池中按优先级选择段
  主优先级：同网格+同管线(10) > 同网格+同包(8) > 同工区+同包(6) > 同网格+同单元(4) > 同工区+同单元(2)
  次级优先级：全局协调，倾向于和其他焊工处理同包/同单元（0.6/0.3）
- 惩罚机制：适应度函数中惩罚跨包跳跃次数（continuity_weight）
- 染色体作用：相同优先级时按染色体顺序选择，保证遗传算法有效
- 逐天分配并记录（精细到每天每个焊工焊接了什么）
 - 网格容量由工区配置决定
- 焊工当天不能跨工区
"""
from typing import List, Set, Dict, Tuple, Optional
from collections import defaultdict
from math import ceil
from ..models import ProjectData, WorkerGroup, Schedule, Task


def decode_with_spatial_constraint(
    chromosome: List[str],
    project_data: ProjectData,
    worker_group: WorkerGroup,
    daily_capacity: float = 25
) -> Schedule:
    """
    焊工驱动 + 优先级选段的空间约束解码器
    
    核心逻辑：
    1. 染色体提供"待分配段池"和"染色体顺序权重"
    2. 选择最早可用的焊工
    3. 为该焊工从待分配池中按优先级选择最合适的段
       - 优先级相同时，染色体靠前的段优先
    4. 重复2-3直到所有段被分配
    
    参数:
        chromosome: 段ID列表（决定段的备选池和顺序权重）
        project_data: 项目数据
        worker_group: 焊工组
        daily_capacity: 默认每日焊接容量
    
    返回:
        Schedule: 调度方案
    """
    worker_group.reset()
    schedule = Schedule()
    
    # 跟踪状态
    grid_occupancy: Dict[Tuple, Dict[int, Tuple[float, float]]] = defaultdict(dict)
    pipeline_locks: Dict[str, Tuple[int, float]] = {}
    worker_daily_zones: Dict[Tuple[int, int], str] = {}
    worker_current_state: Dict[int, Dict] = {}
    
    # 待分配段集合（来自染色体）
    unassigned_segments = set(chromosome)
    
    # 染色体顺序索引（用于在相同优先级时排序）
    chromosome_order = {seg_id: idx for idx, seg_id in enumerate(chromosome)}
    
    # 主循环：直到所有段被分配
    while unassigned_segments:
        # 1. 找到最早可用的焊工
        earliest_worker = min(worker_group.workers, key=lambda w: w.available_time)
        current_time = earliest_worker.available_time
        
        # 2. 为这个焊工从未分配池中按优先级选择段
        selected_segment = _find_best_segment_for_worker(
            worker=earliest_worker,
            unassigned_segments=unassigned_segments,
            chromosome_order=chromosome_order,
            project_data=project_data,
            worker_group=worker_group,
            grid_occupancy=grid_occupancy,
            pipeline_locks=pipeline_locks,
            worker_daily_zones=worker_daily_zones,
            worker_current_state=worker_current_state,
            current_time=current_time
        )
        
        if not selected_segment:
            # 这个焊工找不到可做的段
            # 检查是否有其他焊工可以工作
            other_workers_busy = any(
                w.worker_id != earliest_worker.worker_id and w.available_time < current_time + 1.0
                for w in worker_group.workers
            )
            
            if other_workers_busy:
                # 有其他焊工在工作，让这个焊工等待0.5天
                earliest_worker.available_time += 0.5
            else:
                # 所有焊工都空闲，跳到下一天
                current_day = int(current_time)
                earliest_worker.available_time = float(current_day + 1)
            
            # 检查是否所有焊工都卡住了
            if all(w.available_time > current_time + 100 for w in worker_group.workers):
                # 防止死循环：如果所有焊工都已跳过太多天，停止
                break
            continue
        
        # 3. 分配这个段给焊工
        _assign_segment_to_worker_daily(
            worker=earliest_worker,
            segment=selected_segment,
            grid_occupancy=grid_occupancy,
            pipeline_locks=pipeline_locks,
            worker_daily_zones=worker_daily_zones,
            worker_current_state=worker_current_state,
            schedule=schedule,
            project_data=project_data,
            daily_capacity=daily_capacity
        )
        
        # 4. 从未分配池中移除
        unassigned_segments.discard(selected_segment.segment_id)
    
    return schedule


def _find_best_segment_for_worker(
    worker,
    unassigned_segments: set,
    chromosome_order: Dict[str, int],
    project_data: ProjectData,
    worker_group: WorkerGroup,
    grid_occupancy: Dict,
    pipeline_locks: Dict,
    worker_daily_zones: Dict,
    worker_current_state: Dict,
    current_time: float
):
    """
    为焊工从未分配段池中按优先级选择最合适的段（纯惩罚机制，无等待）
    
    主优先级（0-10分）：
    10. 同网格 + 同管线
    8.  同网格 + 同试压包
    6.  同工区 + 同试压包
    4.  同网格 + 同单元
    2.  同工区 + 同单元
    0.  无连续性
    
    次级优先级（+0.0-0.6分）：
    +0.6 如果有其他焊工在处理同包
    +0.3 如果有其他焊工在处理同单元
    
    相同优先级时，按染色体顺序选择（靠前的优先）
    
    返回: PipelineSegment 或 None
    """
    worker_state = worker_current_state.get(worker.worker_id, {})
    current_grids = worker_state.get('grids', set())
    current_zone = worker_state.get('zone', '')
    current_pipeline = worker_state.get('pipeline', '')
    current_package = worker_state.get('package', '')
    current_unit = worker_state.get('unit', '')
    
    candidates = []
    
    for seg_id in unassigned_segments:
        segment = project_data.get_segment(seg_id)
        if not segment or not segment.zone_name:
            continue
        
        # 约束1：队伍-单元限制
        if not worker_group.can_team_work_on_unit(worker.team_id, segment.unit_name):
            continue
        
        # 约束2：工区每日限制
        start_day = int(current_time)
        if (worker.worker_id, start_day) in worker_daily_zones:
            if worker_daily_zones[(worker.worker_id, start_day)] != segment.zone_name:
                continue
        
        # 约束3：管线锁定
        if segment.original_pipeline_no in pipeline_locks:
            locked_worker, lock_end = pipeline_locks[segment.original_pipeline_no]
            if locked_worker != worker.worker_id and lock_end > current_time:
                continue
        
        # 约束4：网格可用性（按工区配置）
        worker_capacity = getattr(worker, 'daily_capacity', 25.0)
        days_needed = ceil(segment.total_inches / worker_capacity)
        
        zone = project_data.get_zone(segment.zone_name) if segment.zone_name else None
        grid_enabled = zone.grid_enabled if zone else True
        max_workers_allowed = zone.max_workers_per_grid if zone else None
        
        can_execute = True
        if grid_enabled and segment.grid_ids:
            if max_workers_allowed is None or max_workers_allowed <= 0:
                raise ValueError(f"工区'{segment.zone_name}'缺少有效的网格容量配置")
            for day_offset in range(days_needed):
                check_day = int(current_time) + day_offset
                check_time = float(check_day)
                
                if not _check_grids_available_at_time(
                    segment.grid_ids,
                    worker.worker_id,
                    check_time,
                    check_time + 1.0,
                    grid_occupancy,
                    max_workers_allowed
                ):
                    can_execute = False
                    break
        
        if not can_execute:
            continue
        
        # 计算优先级得分（0-10范围，整数部分是主优先级，小数部分是次级优先级）
        priority_score = 0.0
        
        if worker_state:
            has_same_grid = bool(current_grids & segment.grid_ids)
            
            # 1. 同网格 + 同管线（最高优先级）
            if has_same_grid and current_pipeline == segment.original_pipeline_no:
                priority_score = 10.0
            # 2. 同网格 + 同试压包
            elif has_same_grid and current_package == segment.package_no and segment.package_no:
                priority_score = 8.0
            # 3. 同工区 + 同试压包
            elif current_zone == segment.zone_name and current_package == segment.package_no and segment.package_no:
                priority_score = 6.0
            # 4. 同网格 + 同单元
            elif has_same_grid and current_unit == segment.unit_name:
                priority_score = 4.0
            # 5. 同工区 + 同单元
            elif current_zone == segment.zone_name and current_unit == segment.unit_name:
                priority_score = 2.0
        
        # 次级优先级：即使没有连续性，也倾向于同包/同单元（让染色体策略生效）
        # 使用小数部分（0.1-0.9），不影响主优先级
        if priority_score < 2.0:  # 只在低优先级时添加次级优先级
            secondary_score = 0.0
            
            # 查找是否有任何焊工正在处理这个段的包/单元（全局倾向）
            for wid, wstate in worker_current_state.items():
                if not wstate:
                    continue
                
                # 如果有焊工在处理同包，次级优先级+0.6
                if wstate.get('package') == segment.package_no and segment.package_no:
                    secondary_score = max(secondary_score, 0.6)
                    break
                # 如果有焊工在处理同单元，次级优先级+0.3
                elif wstate.get('unit') == segment.unit_name:
                    secondary_score = max(secondary_score, 0.3)
            
            priority_score += secondary_score
        
        # 获取染色体顺序
        chrom_pos = chromosome_order.get(seg_id, 999999)
        
        # 加入候选（优先级、染色体位置、段ID、段对象）
        candidates.append((priority_score, chrom_pos, seg_id, segment))
    
    if not candidates:
        return None
    
    # 排序：优先级降序 > 染色体位置升序 > 段ID升序
    candidates.sort(key=lambda x: (-x[0], x[1], x[2]))
    
    # 选择最佳候选（不再使用等待机制，完全依赖惩罚机制）
    return candidates[0][3]


def _check_grids_available_at_time(
    grid_ids: set,
    worker_id: int,
    start_time: float,
    end_time: float,
    grid_occupancy: Dict,
    max_workers_per_grid: int
) -> bool:
    """
    检查在时间段[start_time, end_time)内，段的所有网格是否可用
    """
    for grid_id in grid_ids:
        workers_in_grid = grid_occupancy[grid_id]
        
        # 检查网格中的每个焊工
        occupied_count = 0
        for wid, (occ_start, occ_end) in workers_in_grid.items():
            # 检查时间是否重叠
            if occ_start < end_time and occ_end > start_time:
                if wid == worker_id:
                    # 同一个焊工，不算占用
                    continue
                occupied_count += 1
        
        # 如果占用数达到上限，则不可用
        if occupied_count >= max_workers_per_grid:
            return False
    
    return True


def _assign_segment_to_worker_daily(
    worker,
    segment,
    grid_occupancy: Dict,
    pipeline_locks: Dict,
    worker_daily_zones: Dict,
    worker_current_state: Dict,
    schedule: Schedule,
    project_data: ProjectData,
    daily_capacity: float
):
    """
    逐天为焊工分配段，并精细记录
    
    流程：
    1. 从worker.available_time开始
    2. 每天最多焊接daily_capacity寸
    3. 记录每天的任务
    4. 更新占用状态和焊工当前状态
    """
    zone = project_data.get_zone(segment.zone_name) if segment.zone_name else None
    grid_enabled = zone.grid_enabled if zone else False
    
    worker_capacity = getattr(worker, 'daily_capacity', daily_capacity)
    remaining_inches = segment.total_inches
    current_time = worker.available_time
    
    while remaining_inches > 0:
        current_day = int(current_time)
        
        # 计算当天已经工作的时间
        day_start = float(current_day)
        time_used_today = current_time - day_start
        
        # 计算今天还能焊接多少
        capacity_remaining_today = worker_capacity * (1.0 - time_used_today)
        welded_today = min(remaining_inches, capacity_remaining_today)
        
        if welded_today <= 0:
            # 今天已经工作满了，跳到明天
            current_time = float(current_day + 1)
            continue
        
        # 计算任务时间
        task_start = current_time
        task_duration = welded_today / worker_capacity
        task_end = task_start + task_duration
        
        # 记录任务
        task = Task(
            pipeline_no=segment.original_pipeline_no,
            worker_id=worker.worker_id,
            team_id=worker.team_id,
            start_time=task_start,
            end_time=task_end,
            total_inches=welded_today,
            package_no=segment.package_no,
            unit_name=segment.unit_name,
            zone_name=segment.zone_name
        )
        schedule.add_task(task)
        
        # 更新状态
        remaining_inches -= welded_today
        current_time = task_end
        
        # 占用网格（整个段的所有网格）
        if grid_enabled and segment.grid_ids:
            for grid_id in segment.grid_ids:
                if worker.worker_id not in grid_occupancy[grid_id]:
                    grid_occupancy[grid_id][worker.worker_id] = (task_start, task_end)
                else:
                    # 扩展占用时间
                    old_start, old_end = grid_occupancy[grid_id][worker.worker_id]
                    grid_occupancy[grid_id][worker.worker_id] = (min(old_start, task_start), max(old_end, task_end))
        
        # 占用管线
        if segment.original_pipeline_no not in pipeline_locks:
            pipeline_locks[segment.original_pipeline_no] = (worker.worker_id, task_end)
        else:
            old_worker, old_end = pipeline_locks[segment.original_pipeline_no]
            pipeline_locks[segment.original_pipeline_no] = (worker.worker_id, max(old_end, task_end))
        
        # 记录工区限制
        worker_daily_zones[(worker.worker_id, current_day)] = segment.zone_name
    
    # 更新焊工可用时间
    worker.available_time = current_time
    
    # 更新焊工当前状态（用于后续优先级判断）
    worker_current_state[worker.worker_id] = {
        'grids': segment.grid_ids.copy(),
        'zone': segment.zone_name,
        'pipeline': segment.original_pipeline_no,
        'package': segment.package_no,
        'unit': segment.unit_name
    }
    
    # 段完成后释放网格和管线锁
    if grid_enabled and segment.grid_ids:
        for grid_id in segment.grid_ids:
            if worker.worker_id in grid_occupancy[grid_id]:
                del grid_occupancy[grid_id][worker.worker_id]
    
    if segment.original_pipeline_no in pipeline_locks:
        del pipeline_locks[segment.original_pipeline_no]
