"""
染色体解码模块（带试压包硬约束）
职责：将染色体（管线排列）转换为实际调度方案，强制试压包优先规则
"""
from typing import List, Set, Dict
from collections import defaultdict
from ..models import Schedule, Task, WorkerGroup, ProjectData, Worker
from ..config import WORKER_CONFIG


def decode_chromosome_with_package_priority(
        chromosome: List[str], 
        project_data: ProjectData,
        worker_group: WorkerGroup,
        daily_capacity: float = None,
        working_hours_per_day: float = None
    ) -> Schedule:
        """
        解码染色体为调度方案（带试压包硬约束）
        
        硬约束规则：
        1. 焊工完成一条管线后，优先焊接同一试压包内的其他管线
        2. 只有当该试压包内没有可焊接管线时，才考虑其他试压包
        3. 没有试压包的管线（Unknown）放到最后处理
        
        参数:
            chromosome: 染色体（管线号的排列）
            project_data: 项目数据对象（ProjectData）
            worker_group: 焊工组对象
            daily_capacity: 每组每天焊接寸数
            working_hours_per_day: 每天工作小时数
        
        返回:
            Schedule对象，包含所有任务
        """
        # 从配置获取默认值
        from ..config import WORKING_HOURS_PER_DAY as DEFAULT_WORKING_HOURS
        
        if daily_capacity is None:
            # 从worker_group获取daily_capacity
            daily_capacity = getattr(worker_group, 'daily_capacity', 25)
        
        if working_hours_per_day is None:
            working_hours_per_day = DEFAULT_WORKING_HOURS
        
        # 重置所有焊工的可用时间
        worker_group.reset()
        
        # 创建调度方案
        schedule = Schedule()
        
        # 注意：染色体中的管线已经在种群初始化前预先筛选过，
        # 只包含该组能焊的管线，无需再次过滤
        
        # 已分配的管线集合
        assigned_pipelines: Set[str] = set()
        
        # 按试压包分组管线（排除Unknown）
        package_pipelines: Dict[str, List[str]] = defaultdict(list)
        unknown_pipelines: List[str] = []
        
        for pipeline_no in chromosome:
            pipeline = project_data.get_pipeline(pipeline_no)
            if pipeline:
                if pipeline.package_no == 'Unknown' or pipeline.package_no is None:
                    unknown_pipelines.append(pipeline_no)
                else:
                    package_pipelines[pipeline.package_no].append(pipeline_no)
        
        # 记录每个焊工当前正在处理的试压包和单元
        worker_current_package: Dict[int, str] = {}
        worker_current_unit: Dict[int, str] = {}
        
        # 标记哪些焊工已经没有可做的管线了
        workers_with_no_task: Set[int] = set()
        
        # 主循环：直到所有管线都被分配
        while len(assigned_pipelines) < len(chromosome):
            # 如果所有焊工都没有可做的管线，退出循环
            if len(workers_with_no_task) >= len(worker_group.workers):
                break
            
            # 找到最早可用且还有可做任务的焊工
            available_workers = [w for w in worker_group.workers if w.worker_id not in workers_with_no_task]
            if not available_workers:
                break
            
            earliest_worker = min(available_workers, key=lambda w: w.available_time)
            
            # 获取该焊工当前的试压包和单元
            current_package = worker_current_package.get(earliest_worker.worker_id)
            current_unit = worker_current_unit.get(earliest_worker.worker_id)
            
            # 选择该焊工能做的下一条管线（考虑队伍-单元限制）
            next_pipeline_no = _select_next_pipeline(
                current_package=current_package,
                current_unit=current_unit,
                package_pipelines=package_pipelines,
                unknown_pipelines=unknown_pipelines,
                assigned_pipelines=assigned_pipelines,
                chromosome=chromosome,
                project_data=project_data,
                worker_group=worker_group,
                worker_team_id=earliest_worker.team_id
            )
            
            if next_pipeline_no is None:
                # 这个焊工找不到能做的管线了，标记它
                workers_with_no_task.add(earliest_worker.worker_id)
                continue
            
            # 获取管线对象
            pipeline = project_data.get_pipeline(next_pipeline_no)
            if pipeline is None:
                assigned_pipelines.add(next_pipeline_no)
                continue
            
            # 计算所需工作天数
            # 使用该焊工的 daily_capacity（支持不同队伍有不同效率）
            # 例如：50寸，该焊工每天能焊25寸，需要2天
            worker_capacity = getattr(earliest_worker, 'daily_capacity', daily_capacity)
            days_needed = pipeline.total_inches / worker_capacity
            
            # 分配任务
            start_time = earliest_worker.available_time
            end_time = start_time + days_needed
            
            # 创建任务
            task = Task(
                pipeline_no=pipeline.pipeline_no,
                worker_id=earliest_worker.worker_id,
                team_id=earliest_worker.team_id,
                start_time=start_time,
                end_time=end_time,
                total_inches=pipeline.total_inches,
                package_no=pipeline.package_no,
                unit_name=pipeline.unit_name
            )
            
            schedule.add_task(task)
            
            # 更新状态
            earliest_worker.available_time = end_time
            assigned_pipelines.add(next_pipeline_no)
            worker_current_package[earliest_worker.worker_id] = pipeline.package_no
            worker_current_unit[earliest_worker.worker_id] = pipeline.unit_name
            
            # 成功分配任务后，清除该焊工的"无任务"标记
            # （因为分配了新任务后，状态可能改变，可能又有新的可做任务了）
            if earliest_worker.worker_id in workers_with_no_task:
                workers_with_no_task.remove(earliest_worker.worker_id)
        
        return schedule


def _select_next_pipeline(
        current_package: str,
        current_unit: str,
        package_pipelines: Dict[str, List[str]],
        unknown_pipelines: List[str],
        assigned_pipelines: Set[str],
        chromosome: List[str],
        project_data,
        worker_group,
        worker_team_id: int
    ) -> str:
        """
        根据试压包和单元优先规则选择下一条管线（同时考虑队伍-单元限制）
        
        优先级：
        1. 当前试压包内未分配且该队伍可以焊的管线（如果有）
        2. 当前单元内未分配且该队伍可以焊的管线（试压包焊完后）
        3. 染色体中下一条未分配且该队伍可以焊的管线（非Unknown）
        4. Unknown管线且该队伍可以焊（最后处理）
        """
        # 优先级1: 当前试压包内还有未分配的管线
        if current_package and current_package != 'Unknown':
            for pipeline_no in package_pipelines[current_package]:
                if pipeline_no not in assigned_pipelines:
                    pipeline = project_data.get_pipeline(pipeline_no)
                    if pipeline and worker_group.can_team_work_on_unit(worker_team_id, pipeline.unit_name):
                        return pipeline_no
        
        # 优先级2: 当前单元内还有未分配的管线（排除Unknown）
        if current_unit:
            for pipeline_no in chromosome:
                if pipeline_no not in assigned_pipelines:
                    # 检查是否是Unknown
                    if pipeline_no in unknown_pipelines:
                        continue
                    # 获取管线对象
                    pipeline = project_data.get_pipeline(pipeline_no)
                    if pipeline and pipeline.unit_name == current_unit:
                        if worker_group.can_team_work_on_unit(worker_team_id, pipeline.unit_name):
                            return pipeline_no
        
        # 优先级3: 按染色体顺序选择下一条未分配的管线（非Unknown）
        for pipeline_no in chromosome:
            if pipeline_no not in assigned_pipelines:
                # 检查是否是Unknown
                is_unknown = pipeline_no in unknown_pipelines
                if not is_unknown:
                    pipeline = project_data.get_pipeline(pipeline_no)
                    if pipeline and worker_group.can_team_work_on_unit(worker_team_id, pipeline.unit_name):
                        return pipeline_no
        
        # 优先级4: Unknown管线
        for pipeline_no in unknown_pipelines:
            if pipeline_no not in assigned_pipelines:
                pipeline = project_data.get_pipeline(pipeline_no)
                if pipeline and worker_group.can_team_work_on_unit(worker_team_id, pipeline.unit_name):
                    return pipeline_no
        
        return None

