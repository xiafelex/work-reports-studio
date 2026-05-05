"""
空间数据处理模块
负责工区划分、块分配、管线分段等空间相关处理
"""
from math import floor
from collections import defaultdict
from typing import List, Dict, Tuple
from src.models import Zone, PipelineSegment, WeldPoint, ProjectData, create_default_zones


def read_zone_coordinates() -> List[Zone]:
    """
    加载内置工区配置
    
    Returns:
        List[Zone]: 工区对象列表
    """
    print("\n[步骤1] 加载工区配置...")
    
    zones = create_default_zones()
    
    unit_zones = defaultdict(int)
    for zone in zones:
        unit_zones[zone.unit_name] += 1
    
    print(f"成功创建 {len(zones)} 个工区对象（覆盖 {len(unit_zones)} 个单元）")
    
    return zones


def assign_weld_points_to_zones(
    weld_points: List[WeldPoint],
    zones: List[Zone]
) -> Tuple[int, int]:
    """
    为焊口分配所属的工区（不再使用块）
    
    Args:
        weld_points: 焊口列表
        zones: 工区列表
    
    Returns:
        Tuple[int, int]: (成功分配的焊口数, 未分配的焊口数)
    """
    print("\n[步骤2] 为焊口分配工区...")
    
    assigned_count = 0
    unassigned_count = 0
    
    for weld_point in weld_points:
        # 检查是否有坐标
        if weld_point.x is None or weld_point.y is None or weld_point.z is None:
            unassigned_count += 1
            continue
        
        x, y, z = weld_point.x, weld_point.y, weld_point.z
        
        # 1. 尝试找到包含该点的工区
        found = False
        for zone in zones:
            if zone.contains_point(x, y, z):
                weld_point.zone_name = zone.zone_name
                assigned_count += 1
                found = True
                break
        
        # 2. 如果不在任何工区内，找最近的工区
        if not found:
            min_distance = float('inf')
            nearest_zone = None
            
            for zone in zones:
                distance = zone.distance_to_point(x, y, z)
                if distance < min_distance:
                    min_distance = distance
                    nearest_zone = zone
            
            if nearest_zone:
                weld_point.zone_name = nearest_zone.zone_name
                assigned_count += 1
            else:
                unassigned_count += 1
    
    print(f"焊口工区分配完成: 成功分配 {assigned_count} 个, 未分配 {unassigned_count} 个")
    
    return assigned_count, unassigned_count


def calculate_grid_id(x: float, y: float, z: float, zone_name: str, grid_size: float) -> Tuple[str, int, int, int]:
    """
    计算焊口所属的网格ID
    
    Args:
        x, y, z: 焊口坐标（毫米）
        zone_name: 工区名称
        grid_size: 网格尺寸
    
    Returns:
        Tuple[str, int, int, int]: (zone_name, grid_x, grid_y, grid_z)
    """
    if grid_size is None or grid_size <= 0:
        raise ValueError(f"工区'{zone_name}'的grid_size配置无效: {grid_size}")
    
    grid_x = floor(x / grid_size)
    grid_y = floor(y / grid_size)
    grid_z = floor(z / grid_size)
    
    return (zone_name, grid_x, grid_y, grid_z)


def assign_weld_points_to_grids(project_data: ProjectData):
    """
    为所有焊口分配网格ID
    
    Args:
        project_data: 项目数据
    """
    print("\n[步骤3] 为焊口分配网格...")
    
    assigned_count = 0
    
    for weld_point in project_data.weld_points:
        if weld_point.x is None or weld_point.zone_name is None:
            continue
        
        zone = project_data.get_zone(weld_point.zone_name)
        if not zone or not zone.grid_enabled:
            continue
        
        grid_size = zone.grid_size
        if grid_size is None or grid_size <= 0:
            raise ValueError(f"工区'{zone.zone_name}'缺少有效的网格尺寸配置")
        weld_point.grid_id = calculate_grid_id(
            weld_point.x,
            weld_point.y,
            weld_point.z,
            weld_point.zone_name,
            grid_size
        )
        assigned_count += 1
    
    print(f"网格分配完成: {assigned_count} 个焊口分配了网格ID（仅统计启用网格的工区）")


def create_pipeline_segments(
    project_data: ProjectData,
    zones: List[Zone]
) -> List[PipelineSegment]:
    """
    智能管线分段：根据管径决定是否分段
    
    Args:
        project_data: 项目数据（包含原始管线）
        zones: 工区列表
    
    Returns:
        List[PipelineSegment]: 管线段列表
    """
    from ..config import LARGE_DIAMETER_THRESHOLD
    
    print(f"\n[步骤4] 智能管线分段（大管径阈值: {LARGE_DIAMETER_THRESHOLD}）...")
    
    segments = []
    large_diameter_count = 0
    small_diameter_count = 0
    
    for pipeline in project_data.pipelines:
        # 计算管线平均管径（暂时默认200，后续可从焊口数据获取）
        avg_diameter = 200.0  # TODO: 从焊口数据中获取实际管径
        
        if avg_diameter > LARGE_DIAMETER_THRESHOLD:
            # 大管径：按工区分段
            large_diameter_count += 1
            
            # 按工区分组该管线的焊口
            zone_groups = defaultdict(list)
            
            for weld_point in pipeline.weld_points:
                # 跳过已焊接的焊口
                if getattr(weld_point, 'is_welded', False):
                    continue
                    
                if weld_point.zone_name:
                    zone_groups[weld_point.zone_name].append(weld_point)
                else:
                    # 无工区的焊口归入"Unknown"
                    zone_groups['Unknown'].append(weld_point)
            
            # 为每个工区创建一个管线段
            for zone_name, weld_points in zone_groups.items():
                # 跳过空段（没有未焊接焊口）
                if not weld_points:
                    continue
                    
                segment = PipelineSegment(
                    segment_id=f"{pipeline.pipeline_no}__{zone_name}",
                    original_pipeline_no=pipeline.pipeline_no,
                    zone_name=zone_name,
                    unit_name=pipeline.unit_name,
                    package_no=pipeline.package_no,
                    weld_points=weld_points
                )
                
                # 收集该段涉及的网格ID
                grids_in_segment = set()
                for wp in weld_points:
                    if wp.grid_id:
                        grids_in_segment.add(wp.grid_id)
                segment.grid_ids = grids_in_segment
                
                segments.append(segment)
        else:
            # 小管径：不分段，整条管线作为一个段
            small_diameter_count += 1
            
            # 过滤未焊接焊口
            unwelded_weld_points = [wp for wp in pipeline.weld_points 
                                  if not getattr(wp, 'is_welded', False)]
            
            # 确定主要工区（焊口最多的工区）
            zone_counts = defaultdict(int)
            for weld_point in unwelded_weld_points:
                zone_name = weld_point.zone_name or 'Unknown'
                zone_counts[zone_name] += 1
            
            # 跳过没有未焊接焊口的管线
            if not unwelded_weld_points:
                continue
                
            main_zone = max(zone_counts.keys(), key=lambda z: zone_counts[z]) if zone_counts else 'Unknown'
            
            # 创建单个段（包含整条管线的所有焊口）
            segment = PipelineSegment(
                segment_id=f"{pipeline.pipeline_no}__{main_zone}",
                original_pipeline_no=pipeline.pipeline_no,
                zone_name=main_zone,
                unit_name=pipeline.unit_name,
                package_no=pipeline.package_no,
                weld_points=unwelded_weld_points.copy()
            )
            
            # 收集该段涉及的网格ID
            grids_in_segment = set()
            for wp in unwelded_weld_points:
                if wp.grid_id:
                    grids_in_segment.add(wp.grid_id)
            segment.grid_ids = grids_in_segment
            
            segments.append(segment)
    
    original_pipeline_count = len(project_data.pipelines)
    avg_segments_per_pipeline = len(segments) / original_pipeline_count if original_pipeline_count > 0 else 0
    
    print(f"智能分段完成:")
    print(f"  大管径管线: {large_diameter_count}条（分段处理）")
    print(f"  小管径管线: {small_diameter_count}条（整条处理）")
    print(f"  原始 {original_pipeline_count} 条管线 → {len(segments)} 个段 "
          f"(平均{avg_segments_per_pipeline:.2f}段/管线)")
    
    return segments


def create_topology_segments(
    project_data: ProjectData
) -> List['TopologySegment']:
    """
    基于拓扑结构创建管线段
    
    策略：
    - 每条管线生成2个段：主管线段 + 支管线段（支管合集）
    - 支管线段可能为空
    - 保持拓扑焊接顺序
    
    Args:
        project_data: 项目数据对象
    
    Returns:
        拓扑管线段列表
    """
    from ..models.data_model import TopologySegment
    from .pipeline_topology import PipelineTopologyBuilder
    
    print(f"\n[步骤] 基于拓扑结构创建管线段...")
    
    topology_segments = []
    topology_builder = PipelineTopologyBuilder(silent_mode=True)
    main_segment_count = 0
    branch_segment_count = 0
    empty_branch_count = 0
    
    # 为每条管线创建拓扑段
    for pipeline in project_data.pipelines:
        if not pipeline.weld_points:
            continue
        
        try:
            # 调用拓扑构建器获取主管线和支管线
            topology_result = topology_builder.build_all_pipeline_topology(pipeline.weld_points)
            
            if pipeline.pipeline_no not in topology_result:
                print(f"  警告: 管线 {pipeline.pipeline_no} 拓扑构建失败，跳过")
                continue
            
            topology_data = topology_result[pipeline.pipeline_no]
            main_line = topology_data['main_line']
            # PipelineTopologyBuilder 返回的支管键名为 'branch_pipelines'
            branch_lines = topology_data['branch_pipelines']
            
            # 创建焊口查找字典
            weld_dict = {wp.weld_no: wp for wp in pipeline.weld_points}
            
            # 1. 创建主管线段
            main_weld_points = []
            for weld_no in main_line:
                if weld_no in weld_dict:
                    main_weld_points.append(weld_dict[weld_no])
            
            if main_weld_points:
                main_segment = TopologySegment(
                    segment_id=f"{pipeline.pipeline_no}__MAIN",
                    original_pipeline_no=pipeline.pipeline_no,
                    segment_type="main",
                    weld_sequence=main_line.copy(),
                    weld_points=main_weld_points,
                    unit_name=pipeline.unit_name,
                    package_no=pipeline.package_no
                )
                topology_segments.append(main_segment)
                main_segment_count += 1
            
            # 2. 创建支管线段（合并所有支管）
            if branch_lines:
                all_branch_welds = []
                branch_sequence = []
                
                for branch in branch_lines:
                    for weld_no in branch:
                        if weld_no in weld_dict:
                            all_branch_welds.append(weld_dict[weld_no])
                            branch_sequence.append(weld_no)
                
                if all_branch_welds:
                    branch_segment = TopologySegment(
                        segment_id=f"{pipeline.pipeline_no}__BRANCH",
                        original_pipeline_no=pipeline.pipeline_no,
                        segment_type="branch",
                        weld_sequence=branch_sequence,
                        weld_points=all_branch_welds,
                        unit_name=pipeline.unit_name,
                        package_no=pipeline.package_no
                    )
                    topology_segments.append(branch_segment)
                    branch_segment_count += 1
                else:
                    empty_branch_count += 1
            else:
                empty_branch_count += 1
                
        except Exception as e:
            print(f"  警告: 管线 {pipeline.pipeline_no} 拓扑段创建失败: {e}")
            continue
    
    print(f"拓扑段创建完成:")
    print(f"  主管线段: {main_segment_count}个")
    print(f"  支管线段: {branch_segment_count}个")
    print(f"  空支管线: {empty_branch_count}条")
    print(f"  总段数: {len(topology_segments)}个")
    
    # 统计管径分布
    large_diameter_segments = [s for s in topology_segments if s.can_parallel_weld]
    small_diameter_segments = [s for s in topology_segments if not s.can_parallel_weld]
    
    print(f"  大管径段(>600mm): {len(large_diameter_segments)}个（可并行焊接）")
    print(f"  小管径段(≤600mm): {len(small_diameter_segments)}个（单焊工焊接）")
    
    return topology_segments


def process_spatial_data(project_data: ProjectData) -> bool:
    """
    处理所有空间相关数据
    
    Args:
        project_data: 项目数据对象
    
    Returns:
        bool: 是否成功处理
    """
    print("\n" + "="*60)
    print("空间数据处理")
    print("="*60)
    
    # 1. 读取工区坐标
    zones = read_zone_coordinates()
    if not zones:
        print("\n警告: 未能读取工区数据，空间约束功能将被禁用")
        return False
    
    # 添加工区到项目数据
    for zone in zones:
        project_data.add_zone(zone)
    
    # 2. 为焊口分配工区
    assigned_count, unassigned_count = assign_weld_points_to_zones(
        project_data.weld_points,
        zones
    )
    
    if assigned_count == 0:
        print("\n警告: 没有焊口被分配到工区，空间约束功能将被禁用")
        return False
    
    # 3. 为焊口分配网格ID
    assign_weld_points_to_grids(project_data)
    
    # 4. 管线分段（按工区）
    segments = create_pipeline_segments(project_data, zones)
    for segment in segments:
        project_data.add_segment(segment)
    
    # 5. 拓扑管线段（按拓扑结构）
    topology_segments = create_topology_segments(project_data)
    for topology_segment in topology_segments:
        project_data.add_topology_segment(topology_segment)
    
    print("\n" + "="*60)
    print("空间数据处理完成！")
    print("="*60)
    print(f"\n最终统计:")
    print(f"  工区数: {len(project_data.zones)}")
    print(f"  工区分段数: {len(project_data.segments)}")
    print(f"  拓扑分段数: {len(project_data.topology_segments)}")
    print(f"  分配了工区的焊口: {assigned_count}")
    print(f"  未分配工区的焊口: {unassigned_count}")
    
    return True

