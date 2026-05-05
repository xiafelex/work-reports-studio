"""
管线汇总模块
职责：按管线号汇总数据，生成 Pipeline 对象
"""
from typing import List, Dict, Optional
from collections import defaultdict, Counter
from ..models.data_model import WeldPoint, Pipeline


def _extract_unit_name(weld_points: List[WeldPoint]) -> Optional[str]:
    """
    从焊口列表中提取单元名称
    
    策略：取出现次数最多的非空unit_name，如果都为空则返回None
    
    参数:
        weld_points: 焊口列表
    
    返回:
        单元名称（字符串）或 None
    """
    # 收集所有非空的unit_name
    unit_names = [w.unit_name for w in weld_points 
                  if w.unit_name is not None and str(w.unit_name).strip() != '']
    
    if not unit_names:
        return None
    
    # 取出现次数最多的
    counter = Counter(unit_names)
    most_common = counter.most_common(1)[0][0]
    
    return most_common


def aggregate_pipeline_inches(weld_points: List[WeldPoint]) -> List[Pipeline]:
    """
    按管线号汇总，创建 Pipeline 对象列表
    
    参数:
        weld_points: WeldPoint 对象列表
    
    返回:
        Pipeline 对象列表
    """
    # 按管线号分组
    pipeline_dict: Dict[str, List[WeldPoint]] = defaultdict(list)
    
    for weld_point in weld_points:
        pipeline_dict[weld_point.pipeline_no].append(weld_point)
    
    # 创建 Pipeline 对象
    pipelines = []
    for pipeline_no, welds in pipeline_dict.items():
        total_inches = sum(w.diameter for w in welds)
        weld_count = len(welds)
        
        # 提取单元名称（取出现次数最多的非空unit_name）
        unit_name = _extract_unit_name(welds)
        
        pipeline = Pipeline(
            pipeline_no=pipeline_no,
            total_inches=total_inches,
            weld_count=weld_count,
            package_no='Unknown',  # 暂时未知，后续合并时赋值
            unit_name=unit_name,
            weld_points=welds
        )
        pipelines.append(pipeline)
    
    # 按总寸径降序排列
    pipelines.sort(key=lambda p: p.total_inches, reverse=True)
    
    # 统计信息
    total_inches = sum(p.total_inches for p in pipelines)
    avg_inches = total_inches / len(pipelines) if pipelines else 0
    
    print(f"\n管线汇总完成:")
    print(f"  管线数量: {len(pipelines)}")
    print(f"  总寸径: {total_inches:.2f}")
    print(f"  平均寸径/管线: {avg_inches:.2f}")
    if pipelines:
        print(f"  最大寸径管线: {pipelines[0].pipeline_no} ({pipelines[0].total_inches:.2f}寸)")
        print(f"  最小寸径管线: {pipelines[-1].pipeline_no} ({pipelines[-1].total_inches:.2f}寸)")
    
    return pipelines

