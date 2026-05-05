"""
数据合并模块
职责：合并管线数据和施压包数据，生成 ProjectData 对象
"""
import pandas as pd
from typing import List
from collections import defaultdict
from ..models.data_model import Pipeline, Package, ProjectData, WeldPoint


def merge_package_data(pipelines: List[Pipeline], 
                       package_df: pd.DataFrame,
                       weld_points: List[WeldPoint] = None) -> ProjectData:
    """
    将管线数据与施压包数据合并，生成完整的 ProjectData 对象
    
    参数:
        pipelines: Pipeline 对象列表
        package_df: 施压包数据 DataFrame，包含列：pipeline_no, package_no
        weld_points: WeldPoint 对象列表（可选）
    
    返回:
        ProjectData 对象，包含所有项目数据
    """
    # 创建管线号到试压包号的映射
    package_map = {}
    for _, row in package_df.iterrows():
        pipeline_no = str(row['pipeline_no'])
        package_no = str(row['package_no']) if pd.notna(row['package_no']) else 'Unknown'
        package_map[pipeline_no] = package_no
    
    # 更新 Pipeline 对象的 package_no
    no_package_count = 0
    no_package_pipelines = []
    
    for pipeline in pipelines:
        if pipeline.pipeline_no in package_map:
            pipeline.package_no = package_map[pipeline.pipeline_no]
        else:
            pipeline.package_no = 'Unknown'
            no_package_count += 1
            no_package_pipelines.append(pipeline.pipeline_no)
    
    if no_package_count > 0:
        print(f"\n警告: {no_package_count} 条管线没有分配试压包")
        print(f"未分配的管线: {no_package_pipelines[:10]}")  # 只显示前10个
        if len(no_package_pipelines) > 10:
            print(f"  ... 还有 {len(no_package_pipelines) - 10} 条")
    
    # 按试压包分组创建 Package 对象
    package_dict = defaultdict(list)
    for pipeline in pipelines:
        package_dict[pipeline.package_no].append(pipeline)
    
    packages = []
    for package_no, pkg_pipelines in package_dict.items():
        package = Package(
            package_no=package_no,
            pipelines=pkg_pipelines
        )
        packages.append(package)
    
    # 按管线数量降序排列
    packages.sort(key=lambda p: p.pipeline_count, reverse=True)
    
    # 创建 ProjectData 对象
    project_data = ProjectData()
    
    # 添加焊口
    if weld_points:
        for wp in weld_points:
            project_data.add_weld_point(wp)
    
    # 添加管线
    for pipeline in pipelines:
        project_data.add_pipeline(pipeline)
    
    # 添加试压包
    for package in packages:
        project_data.add_package(package)
    
    # 统计信息
    print(f"\n试压包统计:")
    print(f"  试压包数量: {len(packages)}")
    if packages:
        avg_pipeline_count = sum(p.pipeline_count for p in packages) / len(packages)
        avg_inches = sum(p.total_inches for p in packages) / len(packages)
        print(f"  平均管线数/试压包: {avg_pipeline_count:.2f}")
        print(f"  平均寸径/试压包: {avg_inches:.2f}")
    
    return project_data

