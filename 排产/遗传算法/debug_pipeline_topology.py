#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试管线拓扑和焊接顺序
测试PWW-13908-150-QHB-C50等管线的拓扑构建和焊接顺序获取
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_io import read_weld_info, read_pressure_package, read_pipe_property
from src.processing import initialize_weld_data, aggregate_pipeline_inches, merge_package_data
from src.processing.pipeline_topology import PipelineTopologyBuilder
from src.models import ProjectData
import json

def test_pipeline_topology(target_pipeline_nos=None):
    """测试多条管线拓扑构建和焊接顺序获取
    
    Args:
        target_pipeline_nos: 目标管线编号列表。如果为None，则显示所有管线选择菜单
    """
    
    print("=" * 80)
    print("🧪 测试多条管线拓扑构建和焊接顺序获取")
    print("=" * 80)
    
    # 1. 读取数据
    print("\n📂 1. 读取数据...")
    weld_data = read_weld_info()
    package_data = read_pressure_package()
    pipe_property_data = read_pipe_property()
    
    # 2. 初始化焊口数据
    print("\n⚙️ 2. 初始化焊口数据...")
    weld_points = initialize_weld_data(weld_data, pipe_property_data)
    
    # 3. 聚合管线数据
    print("\n📊 3. 聚合管线数据...")
    pipelines = aggregate_pipeline_inches(weld_points)
    
    # 4. 合并试压包数据
    print("\n🔗 4. 合并试压包数据...")
    pipelines = merge_package_data(pipelines, package_data)
    
    # 5. 如果没有指定管线，显示所有可用管线让用户选择
    if target_pipeline_nos is None:
        print(f"\n📋 可用管线列表:")
        available_pipelines = []
        for i, pipeline in enumerate(pipelines.pipelines, 1):
            if len(pipeline.weld_points) > 5:  # 只显示有足够焊口的管线
                available_pipelines.append(pipeline.pipeline_no)
                print(f"   {i}. {pipeline.pipeline_no} ({len(pipeline.weld_points)} 个焊口)")
        
        # 默认测试前3条管线
        target_pipeline_nos = available_pipelines[:3]
        print(f"\n🎯 默认测试前3条管线: {target_pipeline_nos}")
    
    # 6. 处理每条目标管线
    all_topology_results = {}
    
    for target_pipeline_no in target_pipeline_nos:
        print(f"\n{'=' * 60}")
        print(f"🔍 处理管线: {target_pipeline_no}")
        print(f"{'=' * 60}")
        
        # 获取目标管线数据
        test_weld_points = []
        target_pipeline = None
        
        for pipeline in pipelines.pipelines:
            if pipeline.pipeline_no == target_pipeline_no:
                target_pipeline = pipeline
                test_weld_points = pipeline.weld_points
                break
        
        if not target_pipeline:
            print(f"❌ 未找到管线 {target_pipeline_no}")
            continue
        
        print(f"\n📋 管线数据统计:")
        print(f"   管线编号: {target_pipeline_no}")
        print(f"   焊口数: {len(test_weld_points)}")
        print(f"   单元名称: {target_pipeline.unit_name}")
        
        # 构建拓扑图
        print(f"\n🏗️ 构建拓扑图...")
        builder = PipelineTopologyBuilder()
        topology_results = builder.build_all_pipeline_topology(test_weld_points)
        
        # 分析结果
        if target_pipeline_no in topology_results:
            print(f"\n🎯 拓扑分析结果:")
            result = topology_results[target_pipeline_no]
            
            print(f"   主管线序列: {result['main_line']}")
            print(f"   支管线数量: {len(result['branch_pipelines'])}")
            
            for i, branch in enumerate(result['branch_pipelines']):
                print(f"   支管线{i+1}: {branch}")
            
            # 输出详细的焊口信息（前10个）
            print(f"\n📍 焊口详细信息 (前10个):")
            for wp in test_weld_points[:10]:
                print(f"   焊口{wp.weld_no}: 寸径{wp.diameter}, 坐标({wp.x:.1f}, {wp.y:.1f}, {wp.z:.1f})")
                print(f"     材料1: {wp.material_unique_code1} - {wp.material_description1}")
                print(f"     材料2: {wp.material_unique_code2} - {wp.material_description2}")
            
            if len(test_weld_points) > 10:
                print(f"   ... (共{len(test_weld_points)}个焊口)")
            
            # 保存到总结果中
            all_topology_results[target_pipeline_no] = topology_results[target_pipeline_no]
            
        else:
            print(f"❌ 未找到管线 {target_pipeline_no} 的拓扑结果")
    
    # 7. 保存结果到JSON文件
    if all_topology_results:
        print(f"\n💾 7. 保存结果...")
        output_file = "pipeline_topology_results.json"
        
        # 为JSON序列化准备数据
        json_results = {}
        for pipeline_no, result in all_topology_results.items():
            json_results[pipeline_no] = {
                "main_line": result["main_line"],
                "branch_pipelines": result["branch_pipelines"]
            }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_results, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 拓扑结果已保存到: {output_file}")
    
    # 8. 总体统计信息
    print(f"\n📊 8. 总体测试结果统计:")
    print(f"   成功处理管线数: {len(all_topology_results)}")
    print(f"   目标管线数: {len(target_pipeline_nos)}")
    
    for pipeline_no, result in all_topology_results.items():
        total_main_welds = len(result["main_line"])
        total_branch_welds = sum(len(branch) for branch in result["branch_pipelines"])
        total_branches = len(result["branch_pipelines"])
        
        print(f"\n   管线: {pipeline_no}")  
        print(f"     主管线焊口数: {total_main_welds}")
        print(f"     支管线数量: {total_branches}")
        print(f"     支管线焊口数: {total_branch_welds}")
        print(f"     总焊口数: {total_main_welds + total_branch_welds}")
    
    return all_topology_results

if __name__ == "__main__":
    test_pipeline_topology()
