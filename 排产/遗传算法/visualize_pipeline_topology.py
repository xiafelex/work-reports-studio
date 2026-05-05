#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
管线拓扑3D可视化工具
生成交互式3D模型来检查拓扑连接是否正确
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_io import read_weld_info, read_pressure_package, read_pipe_property
from src.processing import initialize_weld_data, aggregate_pipeline_inches, merge_package_data
from src.processing.pipeline_topology import PipelineTopologyBuilder
import plotly.graph_objects as go
import plotly.offline as pyo
import networkx as nx
import json


def visualize_pipeline_topology_3d(target_pipeline_nos=None):
    """生成多条管线拓扑的3D可交互模型
    
    Args:
        target_pipeline_nos: 目标管线编号列表。如果为None，则显示所有管线选择菜单
    """
    
    # 1. 读取和处理数据
    print("📂 读取数据...")
    weld_data = read_weld_info()
    package_data = read_pressure_package()
    pipe_property_data = read_pipe_property()
    
    weld_points = initialize_weld_data(weld_data, pipe_property_data)
    pipelines = aggregate_pipeline_inches(weld_points)
    pipelines = merge_package_data(pipelines, package_data)
    
    # 2. 如果没有指定管线，显示所有可用管线让用户选择
    if target_pipeline_nos is None:
        print(f"\n📋 可用管线列表:")
        available_pipelines = []
        for i, pipeline in enumerate(pipelines.pipelines, 1):
            if len(pipeline.weld_points) > 0:  # 只显示有焊口的管线
                available_pipelines.append(pipeline.pipeline_no)
                print(f"   {i}. {pipeline.pipeline_no} ({len(pipeline.weld_points)} 个焊口)")
        
        # 默认测试前3条管线
        target_pipeline_nos = available_pipelines[:3]
        print(f"\n🎯 默认测试前3条管线: {target_pipeline_nos}")
    
    # 3. 处理每条目标管线
    for target_pipeline_no in target_pipeline_nos:
        print(f"\n🎨 生成管线 {target_pipeline_no} 的3D拓扑可视化...")
        
        # 获取目标管线数据
        target_pipeline = None
        test_weld_points = []
        
        for pipeline in pipelines.pipelines:
            if pipeline.pipeline_no == target_pipeline_no:
                target_pipeline = pipeline
                test_weld_points = pipeline.weld_points
                break
        
        if not target_pipeline:
            print(f"❌ 未找到管线 {target_pipeline_no}")
            continue
        
        print(f"✅ 找到管线: {len(test_weld_points)} 个焊口")
    
        # 构建拓扑
        builder = PipelineTopologyBuilder()
        
        # 获取拓扑图和结果
        graph = builder._build_single_pipeline_graph(test_weld_points)
        branch_starts = builder._identify_branch_starts(graph, test_weld_points)
        main_line, branch_pipelines = builder._get_welding_sequences(graph, test_weld_points, branch_starts)
    
        print(f"📊 拓扑信息:")
        print(f"   图节点数: {graph.number_of_nodes()}")
        print(f"   图边数: {graph.number_of_edges()}")
        print(f"   支管起点: {branch_starts}")
        print(f"   主管线: {main_line}")
        print(f"   支管线数: {len(branch_pipelines)}")
    
        # 4. 创建3D可视化
        fig = go.Figure()
        
        # 准备焊口坐标数据
        weld_dict = {wp.weld_no: wp for wp in test_weld_points}
    
        # 添加所有焊口节点
        all_welds = []
        x_coords, y_coords, z_coords = [], [], []
        labels = []
        colors = []
        hover_texts = []
        
        for wp in test_weld_points:
            if wp.x is not None and wp.y is not None and wp.z is not None:
                all_welds.append(wp.weld_no)
                x_coords.append(wp.x)
                y_coords.append(wp.y)
                z_coords.append(wp.z)
                labels.append(wp.weld_no)
                
                # 确定焊口类型和颜色
                if wp.weld_no in main_line:
                    colors.append('blue')
                    node_type = "主管线"
                elif wp.weld_no in branch_starts:
                    colors.append('red')
                    node_type = "支管起点"
                elif any(wp.weld_no in branch for branch in branch_pipelines):
                    colors.append('green')
                    node_type = "支管线"
                else:
                    colors.append('gray')
                    node_type = "未分类"
                
                hover_texts.append(
                    f"焊口: {wp.weld_no}<br>"
                    f"类型: {node_type}<br>"
                    f"寸径: {wp.diameter}<br>"
                    f"坐标: ({wp.x:.1f}, {wp.y:.1f}, {wp.z:.1f})<br>"
                    f"材料1: {wp.material_unique_code1}<br>"
                    f"材料2: {wp.material_unique_code2}"
                )
    
        # 添加焊口节点
        fig.add_trace(go.Scatter3d(
            x=x_coords,
            y=y_coords,
            z=z_coords,
            mode='markers+text',
            marker=dict(
                size=8,
                color=colors,
                opacity=0.8,
                line=dict(color='black', width=1)
            ),
            text=labels,
            textposition='top center',
            textfont=dict(size=10, color='black'),
            hovertext=hover_texts,
            hoverinfo='text',
            name='焊口节点',
            showlegend=False
        ))
    
        # 添加拓扑图的连接线
        for edge in graph.edges():
            weld1, weld2 = edge
            if weld1 in weld_dict and weld2 in weld_dict:
                wp1, wp2 = weld_dict[weld1], weld_dict[weld2]
                if (wp1.x is not None and wp1.y is not None and wp1.z is not None and
                    wp2.x is not None and wp2.y is not None and wp2.z is not None):
                    
                    # 获取连接信息
                    edge_data = graph.edges[edge]
                    material_code = edge_data.get('material_code', '未知')
                    connection_type = edge_data.get('connection_type', 'material')
                    
                    # 根据连接类型设置颜色
                    if connection_type == 'flange':
                        line_color = 'orange'
                        line_width = 4
                    elif '法兰' in material_code:
                        line_color = 'orange'
                        line_width = 4
                    else:
                        line_color = 'gray'
                        line_width = 2
                    
                    fig.add_trace(go.Scatter3d(
                        x=[wp1.x, wp2.x],
                        y=[wp1.y, wp2.y],
                        z=[wp1.z, wp2.z],
                        mode='lines',
                        line=dict(color=line_color, width=line_width),
                        hovertext=f"连接: {weld1} ←→ {weld2}<br>材料: {material_code}",
                        hoverinfo='text',
                        showlegend=False
                    ))
    
        # 添加图例
        legend_data = [
            ('主管线', 'blue'),
            ('支管起点', 'red'), 
            ('支管线', 'green'),
            ('未分类', 'gray')
        ]
        
        for name, color in legend_data:
            fig.add_trace(go.Scatter3d(
                x=[None], y=[None], z=[None],
                mode='markers',
                marker=dict(size=8, color=color),
                name=name,
                showlegend=True
            ))
        
        # 设置布局
        fig.update_layout(
            title=dict(
                text=f'管线 {target_pipeline_no} 拓扑3D可视化<br>'
                     f'<sub>总焊口: {len(test_weld_points)}个 | 主管线: {len(main_line)}个 | 支管线: {len(branch_pipelines)}条</sub>',
                x=0.5,
                font=dict(size=16)
            ),
            scene=dict(
                xaxis_title='X坐标 (mm)',
                yaxis_title='Y坐标 (mm)',
                zaxis_title='Z坐标 (mm)',
                camera=dict(
                    eye=dict(x=1.5, y=1.5, z=1.5)
                ),
                aspectmode='auto'
            ),
            legend=dict(x=0.02, y=0.98),
            width=1200,
            height=800
        )
        
        # 保存HTML文件（清理文件名中的非法字符）
        safe_filename = target_pipeline_no.replace('/', '_').replace('\\', '_').replace('(', '_').replace(')', '_').replace(':', '_').replace('<', '_').replace('>', '_').replace('"', '_').replace('|', '_').replace('?', '_').replace('*', '_')
        output_file = f"{safe_filename}_topology_3d.html"
        fig.write_html(output_file)
        
        print(f"✅ 3D可视化已保存到: {output_file}")
        
        # 显示在浏览器中
        fig.show()
        
        # 输出详细的连接分析
        print(f"\n🔍 连接分析:")
        print(f"   连通分量数: {nx.number_connected_components(graph)}")
        
        # 分析每个焊口的度数
        degree_analysis = {}
        for node in graph.nodes():
            degree = graph.degree(node)
            if degree not in degree_analysis:
                degree_analysis[degree] = []
            degree_analysis[degree].append(node)
        
        print(f"   度数分析:")
        for degree in sorted(degree_analysis.keys()):
            nodes = degree_analysis[degree]
            print(f"     度数{degree}: {len(nodes)}个节点 {nodes[:5]}{'...' if len(nodes) > 5 else ''}")
        
        # 分析法兰焊口
        print(f"\n🔍 法兰分析:")
        flange1_count = 0
        flange2_count = 0
        for wp in test_weld_points:
            if wp.material_description1 and "法兰" in wp.material_description1:
                flange1_count += 1
                print(f"   材料1法兰: 焊口{wp.weld_no} - {wp.material_description1}")
            if wp.material_description2 and "法兰" in wp.material_description2:
                flange2_count += 1
                print(f"   材料2法兰: 焊口{wp.weld_no} - {wp.material_description2}")
        
        print(f"   法兰统计: 材料1法兰{flange1_count}个, 材料2法兰{flange2_count}个")


if __name__ == "__main__":
    visualize_pipeline_topology_3d(["6\"-CHWS-AHA-C70-58000004A"])
