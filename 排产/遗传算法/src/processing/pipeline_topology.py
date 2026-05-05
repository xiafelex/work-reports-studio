#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
管线拓扑图构建器
基于WeldPoint模型构建管线拓扑，支持：
1. 主管线和支管线分离
2. 基于Z坐标最低的端点起始的深度优先遍历
3. 支管线识别（管子类型材料，寸径较小的中间焊口）
4. 法兰特殊处理和材料连接逻辑
"""

import networkx as nx
import numpy as np
import math
from collections import defaultdict, deque
from typing import Dict, List, Set, Optional, Tuple, Any

from ..models.data_model import WeldPoint


class PipelineTopologyBuilder:
    """管线拓扑图构建器"""
    
    def __init__(self, silent_mode: bool = False):
        self.pipeline_graphs: Dict[str, nx.Graph] = {}
        self.pipeline_sequences: Dict[str, Dict[str, List[str]]] = {}  # {pipeline_no: {main_line: [...], branch_pipelines: [...]}}
        
        # 连接距离配置
        self.coordinate_tolerance = 100.0  # 同直线判断的坐标容差（毫米）
        self.min_connection_distance = 500.0  # 推断连接的最小距离（毫米）
        self.flange_connection_distance = 800.0  # 法兰连接距离限制（毫米）
        
        # 是否静默模式（用于大规模调度时减少打印）
        self.silent_mode = silent_mode
        
    def build_all_pipeline_topology(self, weld_points: List[WeldPoint]) -> Dict[str, Dict[str, List[str]]]:
        """构建所有管线的拓扑并获取焊接顺序
        
        Args:
            weld_points: 焊口列表
            
        Returns:
            Dict格式: {pipeline_no: {"main_line": [...], "branch_pipelines": [...]}}
        """
        # 按管线分组
        pipeline_weld_points = defaultdict(list)
        for wp in weld_points:
            pipeline_weld_points[wp.pipeline_no].append(wp)
        
        if not self.silent_mode:
            print(f"🏗️ 构建 {len(pipeline_weld_points)} 条管线的拓扑图...")
        
        # 为每条管线构建拓扑
        for pipeline_no, pipeline_welds in pipeline_weld_points.items():
            if not self.silent_mode:
                print(f"\n📊 处理管线 {pipeline_no} ({len(pipeline_welds)} 个焊口)")
            
            # 构建管线拓扑图
            graph = self._build_single_pipeline_graph(pipeline_welds)
            self.pipeline_graphs[pipeline_no] = graph
            
            # 识别支管线起点
            branch_starts = self._identify_branch_starts(graph, pipeline_welds)
            if not self.silent_mode:
                print(f"   🌿 识别到 {len(branch_starts)} 个支管起点: {branch_starts}")
            
            # 获取焊接顺序
            main_line, branch_pipelines = self._get_welding_sequences(graph, pipeline_welds, branch_starts)
            
            self.pipeline_sequences[pipeline_no] = {
                "main_line": main_line,
                "branch_pipelines": branch_pipelines
            }
            
            if not self.silent_mode:
                print(f"   ✓ 主管线: {len(main_line)} 个焊口")
                print(f"   ✓ 支管线: {len(branch_pipelines)} 条")
        
        return self.pipeline_sequences
    
    def _build_single_pipeline_graph(self, weld_points: List[WeldPoint]) -> nx.Graph:
        """为单条管线构建拓扑图"""
        graph = nx.Graph()
        material_connections = defaultdict(list)
        
        # 添加节点
        for wp in weld_points:
            graph.add_node(wp.weld_no, 
                          weld_point=wp,
                          diameter=wp.diameter,
                          coordinates=(wp.x, wp.y, wp.z) if wp.x is not None else None)
        
        # 收集材料连接关系
        for wp in weld_points:
            if wp.material_unique_code1:
                material_connections[wp.material_unique_code1].append(wp.weld_no)
            if wp.material_unique_code2:
                material_connections[wp.material_unique_code2].append(wp.weld_no)
        
        # 建立连接
        edge_count = 0
        for material_code, connected_welds in material_connections.items():
            if len(connected_welds) == 2:
                # 两个焊口共享材料：直接连接
                graph.add_edge(connected_welds[0], connected_welds[1], 
                             material_code=material_code,
                             connection_type='material')
                edge_count += 1
            elif len(connected_welds) >= 3:
                # 多个焊口共享材料：构建星形连接
                weld_dict = {wp.weld_no: wp for wp in weld_points}
                sorted_welds = sorted([weld_dict[w] for w in connected_welds if w in weld_dict], 
                                    key=lambda wp: (
                                        wp.z if wp.z is not None else 0,
                                        wp.y if wp.y is not None else 0, 
                                        wp.x if wp.x is not None else 0,
                                        int(wp.weld_no) if wp.weld_no.isdigit() else 9999
                                    ))
                
                if len(sorted_welds) >= 3:
                    # 两端焊口（管子主体）
                    first_weld = sorted_welds[0]
                    last_weld = sorted_welds[-1]
                    middle_welds = sorted_welds[1:-1]
                    
                    # 1. 两端焊口直接连接（管子主体连接）
                    graph.add_edge(first_weld.weld_no, last_weld.weld_no, 
                                 material_code=material_code,
                                 connection_type='material_main')
                    edge_count += 1
                    
                    # 2. 每个中间焊口连接到两端焊口（星形连接）
                    for middle_weld in middle_welds:
                        graph.add_edge(middle_weld.weld_no, first_weld.weld_no,
                                     material_code=material_code,
                                     connection_type='material_branch')
                        graph.add_edge(middle_weld.weld_no, last_weld.weld_no,
                                     material_code=material_code,
                                     connection_type='material_branch')
                        edge_count += 2
        
        # 处理法兰特殊连接
        flange_edges = self._add_flange_connections_weld_point(graph, weld_points)
        edge_count += flange_edges
        
        # 检查连通性，如果不连通则尝试修复
        connectivity_edges = self._fix_connectivity_by_line_proximity(graph, weld_points)
        edge_count += connectivity_edges
        
        print(f"     ✓ 创建 {len(weld_points)} 个节点, {edge_count} 条连接")
        return graph
    
    def _identify_branch_starts(self, graph: nx.Graph, weld_points: List[WeldPoint]) -> List[str]:
        """识别支管线起点焊口"""
        weld_dict = {wp.weld_no: wp for wp in weld_points}
        branch_starts = []
        processed_welds = set()  # 跟踪已处理的焊口，确保互斥
        
        # 规则1: 检查"对焊管接台"连接的焊口 + 寸径比例验证（优先级高）
        candidates = []
        for wp in weld_points:
            if ((wp.material_description1 and "对焊管接台" in wp.material_description1) or
                (wp.material_description2 and "对焊管接台" in wp.material_description2)):
                candidates.append(wp.weld_no)
        
        print(f"     🔍 对焊管接台候选焊口: {candidates}")
        
        # 对候选焊口进行寸径比例验证
        for candidate in candidates:
            candidate_weld = weld_dict[candidate]
            max_diameter_ratio = 1.0
            large_diameter_neighbor = None
            
            # 检查所有直接连接的邻居
            for neighbor in graph.neighbors(candidate):
                neighbor_weld = weld_dict.get(neighbor)
                if neighbor_weld and neighbor_weld.diameter > 0 and candidate_weld.diameter > 0:
                    # 计算比例（大径/小径）
                    ratio = max(neighbor_weld.diameter, candidate_weld.diameter) / min(neighbor_weld.diameter, candidate_weld.diameter)
                    if ratio > max_diameter_ratio:
                        max_diameter_ratio = ratio
                        large_diameter_neighbor = neighbor
            
            # 如果寸径比例>=2倍，认为是真正的支管起点
            if max_diameter_ratio >= 2.0:
                branch_starts.append(candidate)
                processed_welds.add(candidate)  # 标记已处理
                neighbor_weld = weld_dict[large_diameter_neighbor]
                print(f"     ✅ 支管起点: 焊口{candidate}({candidate_weld.diameter}寸) 连接焊口{large_diameter_neighbor}({neighbor_weld.diameter}寸), 比例{max_diameter_ratio:.1f} (对焊管接台+寸径验证)")
            else:
                print(f"     ❌ 排除候选: 焊口{candidate} 最大寸径比例仅{max_diameter_ratio:.1f} (对焊管接台但无大径邻居)")
        
        # 规则2: 收集所有管子类型的材料及其连接的焊口
        pipe_materials = defaultdict(list)
        
        for wp in weld_points:
            # 检查材料1是否是管子
            if wp.material_description1 and "管子" in wp.material_description1:
                pipe_materials[wp.material_unique_code1].append(wp)
            # 检查材料2是否是管子
            if wp.material_description2 and "管子" in wp.material_description2:
                pipe_materials[wp.material_unique_code2].append(wp)
        
        # 规则2: 分析每个管子材料（基于寸径比较）
        for material_code, connected_welds in pipe_materials.items():
            if len(connected_welds) >= 3:  # 至少3个焊口才可能有支管
                # 多级排序：Z坐标 -> Y坐标 -> X坐标 -> 焊口号，确保排序确定性
                sorted_welds = sorted(connected_welds, key=lambda wp: (
                    wp.z if wp.z is not None else 0,
                    wp.y if wp.y is not None else 0, 
                    wp.x if wp.x is not None else 0,
                    int(wp.weld_no) if wp.weld_no.isdigit() else 9999
                ))
                
                # 找到两端的大寸径焊口（管子两侧）
                if len(sorted_welds) >= 3:
                    first_weld = sorted_welds[0]
                    last_weld = sorted_welds[-1]
                    
                    # 假设两端焊口寸径相同且是最大的（管子特征）
                    end_diameter = max(first_weld.diameter, last_weld.diameter)
                    
                    # 查找中间所有寸径小于两端的焊口
                    for i in range(1, len(sorted_welds) - 1):  # 排除两端
                        current_weld = sorted_welds[i]
                        
                        # 如果当前焊口寸径明显小于两端大寸径，且未被规则1处理，则认为是支管起点
                        if (current_weld.diameter < end_diameter * 0.8 and  # 小于两端的80%
                            current_weld.weld_no not in processed_welds):  # 未被规则1处理
                            branch_starts.append(current_weld.weld_no)
                            processed_welds.add(current_weld.weld_no)  # 标记已处理
                            print(f"     🌿 发现支管起点: 焊口{current_weld.weld_no} (寸径{current_weld.diameter} < 两端寸径{end_diameter})")
        
        # 去重并返回
        unique_branch_starts = list(set(branch_starts))
        return unique_branch_starts
    
    def _get_welding_sequences(self, graph: nx.Graph, weld_points: List[WeldPoint], 
                              branch_starts: List[str]) -> Tuple[List[str], List[List[str]]]:
        """获取主管线和支管线的焊接顺序"""
        
        # 1. 从每个支管起点识别支管区域（避免重复）
        print(f"     🔍 识别支管区域...")
        all_branch_nodes = set()
        branch_regions = {}
        used_nodes = set()  # 跟踪已被分配的节点
        
        # 按支管起点编号排序，确保较小编号的起点优先处理
        sorted_branch_starts = sorted(branch_starts, key=lambda x: int(x) if x.isdigit() else float('inf'))
        
        for branch_start in sorted_branch_starts:
            if branch_start not in used_nodes:  # 只处理未被使用的起点
                branch_nodes = self._bfs_from_branch_start(graph, branch_start, weld_points, used_nodes)
                
                # 移除已被其他支管使用的节点
                unique_branch_nodes = [node for node in branch_nodes if node not in used_nodes]
                
                if len(unique_branch_nodes) >= 1:  # 只保留有效的支管
                    branch_regions[branch_start] = unique_branch_nodes
                    all_branch_nodes.update(unique_branch_nodes)
                    used_nodes.update(unique_branch_nodes)
                    print(f"       支管起点{branch_start}: {len(unique_branch_nodes)}个焊口 {unique_branch_nodes}")
                else:
                    print(f"       跳过支管起点{branch_start}: 节点已被其他支管占用")
        
        # 2. 提取主管线：全图节点 - 支管节点
        main_nodes = list(set(graph.nodes()) - all_branch_nodes)
        print(f"     🛤️ 主管区域: {len(main_nodes)}个焊口 {sorted(main_nodes, key=lambda x: int(x) if x.isdigit() else float('inf'))}")
        
        # 3. 主管线排序：从焊口1开始
        main_line = self._order_main_pipeline(graph, main_nodes)
        print(f"     📍 主管线序列: {main_line}")
        
        # 4. 提取支管线序列
        branch_pipelines = []
        for branch_start in sorted_branch_starts:
            if branch_start in branch_regions:  # 只处理有效的支管起点
                branch_sequence = branch_regions[branch_start]
                branch_pipelines.append(branch_sequence)
                print(f"     🌿 支管线序列 {len(branch_pipelines)}: {branch_sequence}")
        
        # 5. 完整性验证
        self._verify_completeness(main_line, branch_pipelines, weld_points)
        
        return main_line, branch_pipelines
    
    def _verify_completeness(self, main_line: List[str], branch_pipelines: List[List[str]], 
                            weld_points: List[WeldPoint]) -> bool:
        """验证焊口分配的完整性"""
        all_assigned = set(main_line)
        for branch in branch_pipelines:
            all_assigned.update(branch)
        
        original_set = {wp.weld_no for wp in weld_points}
        
        missing = original_set - all_assigned
        extra = all_assigned - original_set
        duplicates = len(main_line) + sum(len(branch) for branch in branch_pipelines) - len(all_assigned)
        
        if missing:
            print(f"     ⚠️ 遗漏焊口: {missing}")
        if extra:
            print(f"     ⚠️ 多余焊口: {extra}")
        if duplicates > 0:
            print(f"     ⚠️ 重复分配: {duplicates}个")
        
        total_assigned = len(main_line) + sum(len(branch) for branch in branch_pipelines)
        is_complete = (len(missing) == 0 and len(extra) == 0 and duplicates == 0 and 
                      total_assigned == len(weld_points))
        
        if is_complete:
            print(f"     ✅ 完整性验证通过: 主管{len(main_line)}个 + 支管{sum(len(branch) for branch in branch_pipelines)}个 = 总计{len(weld_points)}个")
        else:
            print(f"     ❌ 完整性验证失败")
        
        return is_complete
    
    def _bfs_from_branch_start(self, graph: nx.Graph, branch_start: str, weld_points: List[WeldPoint], 
                              excluded_nodes: Set[str] = None) -> List[str]:
        """从支管起点开始DFS，识别整个支管区域"""
        
        if excluded_nodes is None:
            excluded_nodes = set()
        
        # 创建焊口字典便于查找
        weld_dict = {wp.weld_no: wp for wp in weld_points}
        
        visited = set(excluded_nodes)  # 初始化时排除已被占用的节点
        branch_nodes = []
        
        def dfs(current):
            if current in visited:
                return
                
            visited.add(current)
            branch_nodes.append(current)
            
            # 获取当前焊口信息
            current_wp = weld_dict.get(current)
            if not current_wp:
                return
            
            # 获取相邻节点并排序（优先遍历编号较小的，保持有序）
            neighbors = []
            for neighbor in graph.neighbors(current):
                if neighbor in visited:
                    continue
                    
                neighbor_wp = weld_dict.get(neighbor)
                if not neighbor_wp:
                    continue
                
                # 判断是否继续扩展：
                # 1. 如果相邻焊口寸径是当前的2倍以上，可能是主管，停止
                # 2. 如果相邻焊口编号很小(1-20范围)且寸径较大，可能是主管
                diameter_ratio = neighbor_wp.diameter / current_wp.diameter if current_wp.diameter > 0 else 1
                
                is_main_pipe = False
                
                # 规则1：寸径比例判断
                if diameter_ratio >= 2.0:
                    is_main_pipe = True
                
                # 规则2：编号和寸径综合判断（焊口1-20且寸径>=3被认为是主管）
                if (neighbor.isdigit() and 1 <= int(neighbor) <= 20 and 
                    neighbor_wp.diameter >= 3.0):
                    is_main_pipe = True
                
                if not is_main_pipe:
                    neighbors.append(neighbor)
            
            # 对邻居节点排序：优先遍历路径延续性更好的节点
            neighbors.sort(key=lambda x: (
                # 优先选择编号相近的
                abs(int(x) - int(current)) if x.isdigit() and current.isdigit() else float('inf'),
                # 其次按坐标距离
                self._calculate_distance(weld_dict.get(current), weld_dict.get(x)) if weld_dict.get(x) else float('inf'),
                # 最后按编号
                int(x) if x.isdigit() else float('inf')
            ))
            
            # 递归遍历邻居节点
            for neighbor in neighbors:
                dfs(neighbor)
        
        dfs(branch_start)
        return branch_nodes
    
    def _calculate_distance(self, wp1: WeldPoint, wp2: WeldPoint) -> float:
        """计算两个焊口之间的距离"""
        if not wp1 or not wp2 or wp1.x is None or wp2.x is None:
            return float('inf')
        return math.sqrt((wp1.x - wp2.x)**2 + (wp1.y - wp2.y)**2 + (wp1.z - wp2.z)**2)
    
    def _order_main_pipeline(self, graph: nx.Graph, main_nodes: List[str]) -> List[str]:
        """对主管线焊口排序：默认从焊口1开始DFS遍历"""
        
        if not main_nodes:
            return []
        
        # 1. 确定起点
        if "1" in main_nodes:
            start_node = "1"
            print(f"       📍 主管线起点: 焊口1（默认起点）")
        else:
            # 兜底：选择编号最小的焊口
            main_weld_numbers = [int(node) for node in main_nodes if node.isdigit()]
            if main_weld_numbers:
                start_node = str(min(main_weld_numbers))
                print(f"       📍 主管线起点: 焊口{start_node}（焊口1不在主管中，选择最小编号）")
            else:
                start_node = main_nodes[0]
                print(f"       📍 主管线起点: 焊口{start_node}（兜底选择）")
        
        # 2. 从起点开始DFS遍历主管
        main_subgraph = graph.subgraph(main_nodes)
        return self._dfs_traverse_main_only(main_subgraph, start_node)
    
    def _dfs_traverse_main_only(self, main_subgraph: nx.Graph, start_node: str) -> List[str]:
        """在主管子图中DFS遍历"""
        
        visited = set()
        sequence = []
        
        def dfs(node):
            if node in visited:
                return
            
            visited.add(node)
            sequence.append(node)
            
            # 按编号顺序遍历相邻节点
            neighbors = list(main_subgraph.neighbors(node))
            # 尝试按数字大小排序
            try:
                neighbors.sort(key=lambda x: int(x) if x.isdigit() else float('inf'))
            except:
                pass
                
            for neighbor in neighbors:
                if neighbor not in visited:
                    dfs(neighbor)
        
        if start_node in main_subgraph:
            dfs(start_node)
        
        return sequence
    
    def _find_main_line_start(self, graph: nx.Graph, weld_points: List[WeldPoint], 
                             branch_starts: List[str]) -> str:
        """找到主管线起点（Z坐标最低的端点焊口）"""
        # 找到所有端点焊口（度数为1的节点）
        endpoints = []
        for weld_no in graph.nodes():
            if graph.degree(weld_no) == 1 and weld_no not in branch_starts:
                # 获取对应的焊口对象
                weld_point = next((wp for wp in weld_points if wp.weld_no == weld_no), None)
                if weld_point and weld_point.z is not None:
                    endpoints.append((weld_no, weld_point.z))
        
        if not endpoints:
            # 如果没有端点，选择Z坐标最低的非支管起点焊口
            valid_welds = [(wp.weld_no, wp.z) for wp in weld_points 
                          if wp.z is not None and wp.weld_no not in branch_starts]
            if valid_welds:
                endpoints = valid_welds
        
        # 选择Z坐标最低的作为起点
        if endpoints:
            return min(endpoints, key=lambda x: x[1])[0]
        
        # 兜底：返回第一个焊口
        return weld_points[0].weld_no if weld_points else ""
    
    def _dfs_traverse(self, graph: nx.Graph, start_node: str, 
                     exclude_starts: List[str] = None, exclude_nodes: Set[str] = None) -> List[str]:
        """深度优先遍历获取焊接序列"""
        if exclude_starts is None:
            exclude_starts = []
        if exclude_nodes is None:
            exclude_nodes = set()
        
        visited = set(exclude_nodes)  # 排除已访问的节点
        sequence = []
        
        def dfs(node):
            if node in visited or node in exclude_starts:
                return
            
            visited.add(node)
            sequence.append(node)
            
            # 遍历相邻节点
            for neighbor in graph.neighbors(node):
                if neighbor not in visited:
                    dfs(neighbor)
        
        if start_node not in visited:
            dfs(start_node)
        
        return sequence
    
    def _add_flange_connections_weld_point(self, graph: nx.Graph, weld_points: List[WeldPoint]) -> int:
        """为WeldPoint模型添加法兰特殊连接"""
        flange_at_material1 = []  # 材料1是法兰的焊口
        flange_at_material2 = []  # 材料2是法兰的焊口
        
        # 分类法兰焊口
        for wp in weld_points:
            if self._is_flange_material(wp.material_description1):
                flange_at_material1.append(wp)
            if self._is_flange_material(wp.material_description2):
                flange_at_material2.append(wp)
        
        if not flange_at_material1 and not flange_at_material2:
            return 0
        
        print(f"     🔍 法兰识别: 材料1法兰{len(flange_at_material1)}个, 材料2法兰{len(flange_at_material2)}个")
        
        # 收集所有可能的法兰配对（距离<800mm，不在同一连通分量）
        possible_pairs = []
        all_flanges = flange_at_material1 + flange_at_material2
        
        # 检查所有法兰对法兰的连接
        for i, wp1 in enumerate(all_flanges):
            if wp1.x is None or wp1.y is None or wp1.z is None:
                continue
            coord1 = (wp1.x, wp1.y, wp1.z)
                
            for j, wp2 in enumerate(all_flanges[i+1:], i+1):
                if wp2.x is None or wp2.y is None or wp2.z is None:
                    continue
                coord2 = (wp2.x, wp2.y, wp2.z)
                
                # 计算距离
                distance = self._calculate_3d_distance(coord1, coord2)
                if distance <= self.flange_connection_distance:
                    # 检查是否在同一连通分量
                    try:
                        if not nx.has_path(graph, wp1.weld_no, wp2.weld_no):
                            possible_pairs.append((wp1, wp2, distance))
                    except nx.NetworkXNoPath:
                        possible_pairs.append((wp1, wp2, distance))
        
        # 按距离排序（从近到远）
        possible_pairs.sort(key=lambda x: x[2])
        
        # 贪婪匹配，确保每个法兰只被连接一次
        used_flanges = set()
        connections_added = 0
        
        for wp1, wp2, distance in possible_pairs:
            # 检查两个法兰是否都还没被连接
            if wp1.weld_no not in used_flanges and wp2.weld_no not in used_flanges:
                # 添加连接
                graph.add_edge(wp1.weld_no, wp2.weld_no, 
                             material_code=f"法兰连接_{distance:.0f}mm",
                             connection_type="flange",
                             distance=distance)
                connections_added += 1
                
                # 标记这两个法兰已被使用
                used_flanges.add(wp1.weld_no)
                used_flanges.add(wp2.weld_no)
                
                # 判断法兰类型
                wp1_type = "材料1法兰" if wp1 in flange_at_material1 else "材料2法兰"
                wp2_type = "材料1法兰" if wp2 in flange_at_material1 else "材料2法兰"
        
        if len(possible_pairs) > connections_added:
            skipped = len(possible_pairs) - connections_added
            print(f"       ℹ️  跳过{skipped}个配对（法兰已被其他更近的法兰连接）")
        
        return connections_added
    
    def _is_flange_material(self, material_description: str) -> bool:
        """判断材料是否是法兰：材料描述中包含"法兰"""
        if not material_description:
            return False
        material_desc_str = str(material_description).strip()
        return "法兰" in material_desc_str
    
    def _calculate_3d_distance(self, coord1: Tuple[float, float, float], coord2: Tuple[float, float, float]) -> float:
        """计算两点间的3D距离"""
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(coord1, coord2)))
    
    def _fix_connectivity_by_line_proximity(self, graph: nx.Graph, weld_points: List[WeldPoint]) -> int:
        """通过同一直线上的邻近焊口连接来修复连通性"""
        
        # 检查连通分量数
        components = list(nx.connected_components(graph))
        if len(components) <= 1:
            return 0  # 已经连通
        
        print(f"     🔧 检测到{len(components)}个连通分量，尝试修复连通性...")
        
        connections_added = 0
        weld_dict = {wp.weld_no: wp for wp in weld_points}
        
        # 重复连接最优焊口对，直到图连通
        while len(list(nx.connected_components(graph))) > 1:
            components = list(nx.connected_components(graph))
            best_pair = None
            best_score = float('inf')
            
            # 寻找不同连通分量间的最优连接
            for i, comp1 in enumerate(components):
                for j, comp2 in enumerate(components[i+1:], i+1):
                    # 检查分量间的所有焊口对
                    for weld1_no in comp1:
                        for weld2_no in comp2:
                            weld1 = weld_dict.get(weld1_no)
                            weld2 = weld_dict.get(weld2_no)
                            
                            if not weld1 or not weld2 or weld1.x is None or weld2.x is None:
                                continue
                            
                            # 计算加权积分得分（越小越好）
                            score = self._calculate_connection_score(graph, weld1, weld2)
                            
                            if score < best_score:
                                best_score = score
                                best_pair = (weld1, weld2, score)
            
            # 添加最优连接
            if best_pair:
                weld1, weld2, score = best_pair
                graph.add_edge(weld1.weld_no, weld2.weld_no,
                             material_code=f"连通性修复_{score:.2f}",
                             connection_type="connectivity_fix")
                connections_added += 1
                
                print(f"       🔗 连通性修复: 焊口{weld1.weld_no} ←→ 焊口{weld2.weld_no} (得分: {score:.2f})")
            else:
                # 无法找到连接，退出
                break
        
        final_components = len(list(nx.connected_components(graph)))
        if final_components == 1:
            print(f"       ✅ 连通性修复成功！添加了{connections_added}条连接")
        else:
            print(f"       ⚠️  仍有{final_components}个连通分量，无法完全修复")
        
        return connections_added
    
    def _calculate_connection_score(self, graph: nx.Graph, weld1: WeldPoint, weld2: WeldPoint) -> float:
        """计算两个焊口连接的加权积分得分（越小越优）"""
        
        # 基础距离得分
        distance = self._calculate_3d_distance(
            (weld1.x, weld1.y, weld1.z),
            (weld2.x, weld2.y, weld2.z)
        )
        distance_score = distance / 1000.0  # 归一化到合理范围
        
        # 度数得分（度数小的权重大，即得分小）
        degree1 = graph.degree(weld1.weld_no)
        degree2 = graph.degree(weld2.weld_no)
        degree_score = (degree1 + degree2) / 10.0  # 归一化
        
        # 同一直线得分（xyz中有2个坐标相同）
        line_score = 1.0  # 默认不在同一直线
        coord_matches = 0
        tolerance = 1.0  # 1mm容差
        
        if abs(weld1.x - weld2.x) <= tolerance:
            coord_matches += 1
        if abs(weld1.y - weld2.y) <= tolerance:
            coord_matches += 1  
        if abs(weld1.z - weld2.z) <= tolerance:
            coord_matches += 1
            
        if coord_matches >= 2:
            line_score = 0.1  # 在同一直线上，得分很低
        elif coord_matches == 1:
            line_score = 0.3  # 在同一平面上，得分较低
        
        # 焊口号连续得分
        weld_no_score = 1.0  # 默认不连续
        if weld1.weld_no.isdigit() and weld2.weld_no.isdigit():
            weld_diff = abs(int(weld1.weld_no) - int(weld2.weld_no))
            if weld_diff == 1:
                weld_no_score = 0.2  # 连续焊口号，得分低
            elif weld_diff <= 3:
                weld_no_score = 0.5  # 相近焊口号，得分中等
        
        # 加权积分（权重系数根据重要性调整）
        total_score = (
            distance_score * 1.0 +      # 距离权重：最高
            degree_score * 1.0 +        # 度数权重：最高（和距离同级）  
            line_score * 0.8 +          # 同一直线权重：高
            weld_no_score * 0.5         # 焊口号连续权重：中
        )
        
        return total_score
    