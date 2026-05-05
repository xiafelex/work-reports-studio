"""
焊工模型
"""
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Worker:
    """
    焊工组模型
    """
    worker_id: int          # 焊工ID
    team_id: int            # 所属队伍ID
    available_time: float   # 最早可用时间（天）
    daily_capacity: float = 25  # 每天焊接寸数（默认25）
    
    def __repr__(self):
        return f"Worker(id={self.worker_id}, team={self.team_id}, available={self.available_time:.2f}, capacity={self.daily_capacity})"


class WorkerGroup:
    """
    焊工组管理器（支持动态队伍配置和单元限制）
    """
    def __init__(self, teams: Dict[int, int], team_units: Dict[int, Optional[List[str]]] = None, daily_capacity: float = 25):
        """
        初始化焊工组
        
        参数:
            teams: 队伍配置字典，格式 {队伍ID: 焊工组数}
                  例如: {1: 60, 2: 25, 3: 25}
            team_units: 队伍-单元限制，格式 {队伍ID: [单元名称列表]}
                       None表示该队伍可以焊接所有单元
                       例如: {1: ["丙交酯框架", "丙交酯车间"], 2: ["管廊"], 3: None}
            daily_capacity: 每组每天焊接寸数（默认25）
        """
        self.workers = []
        self.teams = teams
        self.team_units = team_units or {}
        self.daily_capacity = daily_capacity
        worker_id = 0
        
        # 按队伍ID排序后创建焊工
        for team_id in sorted(teams.keys()):
            count = teams[team_id]
            for _ in range(count):
                self.workers.append(Worker(worker_id, team_id, 0.0))
                worker_id += 1
        
        self.total_count = len(self.workers)
        self.team_count = len(teams)
    
    def can_team_work_on_unit(self, team_id: int, unit_name: Optional[str]) -> bool:
        """
        判断某个队伍是否可以焊接指定单元的管线
        
        参数:
            team_id: 队伍ID
            unit_name: 单元名称（None或'Unknown'表示没有单元）
        
        返回:
            True表示可以焊接，False表示不可以
        """
        # 如果单元名称为空或Unknown，允许所有队伍
        if not unit_name or unit_name == 'Unknown':
            return True
        
        # 如果该队伍没有配置单元限制，表示可以焊接所有单元
        if team_id not in self.team_units:
            return True
        
        allowed_units = self.team_units[team_id]
        
        # None表示可以焊接"所有未被其他队伍明确配置的单元"
        if allowed_units is None:
            # 检查是否有其他队伍明确配置了这个单元
            for other_team_id, other_units in self.team_units.items():
                if other_team_id == team_id:
                    continue
                if other_units is not None and unit_name in other_units:
                    # 这个单元被其他队伍明确配置了，当前队伍不能干
                    return False
            # 没有其他队伍配置这个单元，当前队伍可以干
            return True
        
        # 检查单元是否在允许列表中
        return unit_name in allowed_units
    
    def find_earliest_available(self, unit_name: Optional[str] = None) -> Worker:
        """
        找到最早可用的焊工（可以根据单元限制过滤）
        
        参数:
            unit_name: 单元名称，如果提供则只返回可以焊接该单元的焊工
        
        返回:
            最早可用的焊工对象
        """
        # 如果没有指定单元，或没有配置单元限制，返回所有焊工中最早的
        if not unit_name or not self.team_units:
            return min(self.workers, key=lambda w: w.available_time)
        
        # 过滤出可以焊接该单元的焊工
        eligible_workers = [
            w for w in self.workers 
            if self.can_team_work_on_unit(w.team_id, unit_name)
        ]
        
        if not eligible_workers:
            # 如果没有符合条件的焊工，返回所有焊工中最早的（降级处理）
            print(f"警告: 没有队伍可以焊接单元'{unit_name}'，将分配给最早可用的焊工")
            return min(self.workers, key=lambda w: w.available_time)
        
        return min(eligible_workers, key=lambda w: w.available_time)
    
    def reset(self):
        """重置所有焊工的可用时间为0"""
        for worker in self.workers:
            worker.available_time = 0.0
    
    def get_team_completion_time(self, team_id: int) -> float:
        """
        获取指定队伍的完工时间（该队最晚完成的焊工）
        
        参数:
            team_id: 队伍ID (1, 2, 3)
        
        返回:
            该队的完工时间
        """
        team_workers = [w for w in self.workers if w.team_id == team_id]
        if not team_workers:
            return 0.0
        return max(w.available_time for w in team_workers)
    
    def get_max_completion_time(self) -> float:
        """获取所有焊工的最大完工时间（总工期）"""
        return max(w.available_time for w in self.workers)
    
    def get_all_team_ids(self):
        """获取所有队伍ID"""
        return sorted(self.teams.keys())
    
    def get_worker_by_id(self, worker_id: int) -> Optional[Worker]:
        """
        根据焊工ID获取焊工对象
        
        参数:
            worker_id: 焊工ID
        
        返回:
            焊工对象，如果不存在则返回None
        """
        for worker in self.workers:
            if worker.worker_id == worker_id:
                return worker
        return None
    
    def __repr__(self):
        team_info = ", ".join([f"team{tid}={count}" for tid, count in sorted(self.teams.items())])
        return f"WorkerGroup(total={self.total_count}, {team_info})"

