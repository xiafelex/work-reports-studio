"""
调度方案模型
"""
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd


@dataclass
class Task:
    """
    单个任务（管线的焊接任务）
    """
    pipeline_no: str        # 管线号（原始管线号，非segment_id）
    worker_id: int          # 负责的焊工ID
    team_id: int            # 焊工所属队伍
    start_time: float       # 开始时间（天）
    end_time: float         # 结束时间（天）
    total_inches: float     # 管线总寸径
    package_no: str         # 所属试压包号
    unit_name: str = None   # 单元名称
    zone_name: str = None   # 工区名称（仅空间约束模式使用）
    
    def __repr__(self):
        return f"Task({self.pipeline_no}, worker={self.worker_id}, " \
               f"time=[{self.start_time:.2f}, {self.end_time:.2f}])"


class Schedule:
    """
    完整的调度方案
    """
    def __init__(self):
        self.tasks: List[Task] = []
    
    def add_task(self, task: Task):
        """添加任务"""
        self.tasks.append(task)
    
    def get_makespan(self) -> float:
        """
        获取总工期（makespan）
        
        返回:
            最晚完成时间
        """
        if not self.tasks:
            return 0.0
        return max(task.end_time for task in self.tasks)
    
    def get_pipeline_completion_time(self, pipeline_no: str) -> float:
        """
        获取指定管线的完成时间
        
        参数:
            pipeline_no: 管线号
        
        返回:
            该管线的完成时间
        """
        for task in self.tasks:
            if task.pipeline_no == pipeline_no:
                return task.end_time
        return 0.0
    
    def get_package_completion_times(self, package_no: str) -> List[float]:
        """
        获取指定试压包内所有管线的完成时间
        
        参数:
            package_no: 试压包号
        
        返回:
            完成时间列表
        """
        return [task.end_time for task in self.tasks if task.package_no == package_no]
    
    def get_team_completion_time(self, team_id: int) -> float:
        """
        获取指定队伍的完工时间
        
        参数:
            team_id: 队伍ID (1, 2, 3)
        
        返回:
            该队的完工时间
        """
        team_tasks = [task for task in self.tasks if task.team_id == team_id]
        if not team_tasks:
            return 0.0
        return max(task.end_time for task in team_tasks)
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        转换为DataFrame以便分析和导出
        
        返回:
            包含所有任务信息的DataFrame
        """
        data = []
        for task in self.tasks:
            row = {
                '管线号': task.pipeline_no,
                '焊工ID': task.worker_id,
                '队伍': task.team_id,
                '开始时间': round(task.start_time, 2),
                '结束时间': round(task.end_time, 2),
                '工期': round(task.end_time - task.start_time, 2),
                '管线寸径': task.total_inches,
                '试压包号': task.package_no,
                '单元名称': task.unit_name if task.unit_name else 'Unknown'
            }
            
            # 空间约束模式：在单元名称后添加工区列
            if task.zone_name:
                row['工区'] = task.zone_name
            
            data.append(row)
        return pd.DataFrame(data)
    
    def get_statistics(self, team_name_map: Dict[int, str] = None) -> Dict:
        """
        获取调度方案的统计信息
        
        参数:
            team_name_map: 队伍ID到名称的映射，例如 {1: '一队', 2: '二队'}
                          如果不提供，则使用默认格式 "队伍X"
        
        返回:
            统计信息字典
        """
        if not self.tasks:
            return {}
        
        makespan = self.get_makespan()
        
        # 从任务中提取所有队伍ID
        team_ids = set(task.team_id for task in self.tasks)
        
        # 动态计算各队完工时间
        stats = {'总工期': round(makespan, 2)}
        
        team_times = []
        
        for team_id in sorted(team_ids):
            team_time = self.get_team_completion_time(team_id)
            team_times.append(team_time)
            
            # 使用传入的名称映射，或默认格式
            if team_name_map and team_id in team_name_map:
                team_name = team_name_map[team_id]
            else:
                team_name = f'队伍{team_id}'
            
            stats[f'{team_name}完工时间'] = round(team_time, 2)
        
        # 负载不均衡度
        if team_times:
            stats['负载不均衡度'] = round(max(team_times) - min(team_times), 2)
        else:
            stats['负载不均衡度'] = 0.0
        
        stats['管线总数'] = len(self.tasks)
        stats['总寸径'] = sum(task.total_inches for task in self.tasks)
        
        return stats
    
    def __len__(self):
        return len(self.tasks)
    
    def __repr__(self):
        return f"Schedule(tasks={len(self.tasks)}, makespan={self.get_makespan():.2f})"

