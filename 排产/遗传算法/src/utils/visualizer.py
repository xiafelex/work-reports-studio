"""
可视化工具
"""
import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict, List
from ..models import Schedule

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False


def plot_convergence(history: Dict, save_path: str = None):
    """
    绘制收敛曲线
    
    参数:
        history: GA的历史记录，包含best_makespan, avg_fitness等
        save_path: 保存路径，None则显示
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # 图1: 最优工期变化
    generations = range(len(history['best_makespan']))
    ax1.plot(generations, history['best_makespan'], 'b-', linewidth=2, label='最优工期')
    ax1.set_xlabel('迭代代数', fontsize=12)
    ax1.set_ylabel('工期（天）', fontsize=12)
    ax1.set_title('最优工期收敛曲线', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # 图2: 适应度变化
    ax2.plot(generations, history['best_fitness'], 'r-', linewidth=2, label='最优适应度')
    ax2.plot(generations, history['avg_fitness'], 'g--', linewidth=1.5, label='平均适应度')
    ax2.set_xlabel('迭代代数', fontsize=12)
    ax2.set_ylabel('适应度', fontsize=12)
    ax2.set_title('适应度变化曲线', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"收敛曲线已保存到: {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_gantt_chart(schedule: Schedule, 
                     max_workers: int = 50,
                     save_path: str = None):
    """
    绘制甘特图（简化版，只显示部分焊工）
    
    参数:
        schedule: 调度方案
        max_workers: 最多显示的焊工数量（避免图表过大）
        save_path: 保存路径，None则显示
    """
    # 转换为DataFrame
    df = schedule.to_dataframe()
    
    # 只显示前max_workers个焊工
    top_workers = df['焊工ID'].value_counts().head(max_workers).index
    df_plot = df[df['焊工ID'].isin(top_workers)]
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(16, max(8, len(top_workers) * 0.3)))
    
    # 颜色映射（按队伍）
    colors = {1: 'royalblue', 2: 'orangered', 3: 'green'}
    
    # 绘制每个任务
    for _, task in df_plot.iterrows():
        worker_id = task['焊工ID']
        start = task['开始时间']
        duration = task['工期']
        team = task['队伍']
        pipeline = task['管线号']
        
        ax.barh(worker_id, duration, left=start, height=0.8,
                color=colors[team], alpha=0.7, edgecolor='black', linewidth=0.5)
        
        # 添加管线号标签（如果空间足够）
        if duration > 0.5:
            ax.text(start + duration/2, worker_id, str(pipeline)[:10], 
                   ha='center', va='center', fontsize=7, color='white', weight='bold')
    
    ax.set_xlabel('时间（天）', fontsize=12)
    ax.set_ylabel('焊工ID', fontsize=12)
    ax.set_title(f'焊接调度甘特图（显示前{len(top_workers)}个焊工）', fontsize=14, fontweight='bold')
    ax.grid(True, axis='x', alpha=0.3)
    
    # 添加图例
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=colors[1], alpha=0.7, label='一队'),
        Patch(facecolor=colors[2], alpha=0.7, label='二队'),
        Patch(facecolor=colors[3], alpha=0.7, label='三队')
    ]
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"甘特图已保存到: {save_path}")
    else:
        plt.show()
    
    plt.close()


def export_schedule(schedule: Schedule, 
                   output_path: str = "调度方案.xlsx"):
    """
    导出调度方案到Excel
    
    参数:
        schedule: 调度方案
        output_path: 输出文件路径
    """
    df = schedule.to_dataframe()
    
    # 按开始时间排序
    df = df.sort_values('开始时间')
    
    # 导出到Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 工作表1: 完整调度方案
        df.to_excel(writer, sheet_name='调度方案', index=False)
        
        # 工作表2: 统计信息
        stats = schedule.get_statistics()
        stats_df = pd.DataFrame([stats])
        stats_df.to_excel(writer, sheet_name='统计信息', index=False)
        
        # 工作表3: 按队伍分组
        team_summary = df.groupby('队伍').agg({
            '管线号': 'count',
            '管线寸径': 'sum',
            '结束时间': 'max'
        }).reset_index()
        team_summary.columns = ['队伍', '管线数量', '总寸径', '完工时间']
        team_summary.to_excel(writer, sheet_name='队伍统计', index=False)
    
    print(f"调度方案已导出到: {output_path}")

