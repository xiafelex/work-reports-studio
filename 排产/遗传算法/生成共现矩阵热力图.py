"""
单元-天数共现矩阵热力图生成器

功能：读取调度方案Excel，生成单元-天数热力图
- 横坐标：天数（第1天、第2天...）
- 纵坐标：单元名称
- 单元格值：该天该单元有多少焊工在工作
- 颜色：焊工数越多，颜色越深

使用方法：
    python 生成共现矩阵热力图.py [调度方案文件路径]
    
    如果不提供文件路径，默认读取 "output_files/最优调度方案_汇总.xlsx"
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from math import ceil, floor

# 输出文件夹
OUTPUT_DIR = "output_files"


def load_schedule(file_path: str) -> pd.DataFrame:
    """
    加载调度方案Excel文件
    
    参数:
        file_path: Excel文件路径
    
    返回:
        DataFrame，包含调度方案数据
    """
    if not Path(file_path).exists():
        raise FileNotFoundError(f"找不到文件: {file_path}")
    
    df = pd.read_excel(file_path, sheet_name='调度方案')
    
    # 检查必需的列
    required_columns = ['管线号', '焊工ID', '开始时间', '结束时间', '单元名称']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Excel文件缺少以下列: {missing_columns}")
    
    print(f"成功加载调度方案，共 {len(df)} 条任务记录")
    return df


def calculate_unit_day_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算单元-天数共现矩阵
    
    逻辑：
    - 对每个任务，计算它覆盖的天数范围（向下取整开始，向上取整结束）
    - 只要任务时间与某天有重叠，就算该焊工在该天该单元工作
    - 同一个焊工在同一天可以被多个单元统计（重复计数）
    
    参数:
        df: 调度方案DataFrame
    
    返回:
        单元-天数矩阵DataFrame（行=单元，列=天数）
    """
    # 计算最大天数（向上取整）
    max_day = int(ceil(df['结束时间'].max()))
    
    # 初始化矩阵：{(unit, day): worker_set}
    unit_day_workers = {}
    
    for _, task in df.iterrows():
        unit_name = task['单元名称']
        worker_id = task['焊工ID']
        start_time = task['开始时间']
        end_time = task['结束时间']
        
        # 计算任务覆盖的天数范围
        # 如果任务在 5.3-6.2 天，应该覆盖第6天和第7天
        start_day = int(floor(start_time)) + 1  # 第几天（从1开始）
        end_day = int(ceil(end_time))
        
        # 对于每一天，记录该焊工在该单元工作
        for day in range(start_day, end_day + 1):
            if day > max_day:
                break
            key = (unit_name, day)
            if key not in unit_day_workers:
                unit_day_workers[key] = set()
            unit_day_workers[key].add(worker_id)
    
    # 转换为矩阵
    units = sorted(df['单元名称'].unique())
    days = list(range(1, max_day + 1))
    
    matrix = []
    for unit in units:
        row = []
        for day in days:
            key = (unit, day)
            worker_count = len(unit_day_workers.get(key, set()))
            row.append(worker_count)
        matrix.append(row)
    
    # 创建DataFrame
    matrix_df = pd.DataFrame(matrix, index=units, columns=[f'第{d}天' for d in days])
    
    print(f"\n共现矩阵统计:")
    print(f"  单元数量: {len(units)}")
    print(f"  天数范围: 1-{max_day} 天")
    print(f"  最大焊工数: {matrix_df.max().max()}")
    
    return matrix_df


def plot_heatmap(matrix_df: pd.DataFrame, output_path: str = "单元-天数热力图.png"):
    """
    绘制热力图
    
    参数:
        matrix_df: 单元-天数矩阵
        output_path: 输出图片路径
    """
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STSong']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 计算图片大小（根据数据量自适应）
    n_units = len(matrix_df)
    n_days = len(matrix_df.columns)
    
    # 每个单元至少0.4英寸，每天至少0.3英寸
    fig_height = max(8, n_units * 0.4)
    fig_width = max(12, n_days * 0.3)
    
    # 创建图表
    plt.figure(figsize=(fig_width, fig_height))
    
    # 绘制热力图
    ax = sns.heatmap(
        matrix_df, 
        cmap='YlOrRd',  # 黄-橙-红配色
        annot=True,     # 显示数值
        fmt='g',        # 数值格式
        cbar_kws={'label': '焊工数量'},
        linewidths=0.5,
        linecolor='gray'
    )
    
    # 设置标题和标签
    plt.title('单元-天数焊工分布热力图', fontsize=16, pad=20)
    plt.xlabel('天数', fontsize=12)
    plt.ylabel('单元名称', fontsize=12)
    
    # 旋转x轴标签
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n热力图已保存到: {output_path}")
    
    # 显示图片（可选）
    # plt.show()
    plt.close()


def main():
    """
    主函数
    """
    # 获取文件路径（从命令行参数或使用默认值）
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # 默认从 output_files 文件夹读取
        file_path = os.path.join(OUTPUT_DIR, "最优调度方案_汇总.xlsx")
    
    print("=" * 60)
    print("单元-天数共现矩阵热力图生成器")
    print("=" * 60)
    
    try:
        # 1. 加载调度方案
        print(f"\n[步骤1] 加载调度方案: {file_path}")
        df = load_schedule(file_path)
        
        # 2. 计算共现矩阵
        print("\n[步骤2] 计算单元-天数共现矩阵...")
        matrix_df = calculate_unit_day_matrix(df)
        
        # 3. 绘制热力图（保存到 output_files 文件夹）
        print("\n[步骤3] 生成热力图...")
        output_path = os.path.join(OUTPUT_DIR, "单元-天数热力图.png")
        plot_heatmap(matrix_df, output_path)
        
        # 4. 导出矩阵数据（保存到 output_files 文件夹）
        matrix_output = os.path.join(OUTPUT_DIR, "单元-天数矩阵.xlsx")
        matrix_df.to_excel(matrix_output)
        print(f"矩阵数据已导出到: {matrix_output}")
        
        print("\n" + "=" * 60)
        print("处理完成！")
        print("=" * 60)
        print(f"\n生成的文件：")
        print(f"  - {output_path}")
        print(f"  - {matrix_output}")
        
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

