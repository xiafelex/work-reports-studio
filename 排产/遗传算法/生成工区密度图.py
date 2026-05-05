"""
生成工区-天数焊工密度热力图
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from math import ceil

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STSong']
plt.rcParams['axes.unicode_minus'] = False

def generate_zone_density_heatmap(excel_path: str, output_path: str = 'output_files/工区密度热力图.png'):
    """
    生成工区-天数焊工密度热力图
    
    Args:
        excel_path: 调度方案Excel文件路径
        output_path: 输出图片路径
    """
    # 读取Excel数据
    print("正在读取调度方案...")
    df = pd.read_excel(excel_path)
    
    # 检查必要列
    required_cols = ['工区', '开始时间', '结束时间', '焊工ID']
    for col in required_cols:
        if col not in df.columns:
            print(f"错误：Excel文件缺少'{col}'列")
            return
    
    # 过滤掉工区为空的记录
    df = df[df['工区'].notna()].copy()
    
    if len(df) == 0:
        print("错误：没有找到有效的工区数据")
        return
    
    # 获取最大天数
    max_day = int(ceil(df['结束时间'].max()))
    
    # 初始化密度矩阵
    zones = sorted(df['工区'].unique())
    density_matrix = np.zeros((len(zones), max_day))
    zone_to_idx = {zone: idx for idx, zone in enumerate(zones)}
    
    print(f"统计工区密度...")
    print(f"  工区数: {len(zones)}")
    print(f"  最大天数: {max_day}")
    print(f"  任务数: {len(df)}")
    
    # 统计每天每个工区的焊工数
    for _, row in df.iterrows():
        zone = row['工区']
        start_time = row['开始时间']
        end_time = row['结束时间']
        
        # 计算任务跨越的天数
        start_day = int(start_time)
        end_day = int(ceil(end_time))
        
        # 为每一天累加焊工数
        for day in range(start_day, end_day):
            if day < max_day:
                zone_idx = zone_to_idx[zone]
                density_matrix[zone_idx, day] += 1
    
    # 过滤掉全为0的工区（没有任务的工区）
    non_zero_zones = []
    non_zero_matrix = []
    for idx, zone in enumerate(zones):
        if density_matrix[idx].sum() > 0:
            non_zero_zones.append(zone)
            non_zero_matrix.append(density_matrix[idx])
    
    if len(non_zero_zones) == 0:
        print("错误：所有工区密度都为0")
        return
    
    density_matrix = np.array(non_zero_matrix)
    zones = non_zero_zones
    
    print(f"  有效工区数: {len(zones)}")
    print(f"  最大密度: {int(density_matrix.max())}人")
    
    # 创建热力图
    print(f"绘制热力图...")
    
    # 根据工区数量调整图片大小
    fig_height = max(8, len(zones) * 0.4)
    fig_width = max(12, max_day * 0.15)
    
    plt.figure(figsize=(fig_width, fig_height))
    
    # 绘制热力图
    ax = sns.heatmap(
        density_matrix,
        xticklabels=range(max_day),
        yticklabels=zones,
        cmap='YlOrRd',
        cbar_kws={'label': '焊工数量'},
        linewidths=0.5,
        linecolor='white',
        fmt='g',
        annot=density_matrix if len(zones) <= 20 and max_day <= 50 else False,  # 工区或天数太多时不显示数字
    )
    
    plt.title(f'工区焊工密度热力图（共{len(zones)}个工区，{max_day}天）', fontsize=16, pad=20)
    plt.xlabel('天数', fontsize=12)
    plt.ylabel('工区名称', fontsize=12)
    
    # 旋转y轴标签以便阅读
    plt.yticks(rotation=0, fontsize=9)
    plt.xticks(rotation=45, fontsize=9)
    
    plt.tight_layout()
    
    # 保存图片
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n✅ 工区密度热力图已保存到: {output_path}")
    
    # 统计信息
    print(f"\n【密度统计】")
    print(f"  工区总数: {len(zones)}")
    print(f"  总工期: {max_day}天")
    print(f"  平均密度: {density_matrix.mean():.2f}人/工区/天")
    print(f"  最大密度: {int(density_matrix.max())}人")
    
    # 找出最繁忙的工区和时间
    max_density_idx = np.unravel_index(density_matrix.argmax(), density_matrix.shape)
    max_zone = zones[max_density_idx[0]]
    max_day_num = max_density_idx[1]
    max_density_val = int(density_matrix[max_density_idx])
    
    print(f"\n【最繁忙时刻】")
    print(f"  工区: {max_zone}")
    print(f"  天数: 第{max_day_num}天")
    print(f"  焊工数: {max_density_val}人")
    
    # 工区繁忙度排名
    zone_avg_density = density_matrix.mean(axis=1)
    sorted_indices = np.argsort(zone_avg_density)[::-1]
    
    print(f"\n【最繁忙工区TOP5】")
    for i, idx in enumerate(sorted_indices[:5], 1):
        zone_name = zones[idx]
        avg_density = zone_avg_density[idx]
        max_density_in_zone = density_matrix[idx].max()
        print(f"  {i}. {zone_name:35s} | 平均:{avg_density:.1f}人 | 峰值:{int(max_density_in_zone)}人")
    
    plt.show()


if __name__ == '__main__':
    import os
    
    # 默认Excel路径
    excel_file = 'output_files/最优调度方案_汇总.xlsx'
    
    # 检查文件是否存在
    if not os.path.exists(excel_file):
        print(f"错误：找不到文件 {excel_file}")
        print("请先运行遗传算法生成调度方案")
    else:
        # 生成热力图
        generate_zone_density_heatmap(excel_file)

