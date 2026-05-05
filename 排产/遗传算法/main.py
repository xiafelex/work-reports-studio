"""
主程序：焊工调度遗传算法
"""
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 创建输出文件夹
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

from src.data_io import read_weld_info, read_pressure_package, read_pipe_property
from src.processing import initialize_weld_data, aggregate_pipeline_inches, merge_package_data
from src.processing.spatial_processing import process_spatial_data
from src.ga import GeneticAlgorithm
from src.utils import plot_convergence, plot_gantt_chart, export_schedule
from src.config import ENABLE_SPATIAL_CONSTRAINT


def load_and_process_data():
    """加载和处理数据，返回 ProjectData 对象"""
    print("=" * 60)
    print("焊工调度优化系统 - 数据初始化")
    print("=" * 60)
    
    # ====== 步骤1: 读取Excel数据 ======
    print("\n[步骤1] 读取Excel数据...")
    weld_df = read_weld_info()
    package_df = read_pressure_package()
    
    # 尝试读取管道特性表（可选）
    try:
        pipe_property_df = read_pipe_property()
    except FileNotFoundError:
        print("警告: 未找到管道特性表，将跳过单元名称补充")
        pipe_property_df = None
    except Exception as e:
        print(f"警告: 读取管道特性表失败 ({e})，将跳过单元名称补充")
        pipe_property_df = None
    
    # ====== 步骤2: 初始化焊口数据（返回 WeldPoint 对象列表） ======
    print("\n[步骤2] 初始化焊口数据...")
    weld_points = initialize_weld_data(weld_df, pipe_property_df)
    
    # ====== 步骤3: 按管线汇总寸径（返回 Pipeline 对象列表） ======
    print("\n[步骤3] 按管线汇总寸径...")
    pipelines = aggregate_pipeline_inches(weld_points)
    
    # ====== 步骤4: 合并施压包信息（返回 ProjectData 对象） ======
    print("\n[步骤4] 合并施压包信息...")
    project_data = merge_package_data(pipelines, package_df, weld_points)
    
    # ====== 步骤5: 空间数据处理（工区、块、管线分段） ======
    if ENABLE_SPATIAL_CONSTRAINT:
        spatial_success = process_spatial_data(project_data)
        if not spatial_success:
            print("\n警告: 空间数据处理失败，将使用原始管线进行调度")
    else:
        print("\n[跳过] 空间约束功能未启用（ENABLE_SPATIAL_CONSTRAINT=False）")
        spatial_success = False
    
    # ====== 输出最终结果 ======
    print("\n" + "=" * 60)
    print("数据初始化完成！")
    print("=" * 60)
    
    stats = project_data.get_statistics()
    print(f"\n最终数据集信息:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    if ENABLE_SPATIAL_CONSTRAINT and spatial_success:
        print(f"  工区数: {len(project_data.zones)}")
        print(f"  管线段数: {len(project_data.segments)}")
    
    return project_data


def run_genetic_algorithm_for_group(project_data, group_config, all_group_configs):
    """
    为一个优化组运行遗传算法
    
    参数:
        project_data: 项目数据
        group_config: 当前组配置
        all_group_configs: 所有组配置（用于判断"剩余单元"）
    """
    from src.config import GA_CONFIG, WORKING_HOURS_PER_DAY
    from src.models import WorkerGroup
    
    group_name = group_config['group_name']
    group_teams = group_config['teams']
    group_units = group_config['units']
    
    print("\n" + "=" * 60)
    print(f"开始优化 {group_name}")
    print("=" * 60)
    
    # 显示组内队伍信息
    total_workers = sum(team['size'] for team in group_teams)
    print(f"组内队伍数: {len(group_teams)}")
    for team in group_teams:
        print(f"  - {team['name']}: {team['size']}组焊工, 每组{team['daily_capacity']}寸/天")
    print(f"总焊工组数: {total_workers}")
    print(f"负责单元: {group_units if group_units else '剩余所有单元'}")
    
    # 构建 teams 字典和 team_units 字典
    teams_dict = {}
    team_daily_capacity = {}  # 每个队伍的daily_capacity
    
    for team in group_teams:
        teams_dict[team['team_id']] = team['size']
        team_daily_capacity[team['team_id']] = team['daily_capacity']
    
    # 构建 team_units 字典（用于判断"剩余单元"）
    # 收集所有组的单元配置
    all_team_units_dict = {}
    for grp_config in all_group_configs:
        for team in grp_config['teams']:
            team_id = team['team_id']
            units = grp_config['units']
            if units is not None:
                all_team_units_dict[team_id] = units
            else:
                all_team_units_dict[team_id] = None
    
    # 创建 WorkerGroup（使用组内第一个队伍的daily_capacity作为默认值）
    default_capacity = group_teams[0]['daily_capacity']
    worker_group = WorkerGroup(
        teams=teams_dict,
        team_units=all_team_units_dict,
        daily_capacity=default_capacity
    )
    
    # 为每个焊工设置其所属队伍的daily_capacity
    for worker in worker_group.workers:
        if worker.team_id in team_daily_capacity:
            worker.daily_capacity = team_daily_capacity[worker.team_id]
    
    # 优化：预先筛选该组能焊的管线/段，减少染色体长度和内存占用
    from src.config import ENABLE_SPATIAL_CONSTRAINT
    
    # 判断是否使用空间约束
    use_spatial = ENABLE_SPATIAL_CONSTRAINT and len(project_data.segments) > 0
    
    if use_spatial:
        print("\n筛选该组能焊的管线段（空间约束模式）...")
        filtered_segments = []
        for segment in project_data.segments:
            # 检查是否有任何一个队伍能焊这个段
            can_weld = False
            for worker in worker_group.workers:
                if worker_group.can_team_work_on_unit(worker.team_id, segment.unit_name):
                    can_weld = True
                    break
            
            if can_weld:
                filtered_segments.append(segment)
        
        print(f"原始管线段数: {len(project_data.segments)}")
        print(f"该组可焊管线段数: {len(filtered_segments)}")
        print(f"优化比例: {len(filtered_segments)/len(project_data.segments)*100:.1f}%")
        
        # 创建只包含该组段的 ProjectData
        from src.models import ProjectData
        filtered_project_data = ProjectData()
        
        # 添加段
        for segment in filtered_segments:
            filtered_project_data.add_segment(segment)
            # 添加段的焊口
            for weld_point in segment.weld_points:
                filtered_project_data.add_weld_point(weld_point)
        
        # 添加工区和块信息（从原project_data复制）
        for zone in project_data.zones:
            filtered_project_data.add_zone(zone)
        for block in project_data.blocks:
            filtered_project_data.add_block(block)
    else:
        print("\n筛选该组能焊的管线...")
        filtered_pipelines = []
        for pipeline in project_data.pipelines:
            # 检查是否有任何一个队伍能焊这条管线
            can_weld = False
            for worker in worker_group.workers:
                if worker_group.can_team_work_on_unit(worker.team_id, pipeline.unit_name):
                    can_weld = True
                    break
            
            if can_weld:
                filtered_pipelines.append(pipeline)
        
        print(f"原始管线数: {len(project_data.pipelines)}")
        print(f"该组可焊管线数: {len(filtered_pipelines)}")
        print(f"优化比例: {len(filtered_pipelines)/len(project_data.pipelines)*100:.1f}%")
        
        # 创建只包含该组管线的 ProjectData
        from src.models import ProjectData
        filtered_project_data = ProjectData()
        
        for pipeline in filtered_pipelines:
            filtered_project_data.add_pipeline(pipeline)
            for weld_point in pipeline.weld_points:
                filtered_project_data.add_weld_point(weld_point)
        
        # 构建试压包信息
        from collections import defaultdict
        from src.models import Package
        package_dict = defaultdict(list)
        for pipeline in filtered_pipelines:
            if pipeline.package_no:
                package_dict[pipeline.package_no].append(pipeline)
        
        for package_no, pipelines in package_dict.items():
            package = Package(package_no=package_no, pipelines=pipelines)
            filtered_project_data.add_package(package)
    
    # 创建GA实例（使用筛选后的数据）
    ga = GeneticAlgorithm(
        filtered_project_data,  # 使用筛选后的数据
        population_size=GA_CONFIG.get('population_size'),
        generations=GA_CONFIG.get('generations'),
        crossover_rate=GA_CONFIG.get('crossover_rate'),
        mutation_rate=GA_CONFIG.get('mutation_rate'),
        elite_size=GA_CONFIG.get('elite_size'),
        tournament_size=GA_CONFIG.get('tournament_size'),
        use_multiprocessing=GA_CONFIG.get('use_multiprocessing', True),
        n_processes=GA_CONFIG.get('n_processes'),
        worker_group=worker_group
    )
    
    # 运行算法（工区容量统计会在run()方法内自动打印）
    best_chromosome, best_schedule = ga.run(verbose=True)
    
    # 输出统计
    print("\n" + "=" * 60)
    print(f"{group_name} 优化完成！")
    print("=" * 60)
    
    stats = best_schedule.get_statistics()
    makespan = stats.get('总工期', 0)
    pipeline_count = stats.get('管线总数', 0)
    total_inches = stats.get('总寸径', 0)
    
    print(f"\n【最优工期】: {makespan} 天")
    
    # 显示各队伍完工时间
    for team in group_teams:
        team_name = team['name']
        team_id = team['team_id']
        team_time = best_schedule.get_team_completion_time(team_id)
        print(f"  {team_name} 完工时间: {team_time:.2f} 天")
    
    print(f"【管线总数】: {pipeline_count}")
    print(f"【总寸径】: {total_inches:.2f}")
    
    # 导出结果文件（收敛曲线单独保存，调度方案稍后汇总）
    print("\n" + "=" * 60)
    print("导出结果文件")
    print("=" * 60)
    
    # 绘制收敛曲线（每个组单独一个文件）
    convergence_file = os.path.join(OUTPUT_DIR, f"{group_name}_收敛曲线.png")
    plot_convergence(ga.history, convergence_file)
    print(f"{group_name}_收敛曲线.png 已生成")
    
    print(f"\n{group_name} 优化完成！")
    
    return best_schedule, makespan


def main():
    """主函数"""
    try:
        # 步骤1: 加载和处理数据
        project_data = load_and_process_data()
        
        if project_data is None or project_data.pipeline_count == 0:
            print("数据加载失败，程序退出")
            return
        
        # 步骤2: 为每个优化组运行遗传算法
        from src.config import WORKER_CONFIG
        
        print("\n" + "=" * 60)
        print(f"开始遗传算法优化（共 {len(WORKER_CONFIG)} 个优化组）")
        print("=" * 60)
        
        results = []
        for group_config in WORKER_CONFIG:
            schedule, makespan = run_genetic_algorithm_for_group(
                project_data, 
                group_config, 
                WORKER_CONFIG
            )
            results.append({
                'group_name': group_config['group_name'],
                'makespan': makespan,
                'schedule': schedule
            })
        
        # 步骤3: 汇总所有组的结果
        print("\n" + "=" * 60)
        print("所有优化组完成！汇总结果：")
        print("=" * 60)
        
        for result in results:
            print(f"\n{result['group_name']}: {result['makespan']:.2f} 天")
        
        max_makespan = max(r['makespan'] for r in results)
        print(f"\n最大工期（瓶颈）: {max_makespan:.2f} 天")
        
        # 步骤4: 合并所有组的调度方案到一个Excel
        print("\n" + "=" * 60)
        print("生成汇总调度方案")
        print("=" * 60)
        
        from src.models import Schedule
        combined_schedule = Schedule()
        
        # 合并所有组的任务
        for result in results:
            for task in result['schedule'].tasks:
                combined_schedule.add_task(task)
        
        # 导出汇总的调度方案
        combined_file = os.path.join(OUTPUT_DIR, "最优调度方案_汇总.xlsx")
        export_schedule(combined_schedule, combined_file)
        print(f"最优调度方案_汇总.xlsx 已生成（包含所有{len(combined_schedule.tasks)}个任务）")
        
        print("\n" + "=" * 60)
        print("程序执行完毕！")
        print("=" * 60)
        print(f"\n生成的文件（保存在 output_files 文件夹中）:")
        print(f"  - 最优调度方案_汇总.xlsx（所有组汇总）")
        for group_config in WORKER_CONFIG:
            group_name = group_config['group_name']
            print(f"  - {group_name}_收敛曲线.png")
        print(f"\n文件路径: {OUTPUT_DIR}")
        
    except FileNotFoundError as e:
        print(f"\n错误: {e}")
        print("\n请确保以下文件存在:")
        print("  1. 焊口初始化信息.xlsx")
        print("  2. 施压包划分汇总表.xlsx")
        
    except Exception as e:
        print(f"\n发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

