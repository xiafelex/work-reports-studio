"""
配置文件：存储项目路径和常量配置
"""
import os

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Excel文件路径（写死）
WELD_INFO_FILE = os.path.join(PROJECT_ROOT, "焊口初始化数据-金丹项目.xlsx")
PRESSURE_PACKAGE_FILE = os.path.join(PROJECT_ROOT, "试压包划分汇总表1020.xlsx")
PIPE_PROPERTY_FILE = os.path.join(PROJECT_ROOT, "管线特性表.xlsx")

# Excel工作簿名称
PRESSURE_PACKAGE_SHEET_NAME = "试压包划分总表"

# 焊口信息表列名
WELD_COLUMNS = {
    "pipeline_no": "管线号",
    "weld_no": "焊口号",
    "weld_no_with_letter": "加字母焊口号",
    "diameter": "寸径",
    "unit_no": "单元号",
    "unit_name": "单元名称",
    "weld_date": "焊接日期",
    "coord": "焊点坐标",  # 坐标列（格式："x,y,z"）
    "material_unique_code1": "材料唯一码1",  # 材料唯一码1
    "material_unique_code2": "材料唯一码2",  # 材料唯一码2
    "material_description1": "描述1",        # 新增：材料描述1
    "material_description2": "描述2"         # 新增：材料描述2
}

# 管道特性表列名
PIPE_PROPERTY_COLUMNS = {
    "pipeline_no": "管线号",
    "unit_name": "单元名称"
}

# 施压包表列名
PACKAGE_COLUMNS = {
    "pipeline_no": "管线号",
    "package_no": "试压包号"
}

# 遗传算法参数
GA_CONFIG = {
    "population_size": 10,    # 种群大小（越大越慢但可能更优）
    "generations": 5,        # 迭代代数（越多越慢但可能更优）
    "crossover_rate": 0.8,     # 交叉率
    "mutation_rate": 0.25,     # 变异率（提高到25%以增强搜索能力）
    "elite_size": 2,           # 精英数量（约为种群的10%）
    "tournament_size": 2,      # 锦标赛大小
    "use_multiprocessing": True,  # 是否使用多进程加速
    "n_processes": 8,          # 进程数（None=自动检测CPU核心数-1）
    
    # 适应度函数权重配置
    # 适应度 = 1 / (总工期 + package_weight×试压包分散惩罚 + load_balance_weight×负载均衡惩罚 + continuity_weight×连续性惩罚 + diameter_weight×管径惩罚)
    "package_weight": 0.0,      # 试压包分散惩罚权重（硬约束已保证，建议设为0）
    "load_balance_weight": 0.0, # 负载均衡惩罚权重
                                # 0.0  = 不考虑均衡，纯粹追求最短工期（推荐，队伍独立工作）
                                # 0.02 = 稍微考虑均衡（在工期相近时选择更均衡的方案）
                                # 0.5  = 强制均衡（可能导致总工期增加）
    "continuity_weight": 0.0,   # 连续性惩罚权重（混合方案）
                                # 0.0 = 不考虑连续性（只依赖等待机制）
                                # 0.5 = 平衡工期和连续性（推荐：跳包10次≈延迟0.5天）
                                # 1.0 = 强调连续性（可能略微增加工期）
    "diameter_weight": 0.1      # 管径优先惩罚权重（新增）
                                # 0.0 = 不考虑管径优先级
                                # 0.1 = 轻微偏好大管径优先（推荐）
                                # 0.5 = 强制大管径优先（可能影响总工期）
}

# 焊工配置（分组模式）
# 每个组会运行一次遗传算法，组内的队伍共同优化
# 支持：单队伍独立工作 或 多队伍协作工作
WORKER_CONFIG = [
    # 示例1：单队伍独立负责
    {
        "group_name": "一队",           # 组名（用于文件命名和显示）
        "teams": [
            {
                "team_id": 1,             # 队伍ID
                "name": "一队",            # 队伍名称
                "size": 60,               # 焊工组数
                "daily_capacity": 25      # 该队伍每组每天焊接寸数
            }
        ],
        "units": ["丙交酯框架", "丙交酯车间"]  # 该组负责的单元
    }
    
    # # 示例2：单队伍独立负责
    # {
    #     "group_name": "二队",
    #     "teams": [
    #         {"team_id": 2, "name": "二队", "size": 25, "daily_capacity": 25}
    #     ],
    #     "units": ["管廊", "聚合车间"]
    # },
    
    # # 示例3：单队伍负责剩余单元
    # {
    #     "group_name": "三队",
    #     "teams": [
    #         {"team_id": 3, "name": "三队", "size": 25, "daily_capacity": 25}
    #     ],
    #     "units": None  # None表示负责所有未被其他组配置的单元
    # }
    
    # 示例4（可选）：多队伍协作负责同一区域
    # {
    #     "group_name": "丙交酯协作组",
    #     "teams": [
    #         {"team_id": 1, "name": "一队", "size": 60, "daily_capacity": 25},
    #         {"team_id": 2, "name": "二队", "size": 25, "daily_capacity": 20}  # 二队效率较低
    #     ],
    #     "units": ["丙交酯框架", "丙交酯车间"]
    # }
]

# 全局配置
WORKING_HOURS_PER_DAY = 8  # 每天工作小时数

# 空间工区配置
ENABLE_SPATIAL_CONSTRAINT = True  # 是否启用空间约束
LARGE_DIAMETER_THRESHOLD = 600.0  # 大管径阈值，超过此值的管线才按工区分段

