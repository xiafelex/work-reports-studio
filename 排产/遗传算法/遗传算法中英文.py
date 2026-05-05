import pandas as pd
import random
from deap import base, creator, tools

# 加载数据
excel_path = r'C:\Users\HP\Desktop\马来西亚项目\马来西亚分区\800\800单元脱硫框架一层安装口.xlsx'
df = pd.read_excel(excel_path)

# 转换 '英制' 列为浮点数
df['英制'] = df['英制'].astype(float)

# 提取 '英制' 和 '焊点位置' 列
diameters = df['英制'].tolist()
coords = df['焊点位置'].tolist()

# 处理坐标
x_coords = []
y_coords = []
z_coords = []
for coord in coords:
    x, y, z = map(float, coord.split(","))
    x_coords.append(x)
    y_coords.append(y)
    z_coords.append(z)

# 分区大小
block_size = 10000

# 设置每天的焊工人数
num_workers_per_day = 10

# 设置每个焊工每天焊接寸径量
num_diameters_per_worker = 10

# 创建工区字典
weld_zones = {}
for (x, y, z), diameter in zip(zip(x_coords, y_coords, z_coords), diameters):
    weld_zone = (int(x // block_size), int(y // block_size), int(z // block_size))
    if weld_zone not in weld_zones:
        weld_zones[weld_zone] = {'total_diameter': 0, 'weld_count': 0}
    weld_zones[weld_zone]['total_diameter'] += float(diameter)
    weld_zones[weld_zone]['weld_count'] += 1

# 映射工区到索引
zone_to_index = {zone: idx for idx, zone in enumerate(weld_zones.keys())}
index_to_zone = {idx: zone for zone, idx in zone_to_index.items()}

# 打印每个工区的英制总和和焊缝数量
print(f"各工区信息：\nDetails per zone:\n")
print(f"工区总数：{len(weld_zones)}\nTotal Zones: {len(weld_zones)}\n")
total_diameter = sum(zone['total_diameter'] for zone in weld_zones.values())
total_welds = sum(zone['weld_count'] for zone in weld_zones.values())
print(f"寸径总数：{total_diameter}，焊缝总数：{total_welds}\nTotal diameter Sum: {total_diameter}, Total Welds: {total_welds}\n")
for idx, zone_data in enumerate(weld_zones.values()):
    print(f"工区 {idx}：寸径数 = {zone_data['total_diameter']}，焊缝数 = {zone_data['weld_count']}\nZone {idx}: Sum of diameter = {zone_data['total_diameter']}, Welds = {zone_data['weld_count']}\n")
    
# 确定总工区数量
total_zones = len(weld_zones.keys())
def valid_individual_generator():
    individual = []
    # 随机选择不同的工区，直到达到所需长度
    while len(individual) < num_workers_per_day * total_zones:
        zone = random.randint(0, total_zones - 1)
        if individual.count(zone) < num_workers_per_day:
            individual.append(zone)
    random.shuffle(individual) # 打乱顺序以增加多样性
    return individual

# 适应度函数
def fitness(individual):
    days = 0
    weld_zones_copy = {key: list(value) for key, value in weld_zones.items()}
    for i in range(0, len(individual), num_workers_per_day):
        daily_zones = individual[i:i + num_workers_per_day]

        # 跳过没有焊工分配的天数
        if not any(weld_zones_copy.get(zone) for zone in daily_zones):
            continue

        if len(set(daily_zones)) < num_workers_per_day: # 惩罚未能分配5个不同工区的情况
            return float('inf'),

        for zone in daily_zones:
            if weld_zones_copy[zone]:
                weld_zones_copy[zone].pop()
        days += 1
        if all(not weld_zones_copy[zone] for zone in weld_zones_copy):
            break
    return days,



# 使用DEAP库设置遗传算法
creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)

toolbox = base.Toolbox()
toolbox.register("attr_zone", random.randint, 0, total_zones - 1)
toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_zone, n= num_workers_per_day * total_zones)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
toolbox.register("evaluate", fitness)
toolbox.register("mate", tools.cxTwoPoint)
toolbox.register("mutate", tools.mutUniformInt, low=0, up=total_zones - 1, indpb=0.2)
toolbox.register("select", tools.selTournament, tournsize=3)

# 进行遗传算法迭代
population = toolbox.population(n=100)
NGEN = 100
# 初始化 weld_zones_copy
weld_zones_copy = {key: list(value) for key, value in weld_zones.items()}

for gen in range(NGEN):
    offspring = toolbox.select(population, len(population))
    offspring = list(map(toolbox.clone, offspring))
    

    
    for child1, child2 in zip(offspring[::2], offspring[1::2]):
        if random.random() < 0.7:
            toolbox.mate(child1, child2)
            del child1.fitness.values
            del child2.fitness.values

    for mutant in offspring:
        if random.random() < 0.2:
            toolbox.mutate(mutant)
            del mutant.fitness.values

    fitnesses = map(toolbox.evaluate, offspring)
    for ind, fit in zip(offspring, fitnesses):
        ind.fitness.values = fit

    remaining_zones = [i for i, welds in weld_zones_copy.items() if welds]
    if not remaining_zones:
        break

# 输出最佳解
best_ind = tools.selBest(population, 1)[0]
best_schedule = [best_ind[i:i+ num_workers_per_day] for i in range(0, len(best_ind), num_workers_per_day)]
weld_zones_copy = {key: value.copy() for key, value in weld_zones.items()}
remaining_zones = [i for i, welds in weld_zones_copy.items() if welds]

# 初始化当前焊接数量
# 修改 initial_welds_count 为每个工区的'英制'总和
initial_welds_sum = {idx: zone_data['total_diameter'] for idx, (zone, zone_data) in enumerate(weld_zones.items())}

# 初始化当前工区的剩余'英制'
current_welds_sum = initial_welds_sum.copy()

print("最佳调度是：\nBest scheduling is:\n")
day_count = 0
for daily_zones in best_schedule:
    if not any(idx in remaining_zones for idx in daily_zones):
        continue  # 跳过没有焊工分配的天数

    day_count += 1
    print(f"第 {day_count} 天：\nDay {day_count}:")
    for worker, idx in enumerate(daily_zones, 1):
        if idx not in remaining_zones:
            print(f"  焊工 {worker}：今天没有分配工作\n Welder {worker}: There is no assignment of work today\n")
            continue

        zone = index_to_zone[idx]
        # 减少工区的剩余'英制'数为10
        current_welds_sum[idx] = max(0, current_welds_sum[idx] - num_diameters_per_worker)
        if current_welds_sum[idx] <= 0:
            if idx in remaining_zones:
                remaining_zones.remove(idx)


        print(f"  焊工 {worker}：分配至工区 {idx}, 当日后该工区剩余寸径：{current_welds_sum[idx]}\n  Welder {worker}: Assigned to work area {idx},after the same day, the remaining inch diameter of the work area: {current_welds_sum[idx]}\n")
    print(f"   {day_count} 天后剩余总寸径: {sum(current_welds_sum.values())}\n   Total diameter remaining after {day_count} days: {sum(current_welds_sum.values())}\n")

# 如果在计划的天数内没有完成所有焊接任务，则继续安排剩余任务
while sum(current_welds_sum.values()) > 0:
    available_zones = [zone for zone, total in current_welds_sum.items() if total > 0]
    num_workers = min(len(available_zones), num_workers_per_day)

    print(f"第 {day_count + 1} 天：\nDay {day_count + 1}:")
    for worker in range(num_workers):
        idx = available_zones[worker]
        zone = index_to_zone[idx]

        # 减少工区的剩余'英制'数为10
        current_welds_sum[idx] = max(0, current_welds_sum[idx] - num_diameters_per_worker)
        if current_welds_sum[idx] <= 0:
            if idx in remaining_zones:
                remaining_zones.remove(idx)


        print(f"  焊工 {worker + 1}：分配至工区 {idx}, 当日后该工区剩余寸径：{current_welds_sum[idx]}\n  Welder {worker + 1}: Assigned to work area {idx},after the same day, the remaining inch diameter of the work area: {current_welds_sum[idx]}\n")
    print(f"   {day_count + 1} 天后剩余总寸径：{sum(current_welds_sum.values())}\n   Total diameter remaining after {day_count + 1} days: {sum(current_welds_sum.values())}\n")
    day_count += 1
