import pandas as pd
import requests
import json
import logging

# 设置API请求参数
api_url = 'https://www.h3yun.com/OpenApi/Invoke'
headers = {
    "Content-Type": "application/json",
    "EngineCode": "ety58sf4upb95mibri9qatvi5",
    "EngineSecret": "u3toDgywMDwgbYgnrHNmjzH0g0fzn9mWAj0PY659taS7sxeVPoor5g=="
}

# 定义每页数据大小
batch_size = 500

# 设置字段及列名映射关系
# key: API返回的字段名, value: Excel中显示的列名
# 调整此处的顺序即可调整Excel中列的顺序
column_mapping = {
    'F0000036': '单元号',
    'F0000011': '管线号',
    'F0000037': '焊口号',
    'F0000057': '加字母焊口号',
    'F0000073': '单元名称',
    'F0000082': 'IDF发图日期',
    'F0000064': '增加焊口号',
    'F0000022': '管段号',
    'F0000046': '作业指导书编号（WPS号）',
    'F0000092': '出图页码',
    'F0000024': '焊接区域',
    'F0000052': '寸径',
    'F0000026': '壁厚号',
    'F0000027': '接头类型',
    'F0000028': '材质',
    'F0000053': '外径',
    'F0000054': '壁厚',
    'F0000031': '材料代号',
    'F0000032': '抽检代号',
    'F0000049': '焊接日期',
    'F0000034': '焊接方法',
    'F0000035': '焊接位置',
    'F0000038': '打底',
    'F0000039': '盖面',
    'F0000040': '原打底',
    'F0000041': '原盖面',
    'F0000042': '材料1',
    'F0000043': '炉批号A',
    'F0000044': '材料2',
    'F0000045': '炉批号B',
    'F0000050': '材质类型1',
    'F0000051': '材质类型2',
    'F0000055': '材质类型3',
    'F0000047': '焊材牌号',
    'F0000056': '排产单号',
    'F0000093': 'RT比例',
    'F0000058': '变更单号',
    'F0000059': '变更日期',
    'F0000060': '变更类型',
    'F0000072': '备注',
    'F0000062': '材料唯一码1',
    'F0000070': '描述1',
    'F0000063': '材料唯一码2',
    'F0000071': '描述2',
    'F0000066': '数量1',
    'F0000067': '数量2',
    'F0000068': '焊点坐标',
    'F0000069': '分区信息',
    'F0000075': '材料1防腐等级',
    'F0000076': '材料2防腐等级',
    'F0000074': '压力管道等级',
    'F0000077': '安装包分类',
    'F0000078': '安装包号',
    'F0000079': '安装包顺序',
    'F0000080': '施工班组',
    'F0000065': '试压包号',
    'F0000048': '自定义页码',
    'F0000061': '页码',
}

# 从映射关系中自动获取字段名列表
fields = list(column_mapping.keys())

# 构建分页查询参数函数
def build_params(start_row, batch_size):
    return {
        "ActionName": "LoadBizObjects",
        "SchemaCode": "D148357st3njemphkeqycrsatb0j",
        "Filter": "{\"FromRowNum\": " + str(
            start_row) + ", \"RequireCount\": false, \"ReturnItems\": [], \"SortByCollection\": [], \"ToRowNum\": " + str(
            start_row + batch_size) + ", \"Matcher\": {\"Type\": \"And\", \"Matchers\": []}}"
}

# 发送API请求，并分批获取数据
def fetch_data():
    start_row = 0
    all_data = []

    while True:
        request_body = build_params(start_row, batch_size)
        response = requests.post(api_url, json=request_body, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data["Successful"]:
                biz_objects = data["ReturnData"]["BizObjectArray"]
                all_data.extend(biz_objects)
                start_row += batch_size
                if len(biz_objects) < batch_size:
                    break
            else:
                logging.error(f"Error: {data['ErrorMessage']}")
                break
        else:
            logging.error(f"Request failed, Status code: {response.status_code}")
            break

    return all_data


# 获取数据并保存到本地 Excel 文件
all_data = fetch_data()

if all_data:
    # 将数据转换为DataFrame
    df = pd.DataFrame(all_data)

    # 确保只包含我们需要的字段，并按指定顺序排列
    # 如果API返回的字段比fields多或顺序不同，这能保证列的顺序和完整性
    df_filtered = df.reindex(columns=fields)

    # 应用列名映射
    df_renamed = df_filtered.rename(columns=column_mapping)

    # 定义输出文件名
    output_filename = "焊口初始化数据-金丹项目.xlsx"

    # 保存到Excel文件
    try:
        df_renamed.to_excel(output_filename, index=False)
        message = f"{len(all_data)} 条记录已成功保存到文件: {output_filename}"
        logging.info(message)
        print(message)
    except Exception as e:
        message = f"保存文件失败: {e}"
        logging.error(message)
        print(message)
        raise
else:
    message = "未获取到任何数据。"
    logging.info(message)
    print(message)
