"""
焊口数据初始化模块
职责：清洗和规范化焊口数据，并转换为领域对象
"""
import pandas as pd
from typing import List, Optional
from ..models.data_model import WeldPoint


def initialize_weld_data(weld_df: pd.DataFrame, 
                        pipe_property_df: Optional[pd.DataFrame] = None) -> List[WeldPoint]:
    """
    初始化焊口数据并转换为领域对象
    
    处理逻辑：
    1. 如果焊口号为空，使用加字母焊口号填充
    2. 过滤焊口号仍为空的记录
    3. 过滤寸径无效的记录
    4. 如果单元名称为空，通过管线号从管道特性表获取
    5. 过滤焊接日期有值的记录（已焊接）
    6. 转换为 WeldPoint 对象列表
    
    参数:
        weld_df: 原始焊口数据DataFrame
                 需包含列：pipeline_no, weld_no, weld_no_with_letter, diameter, 
                          unit_no, unit_name, weld_date
        pipe_property_df: 管道特性表DataFrame（可选）
                         包含列：pipeline_no, unit_name
    
    返回:
        WeldPoint 对象列表
    """
    # 复制数据，避免修改原始数据
    df = weld_df.copy()
    
    # 记录原始数据量
    original_count = len(df)
    
    # 1. 如果焊口号为空，使用加字母焊口号填充
    mask_empty_weld_no = df['weld_no'].isna() | (df['weld_no'].astype(str).str.strip() == '')
    df.loc[mask_empty_weld_no, 'weld_no'] = df.loc[mask_empty_weld_no, 'weld_no_with_letter']
    
    filled_count = mask_empty_weld_no.sum()
    print(f"使用加字母焊口号填充了 {filled_count} 条记录")
    
    # 2. 过滤焊口号仍为空的记录
    df = df[~(df['weld_no'].isna() | (df['weld_no'].astype(str).str.strip() == ''))]
    
    filtered_count = original_count - len(df)
    print(f"过滤掉焊口号为空的记录 {filtered_count} 条")
    
    # 3. 过滤寸径为空或无效的记录
    invalid_diameter = df['diameter'].isna() | (df['diameter'] <= 0)
    if invalid_diameter.any():
        invalid_count = invalid_diameter.sum()
        df = df[~invalid_diameter]
        print(f"过滤掉寸径无效的记录 {invalid_count} 条")
    
    # 4. 如果单元名称为空，通过管线号从管道特性表获取
    if pipe_property_df is not None:
        # 创建管线号到单元名称的映射
        pipe_unit_map = dict(zip(pipe_property_df['pipeline_no'], 
                                pipe_property_df['unit_name']))
        
        # 统计空单元名称数量
        empty_unit_name = df['unit_name'].isna() | (df['unit_name'].astype(str).str.strip() == '')
        empty_count = empty_unit_name.sum()
        
        if empty_count > 0:
            # 为空单元名称填充
            for idx in df[empty_unit_name].index:
                pipeline_no = str(df.loc[idx, 'pipeline_no'])
                if pipeline_no in pipe_unit_map:
                    df.loc[idx, 'unit_name'] = pipe_unit_map[pipeline_no]
            
            filled_count = sum(1 for idx in df[empty_unit_name].index 
                             if str(df.loc[idx, 'pipeline_no']) in pipe_unit_map)
            print(f"从管道特性表填充了 {filled_count} 条单元名称")
    
    # 5. 标记已焊接焊口（保留在数据中，但标记状态）
    welded_mask = df['weld_date'].notna() & (df['weld_date'].astype(str).str.strip() != '')
    welded_count = welded_mask.sum()
    
    # 添加焊接状态标记
    df['is_welded'] = welded_mask
    
    if welded_count > 0:
        print(f"标记已焊接的焊口 {welded_count} 条（保留在拓扑中）")
    
    # 6. 解析焊点坐标
    coord_parsed = 0
    coord_empty = 0
    
    if 'coord' in df.columns:
        # 添加坐标列
        df['x'] = None
        df['y'] = None
        df['z'] = None
        
        for idx, row in df.iterrows():
            coord_str = row['coord']
            if pd.notna(coord_str) and str(coord_str).strip():
                try:
                    # 解析坐标字符串（格式："-100.00,46200.00,3848.65" 或 "-1000.00 ,44365.00 ,6658.50"）
                    coord_clean = str(coord_str).strip()
                    parts = coord_clean.split(',')
                    if len(parts) == 3:
                        df.at[idx, 'x'] = float(parts[0].strip())
                        df.at[idx, 'y'] = float(parts[1].strip())
                        df.at[idx, 'z'] = float(parts[2].strip())
                        coord_parsed += 1
                except Exception:
                    pass  # 解析失败的记录会在后续被过滤掉
            else:
                coord_empty += 1
        
        print(f"坐标解析完成: 成功 {coord_parsed} 条, 空坐标 {coord_empty} 条")
        
        # 过滤掉无坐标的焊口（空间约束要求）
        has_coord = df['x'].notna() & df['y'].notna() & df['z'].notna()
        no_coord_count = (~has_coord).sum()
        
        if no_coord_count > 0:
            df = df[has_coord]
            print(f"过滤掉无坐标的焊口 {no_coord_count} 条（空间约束要求）")
    
    # 7. 转换为 WeldPoint 对象列表
    weld_points = []
    for _, row in df.iterrows():
        weld_point = WeldPoint(
            pipeline_no=str(row['pipeline_no']),
            weld_no=str(row['weld_no']),
            diameter=float(row['diameter']),
            unit_no=str(row['unit_no']) if pd.notna(row['unit_no']) else None,
            unit_name=str(row['unit_name']) if pd.notna(row['unit_name']) else None,
            weld_date=str(row['weld_date']) if pd.notna(row['weld_date']) else None,
            # 新增：坐标
            x=float(row['x']) if 'x' in row and pd.notna(row['x']) else None,
            y=float(row['y']) if 'y' in row and pd.notna(row['y']) else None,
            z=float(row['z']) if 'z' in row and pd.notna(row['z']) else None,
            # 新增：焊接状态
            is_welded=bool(row['is_welded']) if 'is_welded' in row else False,
            # 新增：材料唯一码
            material_unique_code1=str(row['material_unique_code1']) if 'material_unique_code1' in row and pd.notna(row['material_unique_code1']) else None,
            material_unique_code2=str(row['material_unique_code2']) if 'material_unique_code2' in row and pd.notna(row['material_unique_code2']) else None,
            # 新增：材料描述
            material_description1=str(row['material_description1']) if 'material_description1' in row and pd.notna(row['material_description1']) else None,
            material_description2=str(row['material_description2']) if 'material_description2' in row and pd.notna(row['material_description2']) else None
        )
        weld_points.append(weld_point)
    
    # 统计焊接状态
    welded_count = sum(1 for wp in weld_points if wp.is_welded)
    unwelded_count = len(weld_points) - welded_count
    print(f"数据初始化完成，生成 {len(weld_points)} 个焊口对象（已焊接: {welded_count}, 未焊接: {unwelded_count}）")
    
    return weld_points

