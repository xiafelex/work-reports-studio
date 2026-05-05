"""
Excel文件读取模块
职责：读取Excel文件并返回DataFrame
"""
import pandas as pd
from typing import Optional
from ..config import (
    WELD_INFO_FILE, 
    PRESSURE_PACKAGE_FILE,
    PIPE_PROPERTY_FILE,
    PRESSURE_PACKAGE_SHEET_NAME,
    WELD_COLUMNS,
    PACKAGE_COLUMNS,
    PIPE_PROPERTY_COLUMNS
)


def read_weld_info(file_path: Optional[str] = None) -> pd.DataFrame:
    """
    读取焊口初始化信息Excel
    
    参数:
        file_path: Excel文件路径，默认使用配置文件中的路径
    
    返回:
        DataFrame，包含列：管线号、焊口号、加字母焊口号、寸径
    """
    if file_path is None:
        file_path = WELD_INFO_FILE
    
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path)
        
        # 提取需要的列
        required_columns = list(WELD_COLUMNS.values())
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Excel文件缺少以下列: {missing_columns}")
        
        # 只保留需要的列并重命名
        df = df[required_columns].copy()
        df.columns = list(WELD_COLUMNS.keys())
        
        print(f"成功读取焊口信息，共 {len(df)} 条记录")
        return df
        
    except FileNotFoundError:
        raise FileNotFoundError(f"找不到文件: {file_path}")
    except Exception as e:
        raise Exception(f"读取焊口信息失败: {str(e)}")


def read_pressure_package(file_path: Optional[str] = None, 
                          sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    读取施压包划分汇总表Excel
    
    参数:
        file_path: Excel文件路径，默认使用配置文件中的路径
        sheet_name: 工作簿名称，默认使用配置文件中的名称
    
    返回:
        DataFrame，包含列：管线号、试压包号
    """
    if file_path is None:
        file_path = PRESSURE_PACKAGE_FILE
    
    if sheet_name is None:
        sheet_name = PRESSURE_PACKAGE_SHEET_NAME
    
    try:
        # 读取指定工作簿
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # 提取需要的列
        required_columns = list(PACKAGE_COLUMNS.values())
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Excel文件缺少以下列: {missing_columns}")
        
        # 只保留需要的列并重命名
        df = df[required_columns].copy()
        df.columns = list(PACKAGE_COLUMNS.keys())
        
        print(f"成功读取施压包信息，共 {len(df)} 条记录")
        return df
        
    except FileNotFoundError:
        raise FileNotFoundError(f"找不到文件: {file_path}")
    except ValueError as e:
        if "Worksheet" in str(e):
            raise ValueError(f"找不到工作簿: {sheet_name}")
        raise e
    except Exception as e:
        raise Exception(f"读取施压包信息失败: {str(e)}")


def read_pipe_property(file_path: Optional[str] = None) -> pd.DataFrame:
    """
    读取管道特性表Excel
    
    参数:
        file_path: Excel文件路径，默认使用配置文件中的路径
    
    返回:
        DataFrame，包含列：管线号、单元名称
    """
    if file_path is None:
        file_path = PIPE_PROPERTY_FILE
    
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path)
        
        # 提取需要的列
        required_columns = list(PIPE_PROPERTY_COLUMNS.values())
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Excel文件缺少以下列: {missing_columns}")
        
        # 只保留需要的列并重命名
        df = df[required_columns].copy()
        df.columns = list(PIPE_PROPERTY_COLUMNS.keys())
        
        print(f"成功读取管道特性表，共 {len(df)} 条记录")
        return df
        
    except FileNotFoundError:
        raise FileNotFoundError(f"找不到文件: {file_path}")
    except Exception as e:
        raise Exception(f"读取管道特性表失败: {str(e)}")

