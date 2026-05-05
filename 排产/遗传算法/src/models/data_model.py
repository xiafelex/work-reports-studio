"""
数据领域模型
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from math import sqrt


@dataclass
class WeldPoint:
    """
    焊口模型
    """
    pipeline_no: str        # 管线号
    weld_no: str            # 焊口号
    diameter: float         # 寸径
    unit_no: str = None     # 单元号
    unit_name: str = None   # 单元名称
    weld_date: str = None   # 焊接日期
    # 空间坐标
    x: float = None         # X坐标（毫米）
    y: float = None         # Y坐标（毫米）
    z: float = None         # Z坐标（毫米）
    zone_name: str = None   # 所属工区名称
    block_id: str = None    # 所属块ID（已废弃）
    grid_id: Tuple[str, int, int, int] = None  # 所属网格ID (zone_name, grid_x, grid_y, grid_z)
    # 拓扑相关
    material_unique_code1: str = None  # 材料唯一码1
    material_unique_code2: str = None  # 材料唯一码2
    material_description1: str = None  # 材料描述1
    material_description2: str = None  # 材料描述2
    sequence_in_pipeline: int = None   # 在管线中的焊接序号
    is_welded: bool = False            # 是否已焊接
    
    def __repr__(self):
        return f"WeldPoint({self.pipeline_no}, {self.weld_no}, {self.diameter}寸)"
    
    def __hash__(self):
        return hash((self.pipeline_no, self.weld_no))
    
    def __eq__(self, other):
        if not isinstance(other, WeldPoint):
            return False
        return self.pipeline_no == other.pipeline_no and self.weld_no == other.weld_no


@dataclass
class MaterialComponent:
    """
    管件模型（管子、弯头、法兰、阀门等）
    """
    material_unique_code: str   # 材料唯一码
    material_description: str   # 材料描述
    unit_name: str = None       # 单元名称
    
    def __hash__(self):
        return hash(self.material_unique_code)
    
    def __eq__(self, other):
        if not isinstance(other, MaterialComponent):
            return False
        return self.material_unique_code == other.material_unique_code
    
    def __repr__(self):
        return f"MaterialComponent({self.material_unique_code}, {self.material_description[:20]}...)"


@dataclass
class Pipeline:
    """
    管线模型
    """
    pipeline_no: str        # 管线号
    total_inches: float     # 总寸径
    weld_count: int         # 焊口数量
    package_no: str         # 所属试压包号
    unit_name: str = None   # 单元名称
    weld_points: List[WeldPoint] = None  # 包含的焊口列表（可选）
    # 拓扑相关
    is_cross_zone: bool = False  # 是否跨工区管线
    zones_involved: List[str] = None  # 涉及的工区列表
    
    def __post_init__(self):
        if self.weld_points is None:
            self.weld_points = []
        if self.zones_involved is None:
            self.zones_involved = []
    
    def add_weld_point(self, weld_point: WeldPoint):
        """添加焊口"""
        self.weld_points.append(weld_point)
    
    def __repr__(self):
        return f"Pipeline({self.pipeline_no}, {self.total_inches}寸, {self.weld_count}个焊口, 包{self.package_no})"
    
    def __hash__(self):
        return hash(self.pipeline_no)
    
    def __eq__(self, other):
        if isinstance(other, Pipeline):
            return self.pipeline_no == other.pipeline_no
        return False


@dataclass
class Package:
    """
    试压包模型
    """
    package_no: str                 # 试压包号
    pipelines: List[Pipeline]       # 包含的管线列表
    
    def __post_init__(self):
        if self.pipelines is None:
            self.pipelines = []
    
    @property
    def total_inches(self) -> float:
        """试压包总寸径"""
        return sum(p.total_inches for p in self.pipelines)
    
    @property
    def pipeline_count(self) -> int:
        """管线数量"""
        return len(self.pipelines)
    
    def add_pipeline(self, pipeline: Pipeline):
        """添加管线"""
        self.pipelines.append(pipeline)
    
    def get_pipeline_ids(self) -> List[str]:
        """获取所有管线号"""
        return [p.pipeline_no for p in self.pipelines]
    
    def __repr__(self):
        return f"Package({self.package_no}, {self.pipeline_count}条管线, {self.total_inches}寸)"


@dataclass
class Block:
    """
    空间块模型（立方体）
    """
    block_id: str              # 块ID，格式："单元名_工区名_x_y_z"
    zone_name: str             # 所属工区名称
    unit_name: str             # 所属单元名称
    x_min: float               # 块的空间范围（毫米）
    y_min: float
    z_min: float
    x_max: float
    y_max: float
    z_max: float
    center: Tuple[float, float, float] = None  # 块中心坐标
    weld_points: List[WeldPoint] = field(default_factory=list)  # 该块内的焊口
    
    def __post_init__(self):
        """计算块中心坐标"""
        if self.center is None:
            self.center = (
                (self.x_min + self.x_max) / 2,
                (self.y_min + self.y_max) / 2,
                (self.z_min + self.z_max) / 2
            )
    
    def contains_point(self, x: float, y: float, z: float) -> bool:
        """判断点是否在块内"""
        return (self.x_min <= x <= self.x_max and
                self.y_min <= y <= self.y_max and
                self.z_min <= z <= self.z_max)
    
    def distance_to_point(self, x: float, y: float, z: float) -> float:
        """计算点到块中心的距离"""
        return sqrt(
            (x - self.center[0])**2 +
            (y - self.center[1])**2 +
            (z - self.center[2])**2
        )
    
    @property
    def total_inches(self) -> float:
        """块内总寸径"""
        return sum(wp.diameter for wp in self.weld_points)
    
    def __repr__(self):
        return f"Block({self.block_id}, {len(self.weld_points)}个焊口, {self.total_inches:.2f}寸)"


@dataclass
class Zone:
    """
    工区模型
    """
    zone_name: str             # 工区名称
    unit_name: str             # 所属单元
    x1: float                  # 工区边界（毫米）
    y1: float
    z1: float
    x2: float
    y2: float
    z2: float
    blocks: List[Block] = field(default_factory=list)  # 该工区的所有块
    grid_enabled: bool = True        # 是否启用网格划分
    grid_size: float = None          # 网格尺寸（毫米）
    max_workers_per_grid: int = None # 网格容量上限
    
    def contains_point(self, x: float, y: float, z: float) -> bool:
        """判断点是否在工区内"""
        return (min(self.x1, self.x2) <= x <= max(self.x1, self.x2) and
                min(self.y1, self.y2) <= y <= max(self.y1, self.y2) and
                min(self.z1, self.z2) <= z <= max(self.z1, self.z2))
    
    def get_center(self) -> Tuple[float, float, float]:
        """获取工区中心坐标"""
        return (
            (self.x1 + self.x2) / 2,
            (self.y1 + self.y2) / 2,
            (self.z1 + self.z2) / 2
        )
    
    def distance_to_point(self, x: float, y: float, z: float) -> float:
        """计算点到工区中心的距离"""
        center = self.get_center()
        return sqrt(
            (x - center[0])**2 +
            (y - center[1])**2 +
            (z - center[2])**2
        )
    
    @property
    def total_inches(self) -> float:
        """工区总寸径"""
        return sum(block.total_inches for block in self.blocks)
    
    def __repr__(self):
        return f"Zone({self.zone_name}, {self.unit_name}, {len(self.blocks)}个块)"



DEFAULT_GRID_SIZE = 6000
DEFAULT_MAX_WORKERS_PER_GRID = 1

# 内置工区配置（若个别工区需覆写网格参数，可在字典中直接指定）
ZONE_DEFINITIONS = [
    {
        "unit_name": "聚合车间",
        "zone_name": "聚合框架一层",
        "x1": 13262,
        "y1": 26824,
        "z1": -467,
        "x2": 79973,
        "y2": 69695,
        "z2": 7543,
    },
    {
        "unit_name": "聚合车间",
        "zone_name": "聚合框架二层",
        "x1": 24579,
        "y1": 38126,
        "z1": 7470,
        "x2": 79983,
        "y2": 70392,
        "z2": 14443,
    },
    {
        "unit_name": "聚合车间",
        "zone_name": "聚合框架三层",
        "x1": 32223,
        "y1": 38094,
        "z1": 14262,
        "x2": 80791,
        "y2": 70810,
        "z2": 26489,
    },
    {
        "unit_name": "动力站",
        "zone_name": "动力站",
        "x1": -3248,
        "y1": 92028,
        "z1": -67,
        "x2": 16614,
        "y2": 130399,
        "z2": 10002,
    },
    {
        "unit_name": "热媒站",
        "zone_name": "热媒站",
        "x1": -45839,
        "y1": 32550,
        "z1": -231,
        "x2": -34191,
        "y2": 64372,
        "z2": 8414,
    },
    {
        "unit_name": "原料及中间物罐区",
        "zone_name": "原料及中间物罐区",
        "x1": -139487,
        "y1": -88842,
        "z1": -342,
        "x2": -27908,
        "y2": -62815,
        "z2": 15459,
    },
    {
        "unit_name": "管廊",
        "zone_name": "动力站北侧东西向管廊",
        "x1": -3410,
        "y1": 126310,
        "z1": 565,
        "x2": 58923,
        "y2": 131310,
        "z2": 5670,
    },
    {
        "unit_name": "管廊",
        "zone_name": "动力站西侧南北向管廊",
        "x1": -9251,
        "y1": 92006,
        "z1": 736,
        "x2": -3138,
        "y2": 131183,
        "z2": 7064,
    },
    {
        "unit_name": "管廊",
        "zone_name": "热煤站南侧东西向管廊",
        "x1": -118900,
        "y1": 37879,
        "z1": 457,
        "x2": -28895,
        "y2": 46115,
        "z2": 10184,
    },
    {
        "unit_name": "管廊",
        "zone_name": "丙交酯西侧南北向管廊(跨道路)",
        "x1": -29617,
        "y1": -33371,
        "z1": -468,
        "x2": 1549,
        "y2": 92163,
        "z2": 13004,
    },
    {
        "unit_name": "管廊",
        "zone_name": "原料及中间物罐区东侧南北向管廊(跨道路)",
        "x1": -34753,
        "y1": -84286,
        "z1": 315,
        "x2": -2263,
        "y2": -33303,
        "z2": 9775,
    },
    {
        "unit_name": "管廊",
        "zone_name": "熔融结晶界区附近管廊",
        "x1": 1378,
        "y1": 39766,
        "z1": 277,
        "x2": 20564,
        "y2": 46886,
        "z2": 8288,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架北侧泵组",
        "x1": 136,
        "y1": 68045,
        "z1": -309,
        "x2": 20415,
        "y2": 73045,
        "z2": 9973,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架北18米以下",
        "x1": -4368,
        "y1": 59732,
        "z1": -193,
        "x2": 21658,
        "y2": 68082,
        "z2": 17832,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架北18米以上",
        "x1": -502,
        "y1": 59738,
        "z1": 17763,
        "x2": 21384,
        "y2": 67693,
        "z2": 30958,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架中间区域西侧框架",
        "x1": -2621,
        "y1": 52138,
        "z1": -108,
        "x2": 7589,
        "y2": 60432,
        "z2": 51945,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架中间区域东侧框架",
        "x1": 7364,
        "y1": 52216,
        "z1": -301,
        "x2": 21507,
        "y2": 59847,
        "z2": 29596,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架南",
        "x1": -2863,
        "y1": 46494,
        "z1": -2248,
        "x2": 20921,
        "y2": 52278,
        "z2": 39163,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架南侧内管廊",
        "x1": -4507,
        "y1": 41677,
        "z1": 4076,
        "x2": 21701,
        "y2": 46677,
        "z2": 9076,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架南侧内管廊下",
        "x1": -2567,
        "y1": 41669,
        "z1": -864,
        "x2": 21053,
        "y2": 46669,
        "z2": 4136,
    },
    {
        "unit_name": "熔融结晶框架",
        "zone_name": "熔融结晶框架南侧罐区",
        "x1": -3099,
        "y1": 33453,
        "z1": -439,
        "x2": 24796,
        "y2": 41862,
        "z2": 14202,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北侧塔器",
        "x1": -9453,
        "y1": 20971,
        "z1": -951,
        "x2": 81178,
        "y2": 39893,
        "z2": 66789,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架一层",
        "x1": -2969,
        "y1": 10685,
        "z1": -591,
        "x2": 81592,
        "y2": 20966,
        "z2": 3762,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架二层",
        "x1": -2589,
        "y1": 10705,
        "z1": 3760,
        "x2": 79660,
        "y2": 20986,
        "z2": 6403,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架三层",
        "x1": -2274,
        "y1": 10711,
        "z1": 6390,
        "x2": 79975,
        "y2": 20993,
        "z2": 9877,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架四层",
        "x1": -1555,
        "y1": 10740,
        "z1": 9842,
        "x2": 75720,
        "y2": 21021,
        "z2": 15913,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架五层",
        "x1": 145,
        "y1": 10742,
        "z1": 15882,
        "x2": 75307,
        "y2": 21023,
        "z2": 21468,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架六层",
        "x1": -29,
        "y1": 10740,
        "z1": 21456,
        "x2": 74098,
        "y2": 21021,
        "z2": 27402,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架七层",
        "x1": 177,
        "y1": 7164,
        "z1": 27297,
        "x2": 75338,
        "y2": 21015,
        "z2": 33368,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架北框架八层",
        "x1": 2335,
        "y1": 7733,
        "z1": 33340,
        "x2": 74435,
        "y2": 21112,
        "z2": 64941,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架南框架一层",
        "x1": -1632,
        "y1": -1656,
        "z1": -15,
        "x2": 31771,
        "y2": 10741,
        "z2": 5890,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架南框架二层",
        "x1": -2839,
        "y1": -1625,
        "z1": 5840,
        "x2": 31743,
        "y2": 10772,
        "z2": 10981,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架南框架三层",
        "x1": -2840,
        "y1": -2378,
        "z1": 10952,
        "x2": 31742,
        "y2": 10817,
        "z2": 18121,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架南框架四层",
        "x1": -1590,
        "y1": -2325,
        "z1": 17995,
        "x2": 30011,
        "y2": 10779,
        "z2": 27390,
    },
    {
        "unit_name": "丙交酯框架",
        "zone_name": "丙交酯框架丙交酯车间",
        "x1": 31782,
        "y1": -1769,
        "z1": -290,
        "x2": 73771,
        "y2": 10715,
        "z2": 27605,
    },
    {
        "unit_name": "丙交酯车间",
        "zone_name": "丙交酯车间",
        "x1": -9453,
        "y1": -2378,
        "z1": -15,
        "x2": 81592,
        "y2": 39893,
        "z2": 66789,
    },
]

# 为未显式配置的工区补充默认网格参数
for definition in ZONE_DEFINITIONS:
    definition.setdefault("grid_enabled", True)
    definition.setdefault("grid_size", DEFAULT_GRID_SIZE)
    definition.setdefault("max_workers_per_grid", DEFAULT_MAX_WORKERS_PER_GRID)


def create_default_zones() -> List[Zone]:
    """基于预设配置创建默认工区"""
    zones: List[Zone] = []
    for definition in ZONE_DEFINITIONS:
        zones.append(
            Zone(
                zone_name=definition["zone_name"],
                unit_name=definition["unit_name"],
                x1=definition["x1"],
                y1=definition["y1"],
                z1=definition["z1"],
                x2=definition["x2"],
                y2=definition["y2"],
                z2=definition["z2"],
                grid_enabled=definition.get("grid_enabled", True),
                grid_size=definition["grid_size"],
                max_workers_per_grid=definition["max_workers_per_grid"],
            )
        )
    return zones


@dataclass
class PipelineSegment:
    """
    管线段模型（按工区分段后的管线）
    """
    segment_id: str            # 段ID，格式："原管线号__工区名"
    original_pipeline_no: str  # 原始管线号
    zone_name: str             # 所属工区
    unit_name: str             # 所属单元
    package_no: str            # 所属试压包
    weld_points: List[WeldPoint] = field(default_factory=list)  # 该段包含的焊口
    grid_ids: set = field(default_factory=set)  # 该段涉及的网格ID集合 (zone, x, y, z)
    
    @property
    def total_inches(self) -> float:
        """该段总寸径"""
        return sum(wp.diameter for wp in self.weld_points)
    
    @property
    def weld_count(self) -> int:
        """焊口数量"""
        return len(self.weld_points)
    
    def __repr__(self):
        return f"PipelineSegment({self.segment_id}, {self.total_inches:.2f}寸, {self.weld_count}个焊口)"
    
    def __hash__(self):
        return hash(self.segment_id)
    
    def __eq__(self, other):
        if isinstance(other, PipelineSegment):
            return self.segment_id == other.segment_id
        return False

@dataclass
class TopologySegment:
    """
    拓扑管线段模型（按拓扑结构分段后的管线）
    每条管线生成2个段：主管线段 + 支管线段（支管合集）
    """
    segment_id: str                    # 段ID，格式："原管线号__MAIN" 或 "原管线号__BRANCH"
    original_pipeline_no: str          # 原始管线号
    segment_type: str                  # "main" 或 "branch"
    weld_sequence: List[str]           # 焊接顺序（拓扑排序后的焊口号列表）
    weld_points: List[WeldPoint] = field(default_factory=list)  # 焊口对象列表
    unit_name: str = None              # 所属单元
    package_no: str = None             # 所属试压包
    
    @property
    def diameter(self) -> float:
        """段的管径 = 最大焊口管径"""
        return max(wp.diameter for wp in self.weld_points) if self.weld_points else 0.0
    
    @property
    def total_inches(self) -> float:
        """该段总寸径"""
        return sum(wp.diameter for wp in self.weld_points)
    
    @property
    def weld_count(self) -> int:
        """焊口数量"""
        return len(self.weld_points)
    
    @property 
    def is_empty(self) -> bool:
        """支管线段可能为空"""
        return len(self.weld_points) == 0
    
    @property
    def can_parallel_weld(self) -> bool:
        """是否允许多焊工并行焊接（管径>600）"""
        return self.diameter > 600.0
    
    def __repr__(self):
        return f"TopologySegment({self.segment_id}, {self.segment_type}, " \
               f"{self.diameter:.0f}mm, {self.weld_count}个焊口)"
    
    def __hash__(self):
        return hash(self.segment_id)
    
    def __eq__(self, other):
        if isinstance(other, TopologySegment):
            return self.segment_id == other.segment_id
        return False


class ProjectData:
    """
    项目数据容器（整个项目的数据）
    """
    def __init__(self):
        self.weld_points: List[WeldPoint] = []      # 所有焊口
        self.pipelines: List[Pipeline] = []          # 所有管线
        self.packages: List[Package] = []            # 所有试压包
        # 新增：空间相关
        self.zones: List[Zone] = []                  # 所有工区
        self.blocks: List[Block] = []                # 所有块
        self.segments: List[PipelineSegment] = []    # 所有管线段（按工区分段）
        self.topology_segments: List[TopologySegment] = []  # 新增：拓扑管线段（按拓扑分段）
        
        # 索引（快速查找）
        self._pipeline_dict: Dict[str, Pipeline] = {}
        self._package_dict: Dict[str, Package] = {}
        self._zone_dict: Dict[str, Zone] = {}
        self._block_dict: Dict[str, Block] = {}
        self._segment_dict: Dict[str, PipelineSegment] = {}
        self._topology_segment_dict: Dict[str, TopologySegment] = {}  # 新增：拓扑段索引
    
    def add_weld_point(self, weld_point: WeldPoint):
        """添加焊口"""
        self.weld_points.append(weld_point)
    
    def add_pipeline(self, pipeline: Pipeline):
        """添加管线"""
        self.pipelines.append(pipeline)
        self._pipeline_dict[pipeline.pipeline_no] = pipeline
    
    def add_package(self, package: Package):
        """添加试压包"""
        self.packages.append(package)
        self._package_dict[package.package_no] = package
    
    def get_pipeline(self, pipeline_no: str) -> Optional[Pipeline]:
        """根据管线号获取管线对象"""
        return self._pipeline_dict.get(pipeline_no)
    
    def get_package(self, package_no: str) -> Optional[Package]:
        """根据试压包号获取试压包对象"""
        return self._package_dict.get(package_no)
    
    def add_zone(self, zone: Zone):
        """添加工区"""
        self.zones.append(zone)
        self._zone_dict[zone.zone_name] = zone
    
    def add_block(self, block: Block):
        """添加块"""
        self.blocks.append(block)
        self._block_dict[block.block_id] = block
    
    def add_segment(self, segment: PipelineSegment):
        """添加管线段"""
        self.segments.append(segment)
        self._segment_dict[segment.segment_id] = segment
    
    def get_zone(self, zone_name: str) -> Optional[Zone]:
        """根据工区名称获取工区对象"""
        return self._zone_dict.get(zone_name)
    
    def get_block(self, block_id: str) -> Optional[Block]:
        """根据块ID获取块对象"""
        return self._block_dict.get(block_id)
    
    def get_segment(self, segment_id: str) -> Optional[PipelineSegment]:
        """根据段ID获取管线段对象"""
        return self._segment_dict.get(segment_id)
    
    def get_pipeline_ids(self) -> List[str]:
        """获取所有管线号列表"""
        return [p.pipeline_no for p in self.pipelines]
    
    def get_segment_ids(self) -> List[str]:
        """获取所有管线段ID列表"""
        return [s.segment_id for s in self.segments]
    
    def add_topology_segment(self, topology_segment: TopologySegment):
        """添加拓扑管线段"""
        self.topology_segments.append(topology_segment)
        self._topology_segment_dict[topology_segment.segment_id] = topology_segment
    
    def get_topology_segment(self, segment_id: str) -> Optional[TopologySegment]:
        """根据段ID获取拓扑管线段对象"""
        return self._topology_segment_dict.get(segment_id)
    
    def get_topology_segment_ids(self) -> List[str]:
        """获取所有拓扑管线段ID列表"""
        return [s.segment_id for s in self.topology_segments if not s.is_empty]
    
    @property
    def total_inches(self) -> float:
        """项目总寸径"""
        return sum(p.total_inches for p in self.pipelines)
    
    @property
    def pipeline_count(self) -> int:
        """管线总数"""
        return len(self.pipelines)
    
    @property
    def package_count(self) -> int:
        """试压包总数"""
        return len(self.packages)
    
    def get_statistics(self) -> Dict:
        """获取项目统计信息"""
        return {
            '管线总数': self.pipeline_count,
            '试压包总数': self.package_count,
            '焊口总数': len(self.weld_points),
            '总寸径': round(self.total_inches, 2),
            '平均寸径/管线': round(self.total_inches / self.pipeline_count, 2) if self.pipeline_count > 0 else 0,
            '平均管线数/试压包': round(self.pipeline_count / self.package_count, 2) if self.package_count > 0 else 0
        }
    
    def __repr__(self):
        return f"ProjectData(管线={self.pipeline_count}, 试压包={self.package_count}, 总寸径={self.total_inches:.2f})"

