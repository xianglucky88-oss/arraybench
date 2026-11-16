# ArrayBench · 多维测量归一化与分块快照

处理时间、空间坐标和多个测量变量构成的稀疏数组，将异构单位归一化后写入可局部更新的不可变块，并提供历史窗口、重采样与统计。

## 已实现

- CSV、JSON 数组和 JSONL 输入；时间必须带时区并归一为 UTC 微秒。
- 字符串/数值空间维度；变量契约、输出单位和物理范围。
- 可扩展仿射单位表，内置摄氏/华氏/开尔文、压力和长度单位；不同量纲直接拒绝。
- 缺失值保留为 null；无效数值可拒绝或置空并标记，量纲错误不会被静默置空。
- 时间桶内分块、SHA256 内容寻址、写时复制快照和原子 HEAD 指针。
- 重复坐标/时间的 `error/update/ignore`，批次和桶行数上限，进程文件锁及可选版本冲突检查。
- `[start,end)` 窗口、空间维度等值筛选、按时间区间和空间坐标重采样。
- Welford 均值/总体方差、缺失计数、最小/最大值、可回滚且不改动历史块。
- 独立的单变量变点分段（`segment`）：强制缺测/长间隔区块、全局最优分割、显式驻留/暂存/单组求解上限。

## 单变量分段（segment）

`Dataset.segment(...)` / CLI `segment` 是只读分析，仅消费 `window` 的规范化行：不重新解释单位、不补点、不重采样，结果绑定固定快照 ID。按**完整坐标**分组并按 UTC 排序后，对选定变量做两层处理：

1. **区块规划（强制边界，不收变点惩罚）**：该变量的实际 `null` 观测、或相邻观测间隔**严格大于** `max_gap_seconds` 都强制切分；其他变量无效不会误切本变量区块；缺测不是零，强制边界两侧永远不会拼成一段。
2. **全局分割（自主变点，每个收一次 `penalty`）**：在所有满足每段 `min_points` 与 `min_span_seconds` 约束的切分中，最小化“各段相对段均值的平方误差之和 + 惩罚 × 自主变点数”。并列时先选段数少的，再按切分时间序列取字典序最小。整块不满足最少点数或跨度时返回 `status: not_estimable`，不伪造均值。

区间平方误差用**平移后的 Welford 递推**计算（不用 `sum(x²) − sum(x)²/n`，该式在大基线上会相消），误差不会被截零。例如等间隔 `[0,8,9,12,12,12]`、最少两点、零跨度、惩罚 10 时，最优切分在第二个点之后，目标值 `48.75`（第二段均值 `11.25`、误差 `6.75`）。

### 内存边界与替代切分的保留

分段不能在读完整个区块前把局部分割定案，因此保留了两类状态：

- **外部按组暂存**：记录先驻留在按坐标分组的缓冲中，超过 `max_resident_records` 时整组溢出到 JSONL 暂存文件，受 `max_staging_bytes` 字节上限约束，最终化时原样读回并排序——输入遍历顺序（bucket/chunk 顺序）不影响结果。总输入大于驻留上限、但各组可解且暂存空间足够时仍然成功。
- **流式区间代价缓存**：区块内每来一个观测就扩展所有开放区间的 Welford 状态，缓存 `n(n+1)/2` 个区间代价；DP 同时保留每个终点的全部非支配前缀（目标值相同但段数不同的前缀对未来扩展仍有区别），区块闭合后才回溯。单区块观测数超过 `max_group_observations` 时**显式拒绝**，不会偷偷退化成阈值/贪心切分。

输出按组给出每段的原始首末观测身份（UTC 时间 + 完整坐标）、有效点数、均值、平方误差，以及 `edge` / `forced:missing` / `forced:gap` / `autonomous` 边界原因。


## 运行

Python 3.11+，macOS/Linux。项目目录执行：

```bash
export PYTHONPATH="$PWD/src"
python3 -m arraybench.cli --root /tmp/arraybench-demo init examples/schema.json
python3 -m arraybench.cli --root /tmp/arraybench-demo append examples/measurements.csv
python3 -m arraybench.cli --root /tmp/arraybench-demo window 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z
python3 -m arraybench.cli --root /tmp/arraybench-demo stats 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z
python3 -m arraybench.cli --root /tmp/arraybench-demo resample 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z --seconds 3600
python3 -m arraybench.cli --root /tmp/arraybench-demo segment 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z --variable temperature --penalty 10 --min-points 2 --min-span-seconds 0 --max-gap-seconds 7200
python3 -m unittest discover -s tests -v
python3 examples/smoke.py
```

样例包含 3 条测量、2 组空间坐标、不同单位和 1 个温度缺失值。归一化后有效温度均值为 21°C；小时重采样保留 2 组空间坐标。

## 模块与扩展

| 模块 | 职责 | 扩展位置 |
|---|---|---|
| `units.py` | 单位与 UTC 时间 | `UnitRegistry.register` 添加单位 |
| `storage.py` | 不可变对象、锁和 HEAD 提交 | 替换编码器、文件/对象存储适配 |
| `dataset.py` | 归一化、追加、窗口、重采样和统计 | 新聚合器、变量质量策略、坐标索引 |
| `segmentation.py` | 单变量缺测区块规划、区间代价缓存和全局变点 DP | 新的区块边界规则或分段目标函数 |
| `cli.py` | CSV/JSON/JSONL 输入和命令 | 增加独立格式读取适配器 |

```python
from arraybench import Dataset
from arraybench.units import UnitRegistry

units = UnitRegistry()
units.register("mm", "length", 0.001)
data = Dataset("measurements", registry=units)
# 已创建数据集时可读取当前/历史窗口；版本 ID 来自 append。
version = data.store.head()
for row in data.window("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z",
                       coordinates={"station": "east"}, version=version):
    print(row)
```

## 一致性、内存和边界

读取只使用一个已固定快照，写入持有数据集进程锁；失败批次不会移动 HEAD，但可能留下可安全忽略的未引用块。回滚创建新修订，旧快照始终可读。源数据不需要转成稠密笛卡尔数组。

内存按输入批次和时间桶的**行数**限制，不是精确字节配额；单个超大字段仍可占用较多内存。追加载入受影响桶并重新分块，未变化桶共享旧对象；窗口逐块读取；重采样按当前时间窗聚合，空间分组数也受限。CLI 为输出 JSON 会物化查询结果，超大导出应直接使用领域生成器或新增流式写出。

采用 JSON 块而非 Zarr/Arrow，数值为 Python 浮点，不承诺十进制精度。单位扩展需每个进程一致配置。暂不包含维度索引、schema 演进、压缩、外部对象存储、块垃圾回收或 Windows 锁适配。
