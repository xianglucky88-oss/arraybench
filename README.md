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
- 跨数据集观测对齐：显式快照、半开窗口、维度双射映射与微秒容差的一对一精确匹配，返回配对身份/时间差与未配对原因，资源超限明确失败。

## 运行

Python 3.11+，macOS/Linux。项目目录执行：

```bash
export PYTHONPATH="$PWD/src"
python3 -m arraybench.cli --root /tmp/arraybench-demo init examples/schema.json
python3 -m arraybench.cli --root /tmp/arraybench-demo append examples/measurements.csv
python3 -m arraybench.cli --root /tmp/arraybench-demo window 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z
python3 -m arraybench.cli --root /tmp/arraybench-demo stats 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z
python3 -m arraybench.cli --root /tmp/arraybench-demo resample 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z --seconds 3600
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
| `align.py` | 对齐计划校验、分组调度与精确求解 | 新调度策略 |
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

## 观测对齐（align）

比较两台设备的稀疏观测：同一坐标下两边测量时间并不相同，直接重采样会把不存在的配对变成均值。`align` 在既有快照读取接口（`snapshot`/`window`）之上建立独立的观测对齐关系，只读不写：不追加数据、不插值、不重新解释单位、不改写坐标身份，也不把数据扩成稠密时间×坐标网格。

计划（JSON）一次性固定所有输入：

```json
{
  "left":  {"root": "/data/a", "version": "<snapshot-sha>"},
  "right": {"root": "/data/b", "version": "<snapshot-sha>"},
  "window": {"start": "2026-11-01T00:00:00Z", "end": "2026-11-02T00:00:00Z"},
  "dimensions": {"station": "site"},
  "tolerance_micros": 3000000,
  "strategy": "memory",
  "limits": {"max_resident_rows": 100000, "max_temp_bytes": 16777216, "max_group_cells": 1000000}
}
```

- 两侧必须显式固定 snapshot 版本，HEAD 永不作为默认值；计划建立后 HEAD 再前进也不会混入新行。两边只承诺各自固定版本，不虚构跨数据集同时刻事务。
- `dimensions` 是左→右维度名的双射：校验阶段读取两边快照摘要，要求映射恰好覆盖全部维度且类型一一对应；未映射的维度直接报错，不会被悄悄合并。坐标值逐字复制，只按映射改名字。
- 窗口为半开 `[start, end)`：等于 `end` 的行不参与；`tolerance_micros` 是非负整数，时间差恰等于容差可匹配；null 测量仍是可配对观测。

每个完整坐标组内独立精确求解（不同组不借用彼此的行）：一对一、按各自时间顺序不交叉；目标按优先级为 **配对数最多 → 总绝对时间差最小 → 配对序列（左右时间与稳定行身份）字典序最小**。例如左侧 4s、6s，右侧 1s、5s，容差 3s：逐行取最近只会得到 1 对，正确结果是 4→1、6→5 两对。求解器是 LCS 型动态规划（交叉匹配总能不损失地解交叉），绝不退化为贪心。

结果中每对给出左右行身份（数据集/snapshot/坐标/时间，可经 `window` 回查，与物理块排列无关）和有符号 `dt_micros`（右−左）；未配对行附原因：`peer_group_missing`（对侧坐标组不存在）、`no_candidate_within_tolerance`（容差内无候选）、`candidate_claimed_by_better_match`（候选被全局更优匹配占用）。

### 调度策略与资源上限

计划必须声明三项上限：驻留记录 `max_resident_rows`、临时字节 `max_temp_bytes`、单组求解状态 `max_group_cells`。超过任一项抛出 `AlignmentLimitError` 明确失败，绝不退化为贪心近似却仍返回成功。

| 策略 | 驻留记录 | 临时字节 | 说明 |
|---|---|---|---|
| `memory`（默认） | O(窗口总行数) | 0 | 一趟读入后逐组求解；窗口可入内存时最省 |
| `staging` | O(组数+单组行数) | O(窗口总行数) | 流式写入按组分片的临时文件（系统临时目录或 `temp_dir`，在数据集根之外），再逐组读回 |
| 外部排序（未采用） | O(单组行数) | O(N) | 多 O(N log N) 比较；精确求解器已要求单组状态有界，按组暂存对稀疏数据严格占优 |

两种策略调用同一个求解器，结果完全一致。临时文件运行后清理。

```bash
python3 -m arraybench.cli align plan.json
python3 examples/align_demo.py
```

## 一致性、内存和边界

读取只使用一个已固定快照，写入持有数据集进程锁；失败批次不会移动 HEAD，但可能留下可安全忽略的未引用块。回滚创建新修订，旧快照始终可读。源数据不需要转成稠密笛卡尔数组。

内存按输入批次和时间桶的**行数**限制，不是精确字节配额；单个超大字段仍可占用较多内存。追加载入受影响桶并重新分块，未变化桶共享旧对象；窗口逐块读取；重采样按当前时间窗聚合，空间分组数也受限。CLI 为输出 JSON 会物化查询结果，超大导出应直接使用领域生成器或新增流式写出。

采用 JSON 块而非 Zarr/Arrow，数值为 Python 浮点，不承诺十进制精度。单位扩展需每个进程一致配置。暂不包含维度索引、schema 演进、压缩、外部对象存储、块垃圾回收或 Windows 锁适配。
