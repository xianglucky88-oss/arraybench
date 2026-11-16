# ArrayBench · 多维测量归一化与分块快照

处理时间、空间坐标和多个测量变量构成的稀疏数组，将异构单位归一化后写入可局部更新的不可变块，并提供历史窗口、重采样与统计。

## 已实现

- CSV、JSON 数组和 JSONL 输入；时间必须带时区并归一为 UTC 微秒。
- 字符串/数值空间维度；变量契约、输出单位和物理范围。
- 可扩展仿射单位表，内置摄氏/华氏/开尔文、压力和长度单位；不同量纲直接拒绝。
- 缺失值保留为 null；无效数值可拒绝或置空并标记，量纲错误不会被静默置空。
- 时间桶内分块、SHA256 内容寻址、写时复制快照和原子 HEAD 指针。
- 重复坐标/时间的 `error/update/ignore`，批次和桶行数上限，进程文件锁及可选版本冲突检查。
- 面向超大逻辑批次的流式追加：一次性消费生成器、磁盘暂存、按桶外排归并，准备阶段不占写锁。
- `[start,end)` 窗口、空间维度等值筛选、按时间区间和空间坐标重采样。
- Welford 均值/总体方差、缺失计数、最小/最大值、可回滚且不改动历史块。

## 运行

Python 3.11+，macOS/Linux。项目目录执行：

```bash
export PYTHONPATH="$PWD/src"
python3 -m arraybench.cli --root /tmp/arraybench-demo init examples/schema.json
python3 -m arraybench.cli --root /tmp/arraybench-demo append examples/measurements.csv
python3 -m arraybench.cli --root /tmp/arraybench-demo append-stream examples/measurements.csv --duplicates update
python3 -m arraybench.cli --root /tmp/arraybench-demo window 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z
python3 -m arraybench.cli --root /tmp/arraybench-demo stats 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z
python3 -m arraybench.cli --root /tmp/arraybench-demo resample 2026-11-01T00:00:00Z 2026-11-02T00:00:00Z --seconds 3600
python3 -m unittest discover -s tests -v
python3 examples/smoke.py
python3 examples/streaming_bench.py
```

样例包含 3 条测量、2 组空间坐标、不同单位和 1 个温度缺失值。归一化后有效温度均值为 21°C；小时重采样保留 2 组空间坐标。

## 模块与扩展

| 模块 | 职责 | 扩展位置 |
|---|---|---|
| `units.py` | 单位与 UTC 时间 | `UnitRegistry.register` 添加单位 |
| `storage.py` | 不可变对象、锁和 HEAD 提交 | 替换编码器、文件/对象存储适配 |
| `streaming.py` | 磁盘暂存、外排键索引和驻留量统计 | 替换 SQLite 暂存或外部排序后端 |
| `dataset.py` | 归一化、追加、窗口、重采样和统计 | 新聚合器、变量质量策略、坐标索引 |
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

## 流式追加协议

`append(rows)` 保持原行为：逻辑批次超过 `max_batch_rows` 就整体失败，HEAD 不动。超大输入改用流式协议：

```python
prepared = data.prepare_append(rows, duplicates="update", invalid="missing")
# rows 只被消费一次，可跨任意多个桶；此处不持有全局写锁。
prepared.publish()          # 基准快照未变才提交，否则抛 SnapshotConflict
# 或 data.append_stream(rows, ...) 一步完成；CLI 对应 append-stream。
```

准备阶段固定基准 HEAD、schema 和单位表（`UnitRegistry.frozen`），把归一化记录连同原始序号写入 `staging/` 下的临时 SQLite 索引，再逐桶与旧块做外部有序归并，生成新的不可变 chunk。发布只在写锁内比较 HEAD 与基准并替换指针；任何并发提交都会让整个批次 `SnapshotConflict`，不会覆盖新 HEAD，也不会把部分桶拼到新状态上。`prepared.discard()`（或离开 `with` 块）只清理暂存，不发布。

资源上界（`ResidencyProbe` 可在 `PreparedAppend` 上读取）：

| 阶段 | 驻留上限 | 说明 |
|---|---|---|
| 归一化输入缓冲 | `max_batch_rows` 条记录 | 达到上限即整批写入暂存并清空缓冲，从不在别处累积 |
| 单桶归并 | `max_bucket_rows + 2 * chunk_rows` | 同键组只保留一条胜者；旧块逐块读取，取数窗口不超过 `chunk_rows` |
| 最终每桶 | `max_bucket_rows` | 超过即拒绝，不移动 HEAD |
| 生成器 yield | 由调用方决定 | 协议不依赖 yield 大小，拉取一条就归一化一条 |

`error/update/ignore` 的语义与一次性按原输入顺序处理一致：同一桶内先检查重复（包括跨很多块的重复）再检查桶容量；`update` 取该键在批次中最后一条（即使它后来变成 missing），`ignore` 取第一条。先出现的另一种单位表示会先归一化。

磁盘与延迟对比（`examples/streaming_bench.py` 的独立小模型）：

| 方案 | 临时空间 | 提交延迟 | 输入顺序 |
|---|---|---|---|
| 按桶外排 | 每条记录约 2 份（读入 + 排序溢出） | 需看到整批才能定稿最后一桶 | 保留原始序号，可做跨块去重 |
| 临时索引（本实现） | 每条约 2 份（行 + 键索引） | 同上；桶按首次出现顺序归并 | SQLite B-tree 稳定排序 |
| 受控归并 | 约 1 份加 k 个归并游标 | 归并扇入占用内存，仍需整批 | 需显式 run 边界 |

三者都必须在整批暂存之后才能发布，因此“跨桶原子性”的代价是整批延迟而不是内存；差别在写放大和归并内存。

失败与恢复：临时空间不足、生成器中途抛错、最后一行单位不合法、`out` 缓冲未写完、commit 前退出，都不会改变旧 HEAD；未引用 chunk 留在 `chunks/` 中但不会出现在任何快照里，可安全忽略或后续回收。成功提交后未变化的桶继续共享原 chunk 对象，`window/stats/resample` 对历史版本保持原样。

## 一致性、内存和边界

读取只使用一个已固定快照，写入持有数据集进程锁；失败批次不会移动 HEAD，但可能留下可安全忽略的未引用块。回滚创建新修订，旧快照始终可读。源数据不需要转成稠密笛卡尔数组。

内存按输入批次和时间桶的**行数**限制，不是精确字节配额；单个超大字段仍可占用较多内存。追加载入受影响桶并重新分块，未变化桶共享旧对象；窗口逐块读取；重采样按当前时间窗聚合，空间分组数也受限。CLI 为输出 JSON 会物化查询结果，超大导出应直接使用领域生成器或新增流式写出。

暂存使用 SQLite（Python 标准库）而非外部消息队列；`staging/` 需与数据集同盘可写，剩余空间不足时按上文失败语义中止。采用 JSON 块而非 Zarr/Arrow，数值为 Python 浮点，不承诺十进制精度。单位扩展需每个进程一致配置。暂不包含维度索引、schema 演进、压缩、外部对象存储、块垃圾回收或 Windows 锁适配。
