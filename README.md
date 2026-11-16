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
