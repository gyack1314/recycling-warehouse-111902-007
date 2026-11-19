# 依赖感知的回收日仓补算编排

在 `materials` 提供的合成原件与 `inputs/jobs.json` 的 SQL 作业定义之上，实现日仓作业编排：
把真实 Parquet 分区组织成带修订版本的仓库，由 DuckDB 实际执行 SQL，并以不可变产物与
固定版本快照保证每次读取都指向一套自洽结果。

## 解决的问题

- 补一天回收明细，次日库存与七日周转需要沿依赖重算，但不能整表重算无关日期；
- 旧补算仍在执行时原件被修订，旧作业最后完成也不能覆盖新结果；
- 零行产物（明确清空）与缺分区分别对待，缺必要前置时指出原因而不是默认补零；
- 失败补算期间，使用者仍可查询上一套自洽结果，不会把新明细、旧库存、半截周转拼在一次读取中；
- 两个调度进程可对同一 run 竞争领取，独立作业可失败重试。

## 目录布局（仓库根，默认 `var/warehouse`，可用 `-w` 覆盖）

```
raw/<table>/dt=<day>/rev=<n>/data.parquet   不可变原件分区（零行也有 parquet 证据）
artifacts/<sha 前缀>/<sha>.parquet          内容寻址的作业产物，永不覆写
attempts/<attempt_id>/                      每次尝试的 input_manifest、output、错误/stale 证据
snapshots/<snapshot_id>/manifest.json       固定版本清单（条目精确到产物 sha 与输入指纹）
snapshots/<snapshot_id>/warehouse.duckdb    绑定不可变路径的查询库（daily/balance/turnover 视图）
meta/catalog.sqlite                         证据目录：修订、尝试、产物、运行项、快照
meta/catalog.lock                          规划动作的跨进程互斥锁
```

## 依赖模型（按业务日期偏移展开，不按表名排序）

- `daily@d` 读 `raw_day@d`（同日，offset 0）；
- `balance@d` 读 `daily@d` 与 `balance@d-1`（前一日余额，offset -1）；
  `balance@2026-10-01` 的前一日由明确给定的 `opening@2026-09-30` 锚点满足，不向更早递归；
- `turnover@d` 读 `daily@d-6..d` 七个日分区与当日 `balance@d`。

影响分析沿反向边按日期传播：`raw_day@x` 的修订只影响 `x` 当天及之后真正引用它的
作业/日期；区间内未受影响且输入指纹仍匹配的 current 产物继续可用（运行项标 `skipped`）。

## 三个可分辨的状态（验收口径）

| 状态 | 证据位置 | 含义 |
| --- | --- | --- |
| 已经算完且当前可用 | `products.is_current=1`，运行项 `committed` | 输入指纹与当前目录一致 |
| 因输入变动而过期 | `attempts.status='stale'` 或旧产物 `is_current=0` | 证据保留可核查，但不发布、不冒充当前补算 |
| 可以一起用于查询 | `snapshots` 的 manifest，全部条目通过新鲜度复核 | 整套自洽，DuckDB 视图绑定不可变 parquet |

尝试在发布前重新解析全部输入并比对指纹：执行期间输入变化即标记 `stale`，
旧成功标志不会授权新输入；重试是新的尝试、新的指纹。

## 四个入口（`python3 -I scripts/wh.py ...`）

```bash
# 1. 登记修订：原件入仓 / 行级替换 / 明确清空 / 批量应用 actions.json
python3 -I scripts/wh.py -w <仓库> register-parquet --table raw_day --file inputs/raw_day.parquet
python3 -I scripts/wh.py -w <仓库> register-rows --table raw_day --day 2026-10-01 \
        --rows-json '[{"day":"2026-10-01",...}]' --schema inputs/raw_day.parquet
python3 -I scripts/wh.py -w <仓库> clear --table raw_day --day 2026-10-02 --schema inputs/raw_day.parquet
python3 -I scripts/wh.py -w <仓库> apply-actions --actions inputs/actions.json --schema inputs/raw_day.parquet

# 2. 查看影响范围（不执行计算）
python3 -I scripts/wh.py -w <仓库> impact --through 2026-10-03 --table-day raw_day:2026-10-01

# 3. 运行补算 / 独立重试 / 双进程竞争
python3 -I scripts/wh.py -w <仓库> run --from 2026-10-01 --to 2026-10-03
python3 -I scripts/wh.py -w <仓库> worker --run-id <run> --worker-id worker-A   # 第二个进程换 worker-B
python3 -I scripts/wh.py -w <仓库> retry --run-id <run> --job balance --day 2026-10-02

# 4. 领取固定版本仓库结果
python3 -I scripts/wh.py -w <仓库> claim --from 2026-10-01 --to 2026-10-03
python3 -I scripts/wh.py -w <仓库> fallback            # 上一套自洽快照
python3 -I scripts/wh.py -w <仓库> verify-snapshot --snapshot-id <id>
```

`status`、`sources`、`revisions`、`snapshots` 用于查看证据目录；
缺前置的运行项为 `blocked`，`blocker_json` 给出具体缺失的表、日期与偏移原因。

## 并发纪律

- 规划与发布经 `fcntl` 文件锁串行化；运行项领取是单条条件 `UPDATE ... WHERE status IN (...)`，
  两个调度进程只有一个能成功，同一作业/日不会被执行两次；
- 等待同 run 其他 worker 提交前置的节点保持 `pending`（软等待），
  只有前置在仓库中确实不存在才落 `blocked`（硬缺失）；
- 租约超时的 `running` 项自动回到可领取，已落库的尝试证据保留。

## 验收与测试

```bash
python3 -I scripts/run_tests.py        # 全部单元/验收测试（含原有 materials 测试）
python3 -I scripts/race_demo.py <仓库>  # 双调度进程竞争演示
python3 -I scripts/acceptance_demo.py   # actions.json 完整时间线端到端演示
```

所有测试都在临时仓库中真实执行 jobs.json 的 SQL，并实际写出分区、尝试与产物证据，
不存在预填的作业状态表。运行环境与原件处理遵守隔离模式（`python -I`），
脚本位于 `scripts/`，数据路径一律由参数传入。
