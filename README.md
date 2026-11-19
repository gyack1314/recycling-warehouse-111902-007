# 回收日仓的依赖感知补算编排

本项目提供合成回收业务原件及物理检查入口。当前没有入仓、仓库发布或产品查询能力；检查通过只表示输入能读取，不表示动作已经处理。

使用 Python 3.11，依赖版本固定在 requirements.txt。安装命令为 python -m pip install -r requirements.txt。运行 python -m materials inputs 校验清单、摘要、Parquet行数并实际使用DuckDB读取；运行 python -m unittest discover -s tests -v 检查独立JSON值、原件损坏及业务材料形状。

可执行 docker build -t recycling-inputs .，再执行 docker run --rm recycling-inputs；测试命令为 docker run --rm recycling-inputs python -m unittest discover -s tests -v。环境不依赖外部业务站点。资料均为本地合成，不含真实个人、企业数据或凭据。

inputs/seed.json与同名Parquet是相同输入的两种载体；inputs/actions.json提供尚未应用的后续资料。docs/input-contract.md解释字段及原有业务约定。materials.physical只校验物理文件，不裁决身份、金额、匹配或有效版本。materials.build_inputs仅重建输入Parquet和物理清单，通常无需运行；不要把其产物误当未来业务结果。

产品输出使用独立目录；原始资料保留供回查。入仓、补算编排与固定版本查询已在 `warehouse` 包实现，
使用方式与验收口径见 [docs/warehouse.md](docs/warehouse.md)：`python3 -I scripts/wh.py` 提供
登记修订、影响分析、补算/重试与快照领取入口；`python3 -I scripts/run_tests.py` 运行全部测试，
`scripts/acceptance_demo.py` 与 `scripts/race_demo.py` 分别演示完整补算时间线与双调度进程竞争。
已有输入检查不替代产品验收。
