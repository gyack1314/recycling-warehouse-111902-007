#!/usr/bin/env python3
"""隔离模式启动器：显式加入仓库根目录后调用 warehouse.cli。

用 `python3 -I scripts/wh.py ...` 运行；-I 忽略环境变量与用户站点包，
项目根在脚本中显式加入，原件路径始终由参数传入。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from warehouse.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
