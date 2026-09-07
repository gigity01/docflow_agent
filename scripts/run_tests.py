"""在隔离配置下发现全部 unittest，避免依赖开发者的本地密钥和数据库。"""

import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    os.environ.update({
        "SQLALCHEMY_DATABASE_URL": "sqlite+pysqlite:///:memory:",
        "DASHSCOPE_API_KEY": "unit-test-placeholder",
        "DEEPSEEK_API_KEY": "",
    })
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.testsRun == 0:
        raise SystemExit("未发现任何测试")
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
