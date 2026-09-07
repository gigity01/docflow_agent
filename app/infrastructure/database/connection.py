"""MySQL 会话与应用使用相同的 UTC 时间基准。"""

from sqlalchemy.engine import make_url


def connection_options(url: str) -> dict:
    if make_url(url).get_backend_name() == "mysql":
        return {"init_command": "SET time_zone = '+00:00'"}
    return {}
