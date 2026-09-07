"""文档模块本地文件路径校验、白名单扩展名过滤与 SHA-256 流式哈希计算工具。"""

import hashlib
from pathlib import Path

from fastapi import HTTPException, UploadFile

from app.config.settings import (
    ALLOWED_CONTENT_TYPES,
    ALLOWED_EXTENSIONS,
)


def validate_filename(filename: str) -> None:
    """校验上传文件名合法性，严格防御路径穿越与空文件名。

    Args:
        filename: 待检查的文件名字符串。

    Raises:
        HTTPException: 空文件名或包含 '..', '/', '\\', '\\x00' 危险字符时抛出 400。
    """
    if not filename:
        raise HTTPException(status_code=400, detail="文件名不能为空")

    dangerous_chars = ["..", "/", "\\", "\x00"]

    if any(char in filename for char in dangerous_chars):
        raise HTTPException(
            status_code=400,
            detail="文件名包含非法字符"
        )


def get_safe_extension(filename: str) -> str:
    """校验文件名并提取规范化的小写扩展名（校验 ALLOWED_EXTENSIONS 白名单）。

    Args:
        filename: 文件名字符串。

    Returns:
        不含前导点号的小写扩展名（如 'pdf', 'docx', 'txt', 'csv'）。

    Raises:
        HTTPException: 文件名无扩展名或扩展名不在允许白名单中抛出 400。
    """
    validate_filename(filename)

    if "." not in filename:
        raise HTTPException(status_code=400, detail="文件必须包含扩展名")

    ext = filename.rsplit(".", 1)[-1].lower().strip()

    if f".{ext}" not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的文件类型：{ext}"
        )

    return ext


def validate_content_type(file: UploadFile) -> None:
    """校验客户端声明的 Content-Type 是否在允许的白名单范围内。

    Args:
        file: FastAPI 上传文件对象。

    Raises:
        HTTPException: Content-Type 不在白名单时抛出 400。
    """
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的 Content-Type: {file.content_type}"
        )


def calculate_file_hash(file_path: Path) -> str:
    """以 1 MiB 分块流式读取并计算本地文件的 SHA-256 16进制摘要。

    Args:
        file_path: 本地文件绝对路径。

    Returns:
        64 位小写 SHA-256 哈希十六进制字符串。
    """
    sha256 = hashlib.sha256()

    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def cleanup_file(path: Path) -> bool:
    """尽力删除指定的本地文件（忽略文件不存在异常）。

    Args:
        path: 待删除的文件路径。

    Returns:
        删除成功返回 True，不存在返回 False。
    """
    try:
        path.unlink(missing_ok=True)
        return True
    except FileNotFoundError:
        return False
