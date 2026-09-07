"""DeepSeek strict Tool JSON Schema 兼容转换适配器。"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.modules.context.domain.models import ContextSelectionDecision


class ContextAgentOutputError(RuntimeError):
    """DeepSeek 未按约定提交唯一且合法的 Context Selection 时抛出。"""


def resolve_local_schema_refs(
    value: Any,
    definitions: dict[str, Any],
) -> Any:
    """递归展开 Pydantic 生成的本地 ``$defs`` 引用（避免模型工具参数 schema 解析失败）。

    Args:
        value: 原始 Schema 节点（字典、列表或基本类型）。
        definitions: 包含定义字典的 $defs 集合。

    Returns:
        展开 $ref 引用后的 Schema 节点。

    Raises:
        ContextAgentOutputError: 引用了不存在的 definition 名称时抛出。
    """
    if isinstance(value, list):
        return [
            resolve_local_schema_refs(item, definitions)
            for item in value
        ]
    if not isinstance(value, dict):
        return value

    reference = value.get("$ref")
    if isinstance(reference, str) and reference.startswith("#/$defs/"):
        definition_name = reference.removeprefix("#/$defs/")
        definition = definitions.get(definition_name)
        if definition is None:
            raise ContextAgentOutputError(
                f"Context Selection Schema 引用了未知定义: {definition_name}"
            )
        merged = deepcopy(definition)
        merged.update(
            {
                key: item
                for key, item in value.items()
                if key != "$ref"
            }
        )
        return resolve_local_schema_refs(merged, definitions)

    return {
        key: resolve_local_schema_refs(item, definitions)
        for key, item in value.items()
        if key != "$defs"
    }


def normalize_strict_tool_schema(value: Any) -> Any:
    """转换为 DeepSeek strict tool 当前支持的标准 JSON Schema 子集。

    转换规则：
    1. 移除 DeepSeek strict 模式不支持或仅用于展示的约束关键字（如 title, default, minLength, maxLength 等）。
    2. 将 object 类型的 additionalProperties 固定设为 False。
    3. 将 object 类型的 properties 键全量加入 required 必填字段数组。

    Args:
        value: 原始或已展开 $ref 的 Schema 节点。

    Returns:
        归一化后的 Schema 节点。
    """
    if isinstance(value, list):
        return [normalize_strict_tool_schema(item) for item in value]
    if not isinstance(value, dict):
        return value

    unsupported_or_display_only = {
        "title",
        "default",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
    }
    normalized = {
        key: normalize_strict_tool_schema(item)
        for key, item in value.items()
        if key not in unsupported_or_display_only
    }
    if normalized.get("type") == "object":
        properties = normalized.get("properties", {})
        if isinstance(properties, dict):
            normalized["required"] = list(properties)
        normalized["additionalProperties"] = False
    return normalized


def build_context_selection_tool_schema() -> dict[str, Any]:
    """从 ContextSelectionDecision Pydantic 契约生成 DeepSeek strict tool 参数 Schema。

    Returns:
        符合 DeepSeek strict 规范的 JSON Schema 字典。

    Raises:
        ContextAgentOutputError: 生成结果不是有效 JSON 对象时抛出。
    """
    raw_schema = ContextSelectionDecision.model_json_schema()
    definitions = raw_schema.get("$defs", {})
    resolved = resolve_local_schema_refs(raw_schema, definitions)
    normalized = normalize_strict_tool_schema(resolved)
    if not isinstance(normalized, dict):
        raise ContextAgentOutputError("Context Selection Schema 不是 JSON 对象")
    return normalized
