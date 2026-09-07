"""Context Selection 历史 Read Set 的纯领域校验与模式派生策略。"""

from __future__ import annotations

from app.modules.context.domain.enums import ContextSelectionMode
from app.modules.context.domain.models import (
    ContextChain,
    ContextSelectionDecision,
)


def derive_context_selection_mode(
    relevant_chain_ids: list[str],
) -> ContextSelectionMode:
    """由读取集合（Read Set）大小确定稳定的上下文选择模式。

    不把冗余复杂的分类交由模型自行生成，而是由系统确定性派生。

    Args:
        relevant_chain_ids: 命中的上下文链 ID 列表。

    Returns:
        NO_CONTEXT (0条), SINGLE_CONTEXT (1条), MULTI_CONTEXT (>=2条)。
    """
    count = len(relevant_chain_ids)
    if count == 0:
        return ContextSelectionMode.NO_CONTEXT
    if count == 1:
        return ContextSelectionMode.SINGLE_CONTEXT
    return ContextSelectionMode.MULTI_CONTEXT


def validate_context_selection(
    decision: ContextSelectionDecision,
    chains: list[ContextChain],
    *,
    conversation_id: str,
) -> ContextSelectionDecision:
    """校验并规范化模型返回的上下文选择决策。

    校验规则：
    1. 确保候选链列表中的所有链均属于当前会话且无重复。
    2. 校验模型选中的每个 Chain ID 均真实存在于候选链列表中。
    3. 校验模型选中的 Chain 未被归档（archived=False）。
    4. 对选中的 Chain ID 进行稳定保序去重。

    Args:
        decision: Context Agent 产出的原始决策对象。
        chains: 传入 Context Agent 的所有候选未归档上下文链列表。
        conversation_id: 当前会话 ID。

    Returns:
        规范化去重后的决策对象。

    Raises:
        ValueError: 候选链不属于当前会话、存在重复链、选中了未知链或已归档链。
    """
    # 1. 建立候选链映射并校验会话归属与唯一性
    chain_map: dict[str, ContextChain] = {}
    for chain in chains:
        if chain.conversation_id != conversation_id:
            raise ValueError(
                "Context chain belongs to another conversation: "
                f"{chain.chain_id}"
            )
        if chain.chain_id in chain_map:
            raise ValueError(f"Duplicate context chain: {chain.chain_id}")
        chain_map[chain.chain_id] = chain

    # 2. 稳定保序去重
    selected_ids = list(dict.fromkeys(decision.relevant_chain_ids))

    # 3. 校验选中链的存在性与非归档状态
    for chain_id in selected_ids:
        chain = chain_map.get(chain_id)
        if chain is None:
            raise ValueError(
                f"Context Agent selected unknown chain: {chain_id}"
            )
        if chain.archived:
            raise ValueError(
                f"Context Agent selected archived chain: {chain_id}"
            )

    return decision.model_copy(
        update={"relevant_chain_ids": selected_ids}
    )
