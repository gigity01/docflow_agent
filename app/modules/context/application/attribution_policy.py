"""Turn 归因（Attribution）的确定性后备策略。"""

from __future__ import annotations

from app.modules.context.application.dto import TurnAttribution


def build_read_set_fallback_attribution(
    relevant_chain_ids: list[str],
    *,
    new_chain_id: str | None = None,
) -> TurnAttribution:
    """根据读取集合（Read Set）构造确定性的 Turn 归因策略。

    规则：
    - 若 relevant_chain_ids 非空，说明存在命中的已有历史链，直接沿用这些链作为写入目标（现有链不可预分配新链 ID）。
    - 若 relevant_chain_ids 为空，说明无历史上下文关联，将当前 Turn 归因至一条新创建的上下文链（create_new_chain=True）。

    Args:
        relevant_chain_ids: Context Selection 选中的历史链 ID 列表（Read Set）。
        new_chain_id: 当需要创建新链时预分配的链 ID（可选）。

    Returns:
        包含目标已有链 ID 列表或新链创建标记的归因对象。

    Raises:
        ValueError: 当已有链存在但同时传入了 new_chain_id 时抛出。
    """
    existing_chain_ids = list(dict.fromkeys(relevant_chain_ids))
    if existing_chain_ids:
        if new_chain_id is not None:
            raise ValueError("Existing attribution cannot preallocate a new chain")
        return TurnAttribution(existing_chain_ids=existing_chain_ids)
    return TurnAttribution(
        create_new_chain=True,
        new_chain_id=new_chain_id,
    )
