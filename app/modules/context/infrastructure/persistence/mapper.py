"""Context ORM Record 构造与领域模型映射。"""

from __future__ import annotations

from typing import Any

from app.modules.context.domain.models import (
    ContextChain,
    ContextChainNodeContext,
    ContextResourceQueue,
    ConversationTurn,
)
from app.modules.context.infrastructure.persistence.models.context_chain import (
    ContextChain as ContextChainModel,
)
from app.modules.context.infrastructure.persistence.models.context_chain_node import (
    ContextChainNode,
)
from app.modules.context.infrastructure.persistence.models.context_resource_event import (
    ContextChainResourceEvent,
)
from app.modules.context.infrastructure.persistence.models.context_selection_record import (
    ContextSelectionRecord,
)
from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn as TurnModel,
)


class SQLAlchemyContextRecordFactory:
    """隔离 Application 与 SQLAlchemy ORM 构造器的实体工厂实现。"""

    def conversation_turn(self, **values: Any) -> TurnModel:
        """创建 ConversationTurn ORM 实体。"""
        return TurnModel(**values)

    def context_selection_record(
        self,
        **values: Any,
    ) -> ContextSelectionRecord:
        """创建 ContextSelectionRecord ORM 实体。"""
        return ContextSelectionRecord(**values)

    def context_chain(self, **values: Any) -> ContextChainModel:
        """创建 ContextChain ORM 实体。"""
        return ContextChainModel(**values)

    def context_chain_node(self, **values: Any) -> ContextChainNode:
        """创建 ContextChainNode ORM 实体。"""
        return ContextChainNode(**values)

    def context_resource_event(
        self,
        **values: Any,
    ) -> ContextChainResourceEvent:
        """创建 ContextChainResourceEvent ORM 实体。"""
        return ContextChainResourceEvent(**values)


def build_context_chain(
    chain: ContextChainModel,
    *,
    resource_queue: ContextResourceQueue,
) -> ContextChain:
    """将持久化 ContextChain ORM 实体投影为完整的领域 ContextChain 模型。

    规则：
    - 忠实保留 Chain 中每个 Node 所引用的 Turn 完整字段与关联资源引用。
    - 注入独立维护的热资源队列（ContextResourceQueue）。

    Args:
        chain: 持久化 ContextChain ORM 实体对象。
        resource_queue: 对应上下文链的热资源队列。

    Returns:
        组装完成的领域模型实例。
    """
    projected_nodes: list[ContextChainNodeContext] = []

    for node in chain.nodes:
        turn = node.turn
        projected_turn = ConversationTurn(
            turn_id=turn.turn_id,
            conversation_id=turn.conversation_id,
            user_input=turn.user_input,
            clarification_input=turn.clarification_input,
            assistant_content=turn.assistant_content,
            assistant_compact=turn.assistant_compact,
            task_ids=list(turn.task_ids or []),
            task_result_summary=turn.task_result_summary,
            status=turn.status,
            created_at=turn.created_at,
            completed_at=turn.completed_at,
        )
        projected_nodes.append(
            ContextChainNodeContext(
                node_id=node.node_id,
                chain_id=node.chain_id,
                turn_id=node.turn_id,
                sequence=node.sequence,
                related_task_ids=list(node.related_task_ids or []),
                related_resource_refs=list(
                    node.related_resource_refs or []
                ),
                created_at=node.created_at,
                turn=projected_turn,
            )
        )

    return ContextChain(
        chain_id=chain.chain_id,
        conversation_id=chain.conversation_id,
        nodes=projected_nodes,
        resource_queue=resource_queue,
        last_active_at=chain.last_active_at,
        archived=chain.archived,
    )
