"""把 Document capability 补偿用例适配为 Task Runtime Port。

包含以下三种确定性补偿适配器：
1. `ProcessDocumentOperationCompensator`:
   - 围栏锁：`document:process:{document_id}` MySQL named-lock。
   - 副作用清理：清理该 `operation_id` 生成的 staging 临时产物目录，将 Document 标记为 failed 并释放 ownership。
2. `BuildDocumentChunksOperationCompensator`:
   - 校验当前 Document 的 `active_operation_id`，将 Document 标记为 failed 并释放 ownership。
3. `IndexDocumentVectorsOperationCompensator`:
   - 围栏锁：`document:index:{document_id}` MySQL named-lock。
   - 副作用清理：从数据库中当前 `indexing` 状态的子块稳定推导 Qdrant Point ID，在锁内执行 Qdrant 向量删除，成功后将子块与文档标记为 failed 并释放 ownership。
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from app.modules.document.application.use_cases.build_chunks import (
    BuildChunksCompensator,
)
from app.modules.document.application.use_cases.index_vectors import (
    IndexVectorsCompensator,
)
from app.modules.document.application.use_cases.process_document import (
    ProcessDocumentCompensator,
)
from app.modules.task_runtime.application.dto import TaskRuntimeContext
from app.modules.task_runtime.application.schemas import (
    BuildDocumentChunksTaskPayload,
    IndexDocumentVectorsTaskPayload,
    ProcessDocumentTaskPayload,
)


class ProcessDocumentOperationCompensator:
    """文档处理（Process）副作用补偿适配器。

    在获取 MySQL `document:process:{document_id}` 围栏锁后校验 ownership，
    清理当前 operation_id 生成的临时 staging 目录并释放文档所有权。
    """

    def __init__(self, compensator: ProcessDocumentCompensator) -> None:
        self._compensator = compensator

    async def compensate(
        self,
        *,
        operation_id: str,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> None:
        """执行文档处理补偿。

        Args:
            operation_id: 本次执行的操作标识 / ownership token。
            payload: ProcessDocumentTaskPayload 输入模型。
            context: 运行时上下文。
        """
        del context
        validated = ProcessDocumentTaskPayload.model_validate(payload)
        await asyncio.to_thread(
            self._compensator.compensate,
            document_id=validated.document_id,
            operation_id=operation_id,
        )


class BuildDocumentChunksOperationCompensator:
    """文档切块（Build Chunks）副作用补偿适配器。

    校验 ownership，将 Document 标记为 failed 并释放 ownership token。
    """

    def __init__(self, compensator: BuildChunksCompensator) -> None:
        self._compensator = compensator

    async def compensate(
        self,
        *,
        operation_id: str,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> None:
        """执行文档切块补偿。

        Args:
            operation_id: 本次执行的操作标识 / ownership token。
            payload: BuildDocumentChunksTaskPayload 输入模型。
            context: 运行时上下文。
        """
        del context
        validated = BuildDocumentChunksTaskPayload.model_validate(payload)
        await asyncio.to_thread(
            self._compensator.compensate,
            document_id=validated.document_id,
            operation_id=operation_id,
        )


class IndexDocumentVectorsOperationCompensator:
    """文档向量索引（Index Vectors）副作用补偿适配器。

    在 `document:index:{document_id}` MySQL 命名锁围栏内校验 ownership，
    从数据库当前 indexing Chunk 的稳定 ID 独立推导 Qdrant Point 并执行回滚删除；
    删除成功后将 Chunk 与 Document 标记为 failed 并清空 ownership token。
    """

    def __init__(self, compensator: IndexVectorsCompensator) -> None:
        self._compensator = compensator

    async def compensate(
        self,
        *,
        operation_id: str,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> None:
        """执行文档向量索引补偿。

        Args:
            operation_id: 本次执行的操作标识 / ownership token。
            payload: IndexDocumentVectorsTaskPayload 输入模型。
            context: 运行时上下文。
        """
        del context
        validated = IndexDocumentVectorsTaskPayload.model_validate(payload)
        await asyncio.to_thread(
            self._compensator.compensate,
            document_id=validated.document_id,
            operation_id=operation_id,
        )
