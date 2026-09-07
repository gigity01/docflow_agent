"""未启用 Agent Provider 时直接调用 Use Case 的确定性后备 Executor。

包含以下三种确定性执行器：
1. DeterministicProcessDocumentExecutor: 文档清洗处理后备执行器。
2. DeterministicBuildDocumentChunksExecutor: 文档切块后备执行器。
3. DeterministicIndexDocumentVectorsExecutor: 向量索引后备执行器。
所有执行器均在线程池中调用 Use Case，并通过 await_side_effect_quiescence 在取消时排空内部操作。
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from app.modules.document.application.errors import DocumentApplicationError
from app.modules.task_runtime.application.dto import (
    TaskExecutorResult,
    TaskRuntimeContext,
)
from app.modules.task_runtime.application.errors import TaskExecutionError
from app.modules.task_runtime.application.schemas import (
    BuildDocumentChunksTaskOutput,
    BuildDocumentChunksTaskPayload,
    IndexDocumentVectorsTaskOutput,
    IndexDocumentVectorsTaskPayload,
    ProcessDocumentTaskOutput,
    ProcessDocumentTaskPayload,
)
from app.shared.observability.correlation import DocumentOperationContext

from ._quiescence import await_side_effect_quiescence


def _operation_context(context: TaskRuntimeContext) -> DocumentOperationContext:
    """从 TaskRuntimeContext 提取 DocumentOperationContext 审计关联上下文。"""
    return DocumentOperationContext(
        workflow_id=context.workflow_id,
        operation_id=context.operation_id,
        attempt=context.attempt,
    )


async def _execute_document_use_case(call):
    """在线程池中调用同步 Use Case 并使用 await_side_effect_quiescence 保护取消排空。"""
    try:
        return await await_side_effect_quiescence(asyncio.to_thread(call))
    except DocumentApplicationError as exc:
        raise TaskExecutionError(
            "document_operation_rejected",
            str(exc),
            retryable=exc.status_code >= 500,
            blocked=exc.status_code in {400, 404, 409},
        ) from exc


class DeterministicProcessDocumentExecutor:
    """未配置 Agent Provider 时的确定性文档处理（Process）后备执行器。

    在线程池中调用 ProcessDocumentUseCase，并通过 await_side_effect_quiescence 保证超时时同步 Use Case 执行完全排空后才传播取消。
    """

    def __init__(self, use_case) -> None:
        self._use_case = use_case

    async def execute(
        self,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> TaskExecutorResult:
        """执行文档处理，并返回标准 TaskExecutorResult。

        Args:
            payload: ProcessDocumentTaskPayload 输入模型。
            context: 运行时上下文。

        Returns:
            执行产出与资源引用。
        """
        validated = ProcessDocumentTaskPayload.model_validate(payload)
        result = await _execute_document_use_case(
            lambda: self._use_case.execute(
                validated.document_id,
                operation_context=_operation_context(context),
            )
        )
        output = ProcessDocumentTaskOutput(
            document_id=result.document_id,
            status=result.status,
            cleaned_uri=result.cleaned_uri,
        )
        return TaskExecutorResult(
            output_json=output.model_dump(mode="json"),
            resource_refs=[f"document:{result.document_id}"],
        )


class DeterministicBuildDocumentChunksExecutor:
    """未配置 Agent Provider 时的确定性文档切块（Build Chunks）后备执行器。"""

    def __init__(self, use_case) -> None:
        self._use_case = use_case

    async def execute(
        self,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> TaskExecutorResult:
        """执行文档切块，并返回标准 TaskExecutorResult。

        Args:
            payload: BuildDocumentChunksTaskPayload 输入模型。
            context: 运行时上下文。

        Returns:
            执行产出与资源引用。
        """
        validated = BuildDocumentChunksTaskPayload.model_validate(payload)
        result = await _execute_document_use_case(
            lambda: self._use_case.execute(
                validated.document_id,
                operation_context=_operation_context(context),
            )
        )
        output = BuildDocumentChunksTaskOutput(
            document_id=result.document_id,
            status=result.status,
            parent_count=result.parent_count,
            child_count=result.child_count,
        )
        return TaskExecutorResult(
            output_json=output.model_dump(mode="json"),
            resource_refs=[f"document:{result.document_id}"],
        )


class DeterministicIndexDocumentVectorsExecutor:
    """未配置 Agent Provider 时的确定性向量索引（Index Vectors）后备执行器。"""

    def __init__(self, use_case) -> None:
        self._use_case = use_case

    async def execute(
        self,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> TaskExecutorResult:
        """执行向量索引生成与 Qdrant 写入，并返回标准 TaskExecutorResult。

        Args:
            payload: IndexDocumentVectorsTaskPayload 输入模型。
            context: 运行时上下文。

        Returns:
            执行产出与资源引用。
        """
        validated = IndexDocumentVectorsTaskPayload.model_validate(payload)
        result = await _execute_document_use_case(
            lambda: self._use_case.execute(
                validated.document_id,
                operation_context=_operation_context(context),
            )
        )
        output = IndexDocumentVectorsTaskOutput(
            document_id=result.document_id,
            status=result.status,
            indexed_chunks=result.indexed_chunks,
            failed_chunks=result.failed_chunks,
        )
        return TaskExecutorResult(
            output_json=output.model_dump(mode="json"),
            resource_refs=[f"document:{result.document_id}"],
        )
