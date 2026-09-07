"""注册文档处理、切块和向量索引能力及其执行策略。

每项能力绑定输入输出模型、执行器和补偿器。超时与最大尝试次数
以下方配置为准；max_attempts 包含首次执行，并非额外重试次数。
"""

from app.modules.planning.domain.enums import PlanningCapabilityCode
from app.modules.task_runtime.application.ports import (
    CapabilityDefinition,
    CapabilityRegistry,
)
from app.modules.task_runtime.application.schemas import (
    BuildDocumentChunksTaskOutput,
    BuildDocumentChunksTaskPayload,
    IndexDocumentVectorsTaskOutput,
    IndexDocumentVectorsTaskPayload,
    ProcessDocumentTaskOutput,
    ProcessDocumentTaskPayload,
)


def build_capability_registry() -> CapabilityRegistry:
    """构建当前支持的文档能力注册表。"""
    return CapabilityRegistry(
        [
            CapabilityDefinition(
                capability_code=PlanningCapabilityCode.PROCESS_DOCUMENT.value,
                input_model=ProcessDocumentTaskPayload,
                output_model=ProcessDocumentTaskOutput,
                executor_code="document.process",
                compensator_code="document.process",
                max_attempts=3,
                timeout_seconds=300,
                side_effect=True,
            ),
            CapabilityDefinition(
                capability_code=(
                    PlanningCapabilityCode.BUILD_DOCUMENT_CHUNKS.value
                ),
                input_model=BuildDocumentChunksTaskPayload,
                output_model=BuildDocumentChunksTaskOutput,
                executor_code="document.build_chunks",
                compensator_code="document.build_chunks",
                max_attempts=3,
                timeout_seconds=300,
                side_effect=True,
            ),
            CapabilityDefinition(
                capability_code=(
                    PlanningCapabilityCode.INDEX_DOCUMENT_VECTORS.value
                ),
                input_model=IndexDocumentVectorsTaskPayload,
                output_model=IndexDocumentVectorsTaskOutput,
                executor_code="document.index_vectors",
                compensator_code="document.index_vectors",
                max_attempts=3,
                timeout_seconds=900,
                side_effect=True,
            ),
        ]
    )
