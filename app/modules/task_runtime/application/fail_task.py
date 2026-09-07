"""FailTask 显式 Application 用例入口。

提供处理任务失败、执行补偿及流转状态的用例封装。
"""

from app.modules.task_runtime.application.dto import (
    ExecutePlanResult,
    TaskSnapshot,
)
from app.modules.task_runtime.application.errors import TaskExecutionError


class FailTaskUseCase:
    """任务失败处理用例类。"""

    def __init__(self, runtime) -> None:
        self._runtime = runtime

    async def execute(
        self,
        snapshot: TaskSnapshot,
        error: TaskExecutionError,
    ) -> ExecutePlanResult:
        """执行任务失败处理流程。

        Args:
            snapshot: 任务 Claim 快照。
            error: 捕获的 TaskExecutionError 异常。

        Returns:
            失败处理与补偿/重试调度结果。
        """
        return await self._runtime.fail_task(snapshot, error)
