"""ClaimNextTask 显式 Application 用例入口。

提供抢占/领取 Plan 下一个就绪任务的用例封装。
"""

from app.modules.task_runtime.application.dto import (
    ClaimNextTaskInput,
    ClaimNextTaskResult,
)


class ClaimNextTaskUseCase:
    """任务领取用例类。"""

    def __init__(self, runtime) -> None:
        self._runtime = runtime

    def execute(self, command: ClaimNextTaskInput) -> ClaimNextTaskResult:
        """执行任务 Claim 抢占。

        Args:
            command: Claim 输入参数。

        Returns:
            任务领取或补偿恢复快照结果。
        """
        return self._runtime.claim_next_task(command)
