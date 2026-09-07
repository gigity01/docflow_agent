"""ExecuteTask 三段式 Application 用例入口。

提供触发执行 Plan 任务步进的用例封装。
"""

from app.modules.task_runtime.application.dto import ExecutePlanResult


class ExecuteTaskUseCase:
    """任务执行主用例类。"""

    def __init__(self, runtime) -> None:
        self._runtime = runtime

    async def execute(self, plan_id: str) -> ExecutePlanResult:
        """驱动指定 Plan 的下一个任务执行步进。

        Args:
            plan_id: 目标 Plan ID。

        Returns:
            本次步进的执行结果。
        """
        return await self._runtime.execute_next(plan_id)
