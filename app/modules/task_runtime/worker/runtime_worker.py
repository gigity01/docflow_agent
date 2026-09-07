"""消费单个 runtime.plan_wakeup 事件的薄 Worker 执行入口。

接收 Redis Stream 消费到的 Plan 唤醒事件并调用 TaskRuntimeService 执行步进。
"""

from app.modules.task_runtime.application.dto import ExecutePlanResult
from app.modules.task_runtime.application.runtime import TaskRuntimeService


class RuntimeWorker:
    """Task Runtime 事件处理器类。"""

    def __init__(self, runtime: TaskRuntimeService) -> None:
        """初始化 RuntimeWorker。

        Args:
            runtime: TaskRuntimeService 业务服务实例。
        """
        self._runtime = runtime

    async def handle_plan_wakeup(self, plan_id: str) -> ExecutePlanResult:
        """响应 `runtime.plan_wakeup` 事件并驱动 Plan 的下一任务执行或补偿。

        Args:
            plan_id: 目标 Plan ID。

        Returns:
            步进执行结果。
        """
        return await self._runtime.execute_next(plan_id)
