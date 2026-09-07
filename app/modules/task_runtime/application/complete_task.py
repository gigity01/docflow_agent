"""CompleteTask 显式 Application 用例入口。

提供在短事务中完成 Task 状态、执行结果与后续事件持久化的用例封装。
"""

from app.modules.task_runtime.application.dto import TaskSnapshot


class CompleteTaskUseCase:
    """任务成功完成用例类。"""

    def __init__(self, runtime) -> None:
        self._runtime = runtime

    def execute(
        self,
        snapshot: TaskSnapshot,
        output_json: dict,
        resource_refs: list[str],
    ) -> None:
        """执行 Task 成功结果持久化与状态流转。

        Args:
            snapshot: 任务 Claim 快照。
            output_json: 任务执行产出 JSON 数据。
            resource_refs: 本次任务涉及的资源引用列表。
        """
        return self._runtime.complete_task(
            snapshot,
            output_json,
            resource_refs,
        )
