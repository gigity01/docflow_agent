"""验证 Windows Worker 的停机信号后备路径。"""

import asyncio
import signal
import unittest
from unittest.mock import Mock, patch


class WorkerSignalTest(unittest.IsolatedAsyncioTestCase):
    async def test_windows_signal_is_forwarded_to_event_loop(self):
        from app.workers.runtime_main import _install_signal_handlers

        event = asyncio.Event()
        loop = Mock()
        loop.add_signal_handler.side_effect = NotImplementedError
        loop.call_soon_threadsafe.side_effect = lambda callback: callback()
        with patch("asyncio.get_running_loop", return_value=loop), patch("signal.signal") as register:
            _install_signal_handlers(event)
        self.assertEqual([call.args[0] for call in register.call_args_list],
                         [signal.SIGINT, signal.SIGTERM])
        register.call_args_list[0].args[1](signal.SIGINT, None)
        self.assertTrue(event.is_set())
