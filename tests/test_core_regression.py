"""将抽取的通用回归场景接入 unittest，逐条报告并关闭所有临时资源。"""

import asyncio
import unittest
from . import regression_cases as cases


class CoreRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)

    async def asyncTearDown(self):
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for db in cases.OPEN_DBS:
            db.close()
        cases.OPEN_DBS.clear()


def make_case(function):
    async def run(self):
        await function()
    run.__name__ = function.__name__
    run.__doc__ = function.__doc__
    return run


for name, function in cases.ALL_TESTS:
    setattr(CoreRegressionTests, name, make_case(function))
