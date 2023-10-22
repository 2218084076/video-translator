"""utils"""
import asyncio
from functools import partial
from typing import Any, Callable


class SingletonMeta(type):
    """
    单例元类

    example:
        class Foo(metaclass=SingletonMeta):...
    """
    __instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instances:
            instance = super().__call__(*args, **kwargs)
            cls.__instances[cls] = instance
        return cls.__instances[cls]


async def run_in_executor(func: Callable, *args, **kwargs) -> Any:
    """
    如果自定义 executor 请在 kwargs 中传入。
    :param func:
    :param kwargs:
        : kwargs func 的字典参数
        : executor 自定义 executor
    :return:
    """
    executor = kwargs.pop('executor', None)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, partial(func, *args, **kwargs))
