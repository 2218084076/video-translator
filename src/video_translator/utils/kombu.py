"""Utils"""

import asyncio
import logging
import socket
from asyncio import AbstractEventLoop
from itertools import count
from typing import Any, Callable, List, Optional

from video_translator.config import settings
from video_translator.utils import SingletonMeta, run_in_executor
from kombu import Connection, Consumer, Exchange, Queue, producers

from video_translator.utils.exceptions import TranslatorException

logger = logging.getLogger(__name__)


class Channel:
    """Channel"""

    def __init__(self, channel):
        self._channel = channel
        self._consume_tags = {}

    @property
    def channel(self):
        """channel"""
        return self._channel

    def set_consume_tags(self, tags: dict):
        """
        Set consume tags with tags dict
        :param tags:
        :return:
        """
        self._consume_tags.update(tags)

    def tag(self, queue_name: str) -> str:
        """
        Get consume tag with queue name
        :param queue_name:
        :return:
        """
        try:
            return self._consume_tags.pop(queue_name)
        except KeyError:
            logger.warning("Key error, %s not in consume_tags", queue_name)
        return ''

    def cancel_consume(self, queue_name: str):
        """Cancel a consume"""
        tag = self.tag(queue_name)
        if tag:
            self.channel.basic_cancel(tag)


class Kombu(metaclass=SingletonMeta):
    """
    Kombu server ，提供队列服务。

    具有单例性质。
    """

    __connect = None
    _server_running = False
    _should_stop: asyncio.Future = None

    channels = {}

    async def server_start(self, **_):
        """server start"""
        self._server_running = True
        if not self._should_stop:
            self._should_stop = asyncio.Future()

    async def server_stop(self, **_):
        """Call to stop when server stop signal fire."""
        self._server_running = False
        if not self._should_stop.done():
            self._should_stop.set_result('Stop')
        if self.connect:
            logger.debug('Stop kombu connection.')
            await asyncio.sleep(2)
            self.connect.close()

    async def check_server(self) -> bool:
        """
        检查 server 的状态，如果 server 没启动，
        则等待 2 秒后再次检查，如果仍没有启动，抛出异常。
        :return:
        """
        if not self._server_running:
            logger.info('Server has not start, delay 5 seconds.')
            await asyncio.sleep(2)
            if not self._server_running:
                raise TranslatorException(
                    'Server not started. You should start server or '
                    'call `Kombu.server_start` first.'
                )
        logger.debug('Server status %s', self._server_running)
        return self._server_running

    @property
    def connect(self) -> Connection:
        """延迟加载单例连接"""
        if self.__connect is None and self._server_running:
            logger.debug('Server is running, kube connect to %s', settings.MQ)
            self.__connect = Connection(settings.MQ)
        return self.__connect

    def create_channel(self, name: str) -> Channel:
        """创建并返回一个新的 channel对象"""
        if name not in self.channels:
            self.channels.setdefault(name, Channel(channel=self.connect.channel()))
        return self.channels.get(name)

    def cancel_consumer(self, queue_name: str, channel_name: str):
        """
        Cancel consumer
        :param queue_name:
        :param channel_name:
        :return:
        """
        self.create_channel(channel_name).cancel_consume(queue_name)
        logger.info('Cancel consumer task, tag: %s', queue_name)

    def create_queue(self, queue_name: str, channel_name: str = 'check'):
        """
        Create queue
        对消费者管理，通常不进行真实创建
        :param queue_name:
        :param channel_name:
        :return:
        """
        return Queue(name=queue_name, channel=self.create_channel(channel_name).channel)

    def delete_queue(self, queue_name, if_unused=False, if_empty=False, nowait=False):
        """
        Delete queue
        :param queue_name:
        :param if_unused:
        :param if_empty:
        :param nowait:
        :return:
        """
        return self.create_queue(queue_name).delete(if_unused=if_unused,
                                                    if_empty=if_empty,
                                                    nowait=nowait)

    def queue_declare(self, queue_name, nowait=False, passive=False, channel=None):
        """
        Get queue count
        :param queue_name:
        :param nowait:
        :param passive:
        :param channel:
        :return:
        """
        return self.create_queue(queue_name).queue_declare(nowait=nowait,
                                                           passive=passive,
                                                           channel=channel)

    async def publish(
            self,
            queue_name: str,
            routing_key: str,
            exchange_name: str,
            body: Any
    ) -> None:
        """
        将消息写入队列。
        :param queue_name:
        :param routing_key:
        :param exchange_name:
        :param body:
        :return:
        """
        queue = Queue(name=queue_name, exchange=Exchange(exchange_name), routing_key=routing_key)

        def _publish():
            with producers[self.connect].acquire(block=True) as producer:
                producer.publish(
                    body=body,
                    retry=True,
                    exchange=queue.exchange,
                    routing_key=queue.routing_key,
                    declare=[queue]
                )

        await run_in_executor(_publish)

    async def consume(
            self,
            queue_name: str,
            routing_key: str,
            exchange_name: str,
            channel_name: str,
            register_callbacks: List[Callable],
            prefetch_count: int = 1,
    ) -> None:
        """

        example:
            def consuming_and_auto_ack(self, items: list[_T], body: _T, message: Message):
                # Consume and auto ack
                items.append(body)
                message.ack()

            data = []   # consumed data
            consume_on_response = functools.partial(consuming_and_auto_ack, data)
            await self.consume(
                queue_name=self.queue_name(app_id),
                routing_key=self.routing_key(app_id),
                register_callbacks=[consume_on_response],
                limit=3,
            )
            for i in data:
                logging.info(i)

        example:
            async def have_to_do_something(item: Any):
                # Have to do something with coroutine
                # when consume each item.
                await asyncio.sleep(0.1)
                logging.info('Consumed data. Detail: %s', item)

            def consuming_and_auto_ack(
                self,
                func: Callable,
                body: _T,
                message: Message,
                loop: Optional[AbstractEventLoop] = None,
            ):
                # Consume and auto ack
                if inspect.iscoroutinefunction(func):
                    # To submit a coroutine object to the event loop. (thread safe)
                    if not loop:
                        raise Exception('You must pass a event loop when call coroutine object.')
                    future = asyncio.run_coroutine_threadsafe(func(body), loop)
                    try:
                        future.result()    # Wait for the result from other os thread
                    except asyncio.TimeoutError:
                        future.cancel()
                        raise
                else:
                    func(body)
                message.ack()

            consume_on_response = functools.partial(consuming_and_auto_ack, have_to_do_something)
            await self.consume(
                queue_name=self.queue_name(app_id),
                routing_key=self.routing_key(app_id),
                register_callbacks=[consume_on_response],
                limit=3,
            )

        example:
            def consuming_and_manual_ack(self, items: list[tuple[_T, Message]], body: _T, message: Message):
                # Consume and auto ack
                items.append((body, message))

            data = []   # consumed data
            consume_on_response = functools.partial(consuming_and_manual_ack, data)
            await self.consume(
                queue_name=self.queue_name(app_id),
                routing_key=self.routing_key(app_id),
                register_callbacks=[consume_on_response],
                limit=3,
            )
            for _body, _message in data:
                logging.info(_body)
                await run_in_executor(_message.ack) # ack

        :param queue_name:
        :param routing_key:
        :param exchange_name:
        :param register_callbacks:  callback 接受两个参数 body 和 message
            注意，不能是 async 函数
            def cb(body: Dict, message: Message):
                logging.debug(body)
                message.ack()
        :param prefetch_count: default 1
        :param channel_name: default data
        :return:
        """
        await self.check_server()
        queue = Queue(name=queue_name, exchange=Exchange(exchange_name), routing_key=routing_key)
        consumer = Consumer(
            self.create_channel(channel_name).channel,
            queues=[queue],
            tag_prefix=queue_name,
            prefetch_count=prefetch_count
        )
        for callback in register_callbacks:
            consumer.register_callback(callback)
        consumer.consume()
        self.create_channel(channel_name).set_consume_tags(consumer._active_tags)  # noqa  # pylint: disable=W0212
        # 添加延迟,让channel绑定成功
        await asyncio.sleep(0.001)

    async def start_consume(
            self,
            limit: Optional[int] = None,
            timeout: Optional[int] = None,
            safety_interval: Optional[int] = 1,
            loop: AbstractEventLoop = asyncio.get_event_loop()
    ):
        """
        开启消费
        :param limit:
        :param timeout:
        :param safety_interval:
        :param loop:
        :return:
        """
        await asyncio.sleep(0.01)
        asyncio.run_coroutine_threadsafe(self._consuming(
            limit=limit,
            timeout=timeout,
            safety_interval=safety_interval,
            should_stop=self._should_stop), loop)

    async def _consuming(
            self,
            limit: Optional[int] = None,
            timeout: Optional[int] = None,
            safety_interval: Optional[int] = 1,
            should_stop: Optional[asyncio.Future] = None
    ):
        """
        循环获取消息
        :param limit:
        :param timeout:
            超时停止消费
        :param safety_interval:
        :param should_stop:
        :return:
        """
        elapsed = 0
        for _ in limit and range(limit) or count():
            # 如果信号中断发生在之前, 需要处理异常,确保循环中断
            try:
                server_running = await self.check_server()
            except TranslatorException:
                logger.debug('Kombu server stop')
                server_running = False
            _should_stop = not server_running or (should_stop and should_stop.done())
            # 如果 Server 不在运行，或者 should_stop
            if _should_stop:
                logger.debug('%s should stop, so stop consuming message.', self)
                break
            try:
                logger.debug('Kombu draining event 1 seconds.')
                await asyncio.sleep(1)
                self.connect.drain_events(timeout=1)
            except socket.timeout:
                self.connect.heartbeat_check()
                # 超时计时，如果超时时间 elapsed 大于给定的超时时间 timeout 则退出
                elapsed += safety_interval
                if timeout and elapsed >= timeout:
                    raise
            except OSError:
                if not _should_stop:
                    raise
            else:
                elapsed = 0
