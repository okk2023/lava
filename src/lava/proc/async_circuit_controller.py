"""
异步电路控制器模块

该模块提供了一个异步电路控制器的实现，支持：
- 异步状态机控制
- 事件驱动的电路控制
- 多通道信号处理
- 异步时序控制
- 电路状态监控

作者: Lava-NC
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Union, Coroutine
from collections import defaultdict
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """电路状态枚举"""
    IDLE = auto()
    INITIALIZING = auto()
    ACTIVE = auto()
    PROCESSING = auto()
    ERROR = auto()
    SHUTDOWN = auto()
    SUSPEND = auto()


class SignalType(Enum):
    """信号类型枚举"""
    DIGITAL = auto()
    ANALOG = auto()
    PWM = auto()
    PULSE = auto()


class EventType(Enum):
    """事件类型枚举"""
    SIGNAL_CHANGE = auto()
    STATE_TRANSITION = auto()
    ERROR_DETECTED = auto()
    TIMEOUT = auto()
    EXTERNAL_TRIGGER = auto()


@dataclass
class Signal:
    """信号数据结构"""
    name: str
    signal_type: SignalType
    value: Union[int, float, bool]
    timestamp: float = field(default_factory=time.time)
    channel: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()


@dataclass
class Event:
    """事件数据结构"""
    event_type: EventType
    source: str
    data: Any = None
    timestamp: float = field(default_factory=time.time)
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class EventHandler(ABC):
    """事件处理器抽象基类"""
    
    @abstractmethod
    async def handle(self, event: Event) -> bool:
        """处理事件"""
        pass

    @abstractmethod
    def can_handle(self, event: Event) -> bool:
        """检查是否可以处理该事件"""
        pass


class StateTransitionHandler(EventHandler):
    """状态转换事件处理器"""
    
    def __init__(self, controller):
        self.controller = controller
    
    def can_handle(self, event: Event) -> bool:
        return event.event_type == EventType.STATE_TRANSITION
    
    async def handle(self, event: Event) -> bool:
        try:
            new_state = event.data.get('new_state')
            if new_state and isinstance(new_state, CircuitState):
                await self.controller._transition_to_state(new_state)
                return True
        except Exception as e:
            logger.error(f"状态转换处理失败: {e}")
        return False


class SignalChangeHandler(EventHandler):
    """信号变化事件处理器"""
    
    def __init__(self, controller):
        self.controller = controller
    
    def can_handle(self, event: Event) -> bool:
        return event.event_type == EventType.SIGNAL_CHANGE
    
    async def handle(self, event: Event) -> bool:
        try:
            signal = event.data.get('signal')
            if signal and isinstance(signal, Signal):
                await self.controller._process_signal_change(signal)
                return True
        except Exception as e:
            logger.error(f"信号变化处理失败: {e}")
        return False


class ErrorHandler(EventHandler):
    """错误事件处理器"""
    
    def __init__(self, controller):
        self.controller = controller
    
    def can_handle(self, event: Event) -> bool:
        return event.event_type == EventType.ERROR_DETECTED
    
    async def handle(self, event: Event) -> bool:
        try:
            error_info = event.data
            await self.controller._handle_error(error_info)
            return True
        except Exception as e:
            logger.error(f"错误处理失败: {e}")
        return False


class AsyncCircuitController:
    """异步电路控制器主类"""
    
    def __init__(self, name: str = "AsyncCircuit", max_channels: int = 16):
        self.name = name
        self.max_channels = max_channels
        self.current_state = CircuitState.IDLE
        self.previous_state = CircuitState.IDLE
        
        # 事件系统
        self.event_queue = asyncio.Queue()
        self.event_handlers: List[EventHandler] = []
        self.event_processing_task: Optional[asyncio.Task] = None
        
        # 信号管理
        self.signals: Dict[str, Signal] = {}
        self.signal_history: Dict[str, List[Signal]] = defaultdict(list)
        self.signal_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
        # 状态管理
        self.state_callbacks: Dict[CircuitState, List[Callable]] = defaultdict(list)
        self.state_history: List[tuple] = []  # (state, timestamp)
        
        # 时序控制
        self.timers: Dict[str, asyncio.Task] = {}
        self.periodic_tasks: Dict[str, asyncio.Task] = {}
        
        # 线程池用于CPU密集型任务
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # 监控数据
        self.stats = {
            'events_processed': 0,
            'signals_processed': 0,
            'state_transitions': 0,
            'errors_count': 0,
            'uptime_start': time.time()
        }
        
        # 初始化事件处理器
        self._init_event_handlers()
        
        # 控制标志
        self._running = False
        self._shutdown_event = asyncio.Event()
    
    def _init_event_handlers(self):
        """初始化事件处理器"""
        self.event_handlers = [
            StateTransitionHandler(self),
            SignalChangeHandler(self),
            ErrorHandler(self)
        ]
    
    async def start(self):
        """启动异步电路控制器"""
        if self._running:
            logger.warning("控制器已经在运行")
            return
        
        self._running = True
        self._shutdown_event.clear()
        
        logger.info(f"启动异步电路控制器: {self.name}")
        
        # 启动事件处理任务
        self.event_processing_task = asyncio.create_task(self._process_events())
        
        # 转换到初始化状态
        await self.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="controller",
            data={'new_state': CircuitState.INITIALIZING}
        ))
        
        # 启动监控任务
        asyncio.create_task(self._monitoring_task())
        
        logger.info("异步电路控制器启动完成")
    
    async def stop(self):
        """停止异步电路控制器"""
        if not self._running:
            return
        
        logger.info("正在停止异步电路控制器...")
        
        # 设置停止标志
        self._running = False
        self._shutdown_event.set()
        
        # 转换到关闭状态
        await self.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="controller",
            data={'new_state': CircuitState.SHUTDOWN}
        ))
        
        # 取消所有定时器和周期性任务
        for timer in self.timers.values():
            timer.cancel()
        for task in self.periodic_tasks.values():
            task.cancel()
        
        # 等待事件处理完成
        if self.event_processing_task:
            self.event_processing_task.cancel()
            try:
                await self.event_processing_task
            except asyncio.CancelledError:
                pass
        
        # 关闭线程池
        self.executor.shutdown(wait=True)
        
        logger.info("异步电路控制器已停止")
    
    async def emit_event(self, event: Event):
        """发出事件"""
        await self.event_queue.put(event)
        logger.debug(f"事件已发出: {event.event_type.name} from {event.source}")
    
    async def _process_events(self):
        """事件处理主循环"""
        logger.info("开始事件处理循环")
        
        while self._running:
            try:
                # 等待事件，设置超时避免阻塞
                event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                
                # 统计
                self.stats['events_processed'] += 1
                
                # 查找合适的处理器
                handled = False
                for handler in self.event_handlers:
                    if handler.can_handle(event):
                        handled = await handler.handle(event)
                        if handled:
                            break
                
                if not handled:
                    logger.warning(f"未找到处理器处理事件: {event.event_type.name}")
                
                # 标记事件完成
                self.event_queue.task_done()
                
            except asyncio.TimeoutError:
                # 超时是正常的，继续循环
                continue
            except Exception as e:
                logger.error(f"事件处理出错: {e}")
                await self._handle_error({'error': e, 'context': 'event_processing'})
    
    async def _transition_to_state(self, new_state: CircuitState):
        """状态转换"""
        if new_state == self.current_state:
            return
        
        logger.info(f"状态转换: {self.current_state.name} -> {new_state.name}")
        
        self.previous_state = self.current_state
        self.current_state = new_state
        self.state_history.append((new_state, time.time()))
        self.stats['state_transitions'] += 1
        
        # 执行状态回调
        callbacks = self.state_callbacks.get(new_state, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self.previous_state, new_state)
                else:
                    callback(self.previous_state, new_state)
            except Exception as e:
                logger.error(f"状态回调执行失败: {e}")
    
    async def _process_signal_change(self, signal: Signal):
        """处理信号变化"""
        logger.debug(f"处理信号变化: {signal.name} = {signal.value}")
        
        # 更新信号
        self.signals[signal.name] = signal
        
        # 添加到历史记录
        history = self.signal_history[signal.name]
        history.append(signal)
        
        # 限制历史记录长度
        if len(history) > 1000:
            history.pop(0)
        
        # 统计
        self.stats['signals_processed'] += 1
        
        # 执行信号回调
        callbacks = self.signal_callbacks.get(signal.name, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(signal)
                else:
                    callback(signal)
            except Exception as e:
                logger.error(f"信号回调执行失败: {e}")
    
    async def _handle_error(self, error_info: Dict[str, Any]):
        """处理错误"""
        logger.error(f"处理错误: {error_info}")
        self.stats['errors_count'] += 1
        
        # 根据错误严重程度决定是否转换状态
        error_severity = error_info.get('severity', 'medium')
        if error_severity == 'critical':
            await self.emit_event(Event(
                event_type=EventType.STATE_TRANSITION,
                source="error_handler",
                data={'new_state': CircuitState.ERROR}
            ))
    
    async def set_signal(self, name: str, value: Union[int, float, bool], 
                        signal_type: SignalType = SignalType.DIGITAL, 
                        channel: int = 0, metadata: Optional[Dict] = None):
        """设置信号值"""
        if channel >= self.max_channels:
            raise ValueError(f"通道号 {channel} 超出范围 (最大: {self.max_channels-1})")
        
        signal = Signal(
            name=name,
            signal_type=signal_type,
            value=value,
            channel=channel,
            metadata=metadata or {}
        )
        
        await self.emit_event(Event(
            event_type=EventType.SIGNAL_CHANGE,
            source="signal_setter",
            data={'signal': signal}
        ))
    
    def get_signal(self, name: str) -> Optional[Signal]:
        """获取信号值"""
        return self.signals.get(name)
    
    def get_signal_history(self, name: str, limit: int = 100) -> List[Signal]:
        """获取信号历史"""
        history = self.signal_history.get(name, [])
        return history[-limit:] if limit > 0 else history
    
    def register_signal_callback(self, signal_name: str, callback: Callable):
        """注册信号变化回调"""
        self.signal_callbacks[signal_name].append(callback)
    
    def register_state_callback(self, state: CircuitState, callback: Callable):
        """注册状态变化回调"""
        self.state_callbacks[state].append(callback)
    
    async def set_timer(self, name: str, delay: float, callback: Callable, 
                       repeat: bool = False):
        """设置定时器"""
        async def timer_task():
            try:
                if repeat:
                    while self._running:
                        await asyncio.sleep(delay)
                        if asyncio.iscoroutinefunction(callback):
                            await callback()
                        else:
                            callback()
                else:
                    await asyncio.sleep(delay)
                    if asyncio.iscoroutinefunction(callback):
                        await callback()
                    else:
                        callback()
            except asyncio.CancelledError:
                logger.debug(f"定时器 {name} 被取消")
            except Exception as e:
                logger.error(f"定时器 {name} 执行出错: {e}")
            finally:
                if name in self.timers:
                    del self.timers[name]
        
        # 取消同名的现有定时器
        if name in self.timers:
            self.timers[name].cancel()
        
        # 创建新定时器
        self.timers[name] = asyncio.create_task(timer_task())
    
    def cancel_timer(self, name: str):
        """取消定时器"""
        if name in self.timers:
            self.timers[name].cancel()
            del self.timers[name]
    
    async def add_periodic_task(self, name: str, interval: float, task_func: Callable):
        """添加周期性任务"""
        async def periodic_wrapper():
            try:
                while self._running:
                    if asyncio.iscoroutinefunction(task_func):
                        await task_func()
                    else:
                        task_func()
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.debug(f"周期性任务 {name} 被取消")
            except Exception as e:
                logger.error(f"周期性任务 {name} 执行出错: {e}")
        
        if name in self.periodic_tasks:
            self.periodic_tasks[name].cancel()
        
        self.periodic_tasks[name] = asyncio.create_task(periodic_wrapper())
    
    def remove_periodic_task(self, name: str):
        """移除周期性任务"""
        if name in self.periodic_tasks:
            self.periodic_tasks[name].cancel()
            del self.periodic_tasks[name]
    
    async def _monitoring_task(self):
        """监控任务"""
        while self._running:
            try:
                await asyncio.sleep(10)  # 每10秒监控一次
                
                uptime = time.time() - self.stats['uptime_start']
                logger.info(
                    f"控制器状态监控 - "
                    f"状态: {self.current_state.name}, "
                    f"运行时间: {uptime:.1f}s, "
                    f"处理事件: {self.stats['events_processed']}, "
                    f"处理信号: {self.stats['signals_processed']}, "
                    f"状态转换: {self.stats['state_transitions']}, "
                    f"错误数量: {self.stats['errors_count']}"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控任务出错: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        stats['current_state'] = self.current_state.name
        stats['uptime'] = time.time() - stats['uptime_start']
        stats['signal_count'] = len(self.signals)
        stats['active_timers'] = len(self.timers)
        stats['periodic_tasks'] = len(self.periodic_tasks)
        return stats
    
    async def reset(self):
        """重置控制器"""
        logger.info("重置异步电路控制器")
        
        # 清除所有信号
        self.signals.clear()
        self.signal_history.clear()
        
        # 取消所有定时器和任务
        for timer in self.timers.values():
            timer.cancel()
        for task in self.periodic_tasks.values():
            task.cancel()
        self.timers.clear()
        self.periodic_tasks.clear()
        
        # 重置统计
        self.stats = {
            'events_processed': 0,
            'signals_processed': 0,
            'state_transitions': 0,
            'errors_count': 0,
            'uptime_start': time.time()
        }
        
        # 转换到空闲状态
        await self.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="reset",
            data={'new_state': CircuitState.IDLE}
        ))


# 便捷的工厂函数
def create_async_circuit_controller(name: str = "AsyncCircuit", 
                                  max_channels: int = 16) -> AsyncCircuitController:
    """创建异步电路控制器实例"""
    return AsyncCircuitController(name=name, max_channels=max_channels)


# 示例用法和测试函数
async def example_usage():
    """异步电路控制器使用示例"""
    
    # 创建控制器
    controller = create_async_circuit_controller("ExampleCircuit", max_channels=8)
    
    # 定义回调函数
    async def on_signal_change(signal: Signal):
        print(f"信号变化: {signal.name} = {signal.value}")
    
    def on_state_change(old_state: CircuitState, new_state: CircuitState):
        print(f"状态变化: {old_state.name} -> {new_state.name}")
    
    # 注册回调
    controller.register_signal_callback("sensor1", on_signal_change)
    controller.register_state_callback(CircuitState.ACTIVE, on_state_change)
    
    try:
        # 启动控制器
        await controller.start()
        
        # 转换到活动状态
        await controller.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="example",
            data={'new_state': CircuitState.ACTIVE}
        ))
        
        # 设置一些信号
        await controller.set_signal("sensor1", True, SignalType.DIGITAL, channel=0)
        await controller.set_signal("temp_sensor", 25.5, SignalType.ANALOG, channel=1)
        await controller.set_signal("pwm_output", 0.75, SignalType.PWM, channel=2)
        
        # 设置定时器
        async def timer_callback():
            print("定时器触发!")
            await controller.set_signal("timer_signal", True, SignalType.DIGITAL)
        
        await controller.set_timer("example_timer", 2.0, timer_callback)
        
        # 运行一段时间
        await asyncio.sleep(5)
        
        # 查看统计信息
        stats = controller.get_stats()
        print(f"控制器统计: {stats}")
        
        # 获取信号历史
        history = controller.get_signal_history("sensor1")
        print(f"sensor1 历史: {len(history)} 条记录")
        
    finally:
        # 停止控制器
        await controller.stop()


if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())