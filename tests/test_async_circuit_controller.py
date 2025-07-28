"""
异步电路控制器测试模块

测试异步电路控制器的各种功能，包括：
- 基本启动停止
- 状态转换
- 信号处理
- 事件系统
- 定时器功能
- 错误处理
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock
import sys
import os

# 添加src路径到系统路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from lava.proc.async_circuit_controller import (
    AsyncCircuitController,
    CircuitState,
    SignalType,
    EventType,
    Signal,
    Event,
    create_async_circuit_controller
)


class TestAsyncCircuitController:
    """异步电路控制器测试类"""
    
    @pytest.fixture
    async def controller(self):
        """创建测试用的控制器实例"""
        controller = create_async_circuit_controller("TestCircuit", max_channels=4)
        yield controller
        # 确保测试后清理
        if controller._running:
            await controller.stop()
    
    @pytest.mark.asyncio
    async def test_controller_creation(self):
        """测试控制器创建"""
        controller = create_async_circuit_controller("TestController", max_channels=8)
        
        assert controller.name == "TestController"
        assert controller.max_channels == 8
        assert controller.current_state == CircuitState.IDLE
        assert not controller._running
        assert len(controller.event_handlers) == 3  # 三个默认处理器
    
    @pytest.mark.asyncio
    async def test_start_stop_controller(self, controller):
        """测试控制器启动和停止"""
        # 测试启动
        await controller.start()
        
        assert controller._running is True
        assert controller.current_state == CircuitState.INITIALIZING
        
        # 等待状态稳定
        await asyncio.sleep(0.1)
        
        # 测试停止
        await controller.stop()
        
        assert controller._running is False
        assert controller.current_state == CircuitState.SHUTDOWN
    
    @pytest.mark.asyncio
    async def test_state_transitions(self, controller):
        """测试状态转换"""
        await controller.start()
        
        # 记录状态变化
        state_changes = []
        
        def on_state_change(old_state, new_state):
            state_changes.append((old_state, new_state))
        
        controller.register_state_callback(CircuitState.ACTIVE, on_state_change)
        
        # 转换到活动状态
        await controller.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="test",
            data={'new_state': CircuitState.ACTIVE}
        ))
        
        # 等待状态转换完成
        await asyncio.sleep(0.1)
        
        assert controller.current_state == CircuitState.ACTIVE
        assert len(state_changes) == 1
        assert state_changes[0][1] == CircuitState.ACTIVE
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_signal_processing(self, controller):
        """测试信号处理"""
        await controller.start()
        
        # 记录信号变化
        signal_changes = []
        
        async def on_signal_change(signal):
            signal_changes.append(signal)
        
        controller.register_signal_callback("test_signal", on_signal_change)
        
        # 设置信号
        await controller.set_signal("test_signal", True, SignalType.DIGITAL, channel=0)
        await controller.set_signal("analog_signal", 3.14, SignalType.ANALOG, channel=1)
        
        # 等待信号处理
        await asyncio.sleep(0.1)
        
        # 验证信号存储
        signal = controller.get_signal("test_signal")
        assert signal is not None
        assert signal.value is True
        assert signal.signal_type == SignalType.DIGITAL
        assert signal.channel == 0
        
        analog_signal = controller.get_signal("analog_signal")
        assert analog_signal is not None
        assert analog_signal.value == 3.14
        assert analog_signal.signal_type == SignalType.ANALOG
        
        # 验证回调被调用
        assert len(signal_changes) == 1
        assert signal_changes[0].name == "test_signal"
        assert signal_changes[0].value is True
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_signal_history(self, controller):
        """测试信号历史记录"""
        await controller.start()
        
        # 设置多个信号值
        for i in range(5):
            await controller.set_signal("counter", i, SignalType.DIGITAL)
            await asyncio.sleep(0.01)  # 确保时间戳不同
        
        await asyncio.sleep(0.1)  # 等待处理完成
        
        # 获取历史记录
        history = controller.get_signal_history("counter")
        assert len(history) == 5
        
        # 验证历史记录的顺序和值
        for i, signal in enumerate(history):
            assert signal.value == i
            assert signal.name == "counter"
        
        # 测试限制历史记录数量
        limited_history = controller.get_signal_history("counter", limit=3)
        assert len(limited_history) == 3
        assert limited_history[-1].value == 4  # 最新的值
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_channel_validation(self, controller):
        """测试通道验证"""
        await controller.start()
        
        # 有效通道
        await controller.set_signal("valid_signal", True, SignalType.DIGITAL, channel=3)
        signal = controller.get_signal("valid_signal")
        assert signal.channel == 3
        
        # 无效通道应该抛出异常
        with pytest.raises(ValueError, match="通道号 4 超出范围"):
            await controller.set_signal("invalid_signal", True, SignalType.DIGITAL, channel=4)
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_timers(self, controller):
        """测试定时器功能"""
        await controller.start()
        
        # 测试单次定时器
        timer_executed = []
        
        async def timer_callback():
            timer_executed.append(time.time())
        
        await controller.set_timer("test_timer", 0.1, timer_callback)
        
        # 等待定时器执行
        await asyncio.sleep(0.2)
        
        assert len(timer_executed) == 1
        
        # 测试重复定时器
        repeat_timer_executed = []
        
        def repeat_callback():
            repeat_timer_executed.append(time.time())
        
        await controller.set_timer("repeat_timer", 0.05, repeat_callback, repeat=True)
        
        # 等待几次执行
        await asyncio.sleep(0.2)
        
        # 取消定时器
        controller.cancel_timer("repeat_timer")
        
        # 应该执行了至少2次
        assert len(repeat_timer_executed) >= 2
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_periodic_tasks(self, controller):
        """测试周期性任务"""
        await controller.start()
        
        # 周期性任务执行计数
        execution_count = []
        
        async def periodic_task():
            execution_count.append(time.time())
        
        # 添加周期性任务
        await controller.add_periodic_task("test_periodic", 0.05, periodic_task())
        
        # 等待几次执行
        await asyncio.sleep(0.2)
        
        # 移除任务
        controller.remove_periodic_task("test_periodic")
        
        # 应该执行了至少2次
        assert len(execution_count) >= 2
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_error_handling(self, controller):
        """测试错误处理"""
        await controller.start()
        
        # 发出错误事件
        await controller.emit_event(Event(
            event_type=EventType.ERROR_DETECTED,
            source="test",
            data={'error': 'Test error', 'severity': 'medium'}
        ))
        
        await asyncio.sleep(0.1)
        
        # 验证错误统计
        stats = controller.get_stats()
        assert stats['errors_count'] == 1
        
        # 发出严重错误
        await controller.emit_event(Event(
            event_type=EventType.ERROR_DETECTED,
            source="test",
            data={'error': 'Critical error', 'severity': 'critical'}
        ))
        
        await asyncio.sleep(0.1)
        
        # 应该转换到错误状态
        assert controller.current_state == CircuitState.ERROR
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_statistics(self, controller):
        """测试统计信息"""
        await controller.start()
        
        # 执行一些操作
        await controller.set_signal("stat_test", 1, SignalType.DIGITAL)
        await controller.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="test",
            data={'new_state': CircuitState.ACTIVE}
        ))
        
        await asyncio.sleep(0.1)
        
        stats = controller.get_stats()
        
        # 验证统计信息
        assert 'events_processed' in stats
        assert 'signals_processed' in stats
        assert 'state_transitions' in stats
        assert 'current_state' in stats
        assert 'uptime' in stats
        assert 'signal_count' in stats
        
        assert stats['signals_processed'] >= 1
        assert stats['events_processed'] >= 2  # 至少处理了状态转换和信号事件
        assert stats['signal_count'] >= 1
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_reset_functionality(self, controller):
        """测试重置功能"""
        await controller.start()
        
        # 设置一些状态
        await controller.set_signal("reset_test", 42, SignalType.DIGITAL)
        await controller.set_timer("reset_timer", 1.0, lambda: None)
        
        await asyncio.sleep(0.1)
        
        # 验证状态
        assert len(controller.signals) >= 1
        assert len(controller.timers) >= 1
        
        # 重置
        await controller.reset()
        await asyncio.sleep(0.1)
        
        # 验证重置效果
        assert len(controller.signals) == 0
        assert len(controller.timers) == 0
        assert controller.current_state == CircuitState.IDLE
        
        # 统计应该被重置
        stats = controller.get_stats()
        assert stats['signals_processed'] == 0
        assert stats['events_processed'] <= 2  # 可能有重置相关的事件
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, controller):
        """测试并发操作"""
        await controller.start()
        
        # 并发设置多个信号
        tasks = []
        for i in range(10):
            task = controller.set_signal(f"concurrent_{i}", i, SignalType.DIGITAL, channel=i % 4)
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.1)
        
        # 验证所有信号都被正确设置
        for i in range(10):
            signal = controller.get_signal(f"concurrent_{i}")
            assert signal is not None
            assert signal.value == i
        
        await controller.stop()
    
    @pytest.mark.asyncio
    async def test_event_priority_and_metadata(self, controller):
        """测试事件优先级和元数据"""
        await controller.start()
        
        # 创建带有元数据的事件
        metadata = {"source_device": "sensor_1", "location": "room_a"}
        
        await controller.set_signal(
            "metadata_test", 
            100, 
            SignalType.ANALOG, 
            channel=0, 
            metadata=metadata
        )
        
        await asyncio.sleep(0.1)
        
        # 验证元数据被保存
        signal = controller.get_signal("metadata_test")
        assert signal is not None
        assert signal.metadata == metadata
        
        await controller.stop()


class TestSignalAndEventClasses:
    """测试信号和事件类"""
    
    def test_signal_creation(self):
        """测试信号创建"""
        signal = Signal(
            name="test_signal",
            signal_type=SignalType.DIGITAL,
            value=True,
            channel=1,
            metadata={"device": "arduino"}
        )
        
        assert signal.name == "test_signal"
        assert signal.signal_type == SignalType.DIGITAL
        assert signal.value is True
        assert signal.channel == 1
        assert signal.metadata["device"] == "arduino"
        assert signal.timestamp > 0
    
    def test_event_creation(self):
        """测试事件创建"""
        event = Event(
            event_type=EventType.SIGNAL_CHANGE,
            source="test_source",
            data={"key": "value"},
            priority=1,
            metadata={"test": True}
        )
        
        assert event.event_type == EventType.SIGNAL_CHANGE
        assert event.source == "test_source"
        assert event.data["key"] == "value"
        assert event.priority == 1
        assert event.metadata["test"] is True
        assert event.timestamp > 0


# 性能测试
class TestPerformance:
    """性能测试"""
    
    @pytest.mark.asyncio
    async def test_high_frequency_signals(self):
        """测试高频信号处理性能"""
        controller = create_async_circuit_controller("PerformanceTest", max_channels=1)
        
        try:
            await controller.start()
            
            start_time = time.time()
            
            # 发送1000个信号
            for i in range(1000):
                await controller.set_signal("perf_test", i, SignalType.DIGITAL)
            
            # 等待处理完成
            await asyncio.sleep(0.5)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # 验证性能（应该在合理时间内完成）
            assert processing_time < 2.0  # 2秒内完成1000个信号处理
            
            # 验证所有信号都被处理
            stats = controller.get_stats()
            assert stats['signals_processed'] == 1000
            
            # 验证历史记录
            history = controller.get_signal_history("perf_test")
            assert len(history) == 1000
            assert history[-1].value == 999  # 最后一个值
            
        finally:
            await controller.stop()


# 集成测试
class TestIntegration:
    """集成测试"""
    
    @pytest.mark.asyncio
    async def test_complete_workflow(self):
        """测试完整的工作流程"""
        controller = create_async_circuit_controller("IntegrationTest", max_channels=4)
        
        # 工作流程状态追踪
        workflow_state = {
            'initialized': False,
            'sensors_activated': False,
            'processing_started': False,
            'output_generated': False
        }
        
        # 状态变化回调
        def on_state_change(old_state, new_state):
            if new_state == CircuitState.INITIALIZING:
                workflow_state['initialized'] = True
            elif new_state == CircuitState.ACTIVE:
                workflow_state['sensors_activated'] = True
            elif new_state == CircuitState.PROCESSING:
                workflow_state['processing_started'] = True
        
        # 信号处理回调
        async def on_sensor_data(signal):
            if signal.name == "temperature" and signal.value > 25.0:
                # 温度过高，启动冷却
                await controller.set_signal("cooling_fan", True, SignalType.DIGITAL, channel=2)
                workflow_state['output_generated'] = True
        
        try:
            # 注册回调
            controller.register_state_callback(CircuitState.INITIALIZING, on_state_change)
            controller.register_state_callback(CircuitState.ACTIVE, on_state_change)
            controller.register_state_callback(CircuitState.PROCESSING, on_state_change)
            controller.register_signal_callback("temperature", on_sensor_data)
            
            # 启动控制器
            await controller.start()
            
            # 转换到活动状态
            await controller.emit_event(Event(
                event_type=EventType.STATE_TRANSITION,
                source="integration_test",
                data={'new_state': CircuitState.ACTIVE}
            ))
            
            # 模拟传感器数据
            await controller.set_signal("temperature", 20.0, SignalType.ANALOG, channel=0)
            await controller.set_signal("humidity", 45.0, SignalType.ANALOG, channel=1)
            
            # 转换到处理状态
            await controller.emit_event(Event(
                event_type=EventType.STATE_TRANSITION,
                source="integration_test",
                data={'new_state': CircuitState.PROCESSING}
            ))
            
            # 温度升高触发控制逻辑
            await controller.set_signal("temperature", 30.0, SignalType.ANALOG, channel=0)
            
            # 等待所有处理完成
            await asyncio.sleep(0.2)
            
            # 验证工作流程
            assert workflow_state['initialized'] is True
            assert workflow_state['sensors_activated'] is True
            assert workflow_state['processing_started'] is True
            assert workflow_state['output_generated'] is True
            
            # 验证最终状态
            fan_signal = controller.get_signal("cooling_fan")
            assert fan_signal is not None
            assert fan_signal.value is True
            
            # 验证统计信息
            stats = controller.get_stats()
            assert stats['signals_processed'] >= 4  # 至少处理了4个信号
            assert stats['state_transitions'] >= 3  # 至少3次状态转换
            
        finally:
            await controller.stop()


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])