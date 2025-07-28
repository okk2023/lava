#!/usr/bin/env python3
"""
异步电路控制器演示脚本

这个脚本演示了如何在实际场景中使用异步电路控制器，
包括模拟一个智能家居控制系统，具有温度控制、安全监控和设备管理功能。

运行方式:
    python examples/async_circuit_demo.py
"""

import asyncio
import random
import sys
import os
import signal
from datetime import datetime

# 添加src路径
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


class SmartHomeController:
    """智能家居控制器演示类"""
    
    def __init__(self):
        # 创建异步电路控制器
        self.controller = create_async_circuit_controller("SmartHome", max_channels=8)
        
        # 系统状态
        self.target_temperature = 22.0
        self.security_armed = False
        self.emergency_shutdown = False
        
        # 设备状态追踪
        self.device_states = {
            'heating': False,
            'cooling': False,
            'security_alarm': False,
            'main_lights': False,
            'garden_lights': False,
            'garage_door': False
        }
        
        # 传感器数据
        self.sensor_data = {
            'indoor_temp': 20.0,
            'outdoor_temp': 15.0,
            'humidity': 45.0,
            'motion_detected': False,
            'door_open': False,
            'window_open': False
        }
    
    async def setup(self):
        """设置控制器和回调"""
        print("🏠 设置智能家居控制系统...")
        
        # 注册状态变化回调
        self.controller.register_state_callback(CircuitState.ACTIVE, self._on_system_active)
        self.controller.register_state_callback(CircuitState.ERROR, self._on_system_error)
        
        # 注册信号回调
        self.controller.register_signal_callback("indoor_temperature", self._on_temperature_change)
        self.controller.register_signal_callback("motion_sensor", self._on_motion_detected)
        self.controller.register_signal_callback("door_sensor", self._on_door_change)
        self.controller.register_signal_callback("emergency_button", self._on_emergency)
        
        # 启动控制器
        await self.controller.start()
        print("✅ 智能家居控制系统启动完成")
    
    async def run_demo(self):
        """运行演示"""
        print("\n🎬 开始智能家居演示...")
        
        try:
            # 激活系统
            await self._activate_system()
            
            # 设置定期任务
            await self._setup_periodic_tasks()
            
            # 运行各种场景
            await self._demo_scenarios()
            
        except KeyboardInterrupt:
            print("\n⏹️  演示被用户中断")
        except Exception as e:
            print(f"\n❌ 演示过程中出现错误: {e}")
        finally:
            await self.cleanup()
    
    async def _activate_system(self):
        """激活系统"""
        print("\n📡 激活智能家居系统...")
        
        # 转换到活动状态
        await self.controller.emit_event(Event(
            event_type=EventType.STATE_TRANSITION,
            source="smart_home",
            data={'new_state': CircuitState.ACTIVE}
        ))
        
        # 等待状态转换
        await asyncio.sleep(0.1)
        
        # 初始化所有传感器和设备
        await self._initialize_devices()
    
    async def _initialize_devices(self):
        """初始化设备和传感器"""
        print("🔧 初始化设备和传感器...")
        
        # 设置初始传感器值
        await self.controller.set_signal("indoor_temperature", self.sensor_data['indoor_temp'], 
                                        SignalType.ANALOG, channel=0)
        await self.controller.set_signal("outdoor_temperature", self.sensor_data['outdoor_temp'], 
                                        SignalType.ANALOG, channel=1)
        await self.controller.set_signal("humidity", self.sensor_data['humidity'], 
                                        SignalType.ANALOG, channel=2)
        await self.controller.set_signal("motion_sensor", False, 
                                        SignalType.DIGITAL, channel=3)
        await self.controller.set_signal("door_sensor", False, 
                                        SignalType.DIGITAL, channel=4)
        await self.controller.set_signal("window_sensor", False, 
                                        SignalType.DIGITAL, channel=5)
        
        # 初始化设备控制信号
        await self.controller.set_signal("heating_system", False, 
                                        SignalType.DIGITAL, channel=6)
        await self.controller.set_signal("cooling_system", False, 
                                        SignalType.DIGITAL, channel=7)
        
        print("✅ 设备初始化完成")
    
    async def _setup_periodic_tasks(self):
        """设置定期任务"""
        print("⏰ 设置定期任务...")
        
        # 温度监控任务
        await self.controller.add_periodic_task(
            "temperature_monitor", 
            2.0, 
            self._monitor_temperature
        )
        
        # 安全监控任务
        await self.controller.add_periodic_task(
            "security_monitor", 
            3.0, 
            self._monitor_security
        )
        
        # 系统状态报告任务
        await self.controller.add_periodic_task(
            "status_report", 
            10.0, 
            self._system_status_report
        )
        
        print("✅ 定期任务设置完成")
    
    async def _demo_scenarios(self):
        """演示各种场景"""
        scenarios = [
            ("🌡️  温度控制演示", self._temperature_control_demo),
            ("🏠 安全系统演示", self._security_system_demo),
            ("💡 智能照明演示", self._smart_lighting_demo),
            ("🚨 应急响应演示", self._emergency_response_demo),
            ("📊 系统监控演示", self._monitoring_demo)
        ]
        
        for scenario_name, scenario_func in scenarios:
            print(f"\n{scenario_name}")
            print("-" * 50)
            await scenario_func()
            await asyncio.sleep(3)  # 场景间隔
    
    async def _temperature_control_demo(self):
        """温度控制演示"""
        print("设置目标温度为 24°C")
        self.target_temperature = 24.0
        
        # 模拟温度变化
        temperatures = [18.0, 20.0, 22.0, 25.0, 27.0, 26.0, 24.0, 23.0]
        
        for temp in temperatures:
            self.sensor_data['indoor_temp'] = temp
            await self.controller.set_signal("indoor_temperature", temp, 
                                            SignalType.ANALOG, channel=0)
            print(f"📊 室内温度: {temp}°C")
            await asyncio.sleep(0.5)
        
        print("🎯 温度控制演示完成")
    
    async def _security_system_demo(self):
        """安全系统演示"""
        print("🔒 启动安全系统")
        self.security_armed = True
        
        # 模拟运动检测
        await asyncio.sleep(1)
        print("👤 检测到运动")
        await self.controller.set_signal("motion_sensor", True, 
                                        SignalType.DIGITAL, channel=3)
        
        await asyncio.sleep(2)
        print("🚪 检测到门被打开")
        await self.controller.set_signal("door_sensor", True, 
                                        SignalType.DIGITAL, channel=4)
        
        await asyncio.sleep(1)
        print("🔓 解除安全系统")
        self.security_armed = False
        
        await asyncio.sleep(1)
        print("🚪 门已关闭")
        await self.controller.set_signal("door_sensor", False, 
                                        SignalType.DIGITAL, channel=4)
        
        print("🔐 安全系统演示完成")
    
    async def _smart_lighting_demo(self):
        """智能照明演示"""
        print("💡 智能照明系统演示")
        
        # 模拟不同时间的照明控制
        lighting_scenarios = [
            ("早晨", "室内灯光开启", True),
            ("白天", "室内灯光关闭", False),
            ("傍晚", "花园灯光开启", True),
            ("夜晚", "安全照明开启", True)
        ]
        
        for time_period, action, state in lighting_scenarios:
            print(f"🕐 {time_period}: {action}")
            if "室内" in action:
                await self.controller.set_signal("main_lights", state, 
                                                SignalType.DIGITAL, channel=0)
                self.device_states['main_lights'] = state
            elif "花园" in action:
                await self.controller.set_signal("garden_lights", state, 
                                                SignalType.DIGITAL, channel=1)
                self.device_states['garden_lights'] = state
            
            await asyncio.sleep(1)
        
        print("🌟 智能照明演示完成")
    
    async def _emergency_response_demo(self):
        """应急响应演示"""
        print("🚨 应急响应系统演示")
        
        print("⚠️  模拟紧急情况（按下应急按钮）")
        await self.controller.set_signal("emergency_button", True, 
                                        SignalType.DIGITAL, channel=2)
        
        await asyncio.sleep(2)
        
        print("✅ 应急状态解除")
        await self.controller.set_signal("emergency_button", False, 
                                        SignalType.DIGITAL, channel=2)
        self.emergency_shutdown = False
        
        print("🔄 应急响应演示完成")
    
    async def _monitoring_demo(self):
        """系统监控演示"""
        print("📊 系统监控数据展示")
        
        # 获取控制器统计信息
        stats = self.controller.get_stats()
        
        print(f"📈 系统运行时间: {stats['uptime']:.1f} 秒")
        print(f"📊 处理事件数: {stats['events_processed']}")
        print(f"📡 处理信号数: {stats['signals_processed']}")
        print(f"🔄 状态转换数: {stats['state_transitions']}")
        print(f"⚠️  错误数量: {stats['errors_count']}")
        print(f"📟 活动信号数: {stats['signal_count']}")
        print(f"⏲️  活动定时器: {stats['active_timers']}")
        
        # 显示设备状态
        print("\n🏠 设备状态:")
        for device, state in self.device_states.items():
            status = "🟢 开启" if state else "🔴 关闭"
            print(f"  {device}: {status}")
        
        # 显示传感器数据
        print("\n📊 传感器数据:")
        for sensor, value in self.sensor_data.items():
            if isinstance(value, bool):
                status = "🟢 是" if value else "🔴 否"
                print(f"  {sensor}: {status}")
            else:
                print(f"  {sensor}: {value}")
    
    # 回调函数
    def _on_system_active(self, old_state, new_state):
        """系统激活回调"""
        print(f"🚀 系统状态变化: {old_state.name} -> {new_state.name}")
    
    def _on_system_error(self, old_state, new_state):
        """系统错误回调"""
        print(f"❌ 系统进入错误状态: {old_state.name} -> {new_state.name}")
    
    async def _on_temperature_change(self, signal: Signal):
        """温度变化回调"""
        temp = signal.value
        self.sensor_data['indoor_temp'] = temp
        
        # 温度控制逻辑
        if temp < self.target_temperature - 1.0:
            if not self.device_states['heating']:
                print(f"🔥 启动加热系统 (当前: {temp}°C, 目标: {self.target_temperature}°C)")
                await self.controller.set_signal("heating_system", True, 
                                                SignalType.DIGITAL, channel=6)
                self.device_states['heating'] = True
                self.device_states['cooling'] = False
        
        elif temp > self.target_temperature + 1.0:
            if not self.device_states['cooling']:
                print(f"❄️  启动冷却系统 (当前: {temp}°C, 目标: {self.target_temperature}°C)")
                await self.controller.set_signal("cooling_system", True, 
                                                SignalType.DIGITAL, channel=7)
                self.device_states['cooling'] = True
                self.device_states['heating'] = False
        
        else:
            # 温度合适，关闭所有系统
            if self.device_states['heating'] or self.device_states['cooling']:
                print(f"✅ 温度达到目标，关闭温控系统 (当前: {temp}°C)")
                await self.controller.set_signal("heating_system", False, 
                                                SignalType.DIGITAL, channel=6)
                await self.controller.set_signal("cooling_system", False, 
                                                SignalType.DIGITAL, channel=7)
                self.device_states['heating'] = False
                self.device_states['cooling'] = False
    
    async def _on_motion_detected(self, signal: Signal):
        """运动检测回调"""
        motion = signal.value
        self.sensor_data['motion_detected'] = motion
        
        if motion and self.security_armed:
            print("🚨 安全警报: 检测到可疑活动!")
            await self.controller.set_signal("security_alarm", True, 
                                            SignalType.DIGITAL, channel=3)
            self.device_states['security_alarm'] = True
            
            # 设置警报定时器
            async def clear_alarm():
                await asyncio.sleep(5)
                print("🔕 安全警报解除")
                await self.controller.set_signal("security_alarm", False, 
                                                SignalType.DIGITAL, channel=3)
                self.device_states['security_alarm'] = False
            
            await self.controller.set_timer("alarm_timer", 0.1, clear_alarm)
    
    async def _on_door_change(self, signal: Signal):
        """门状态变化回调"""
        door_open = signal.value
        self.sensor_data['door_open'] = door_open
        
        if door_open:
            print("🚪 门已打开")
            if self.security_armed:
                print("⚠️  安全系统已启动，检查门状态")
        else:
            print("🚪 门已关闭")
    
    async def _on_emergency(self, signal: Signal):
        """应急按钮回调"""
        emergency = signal.value
        
        if emergency:
            print("🆘 应急按钮被按下! 启动应急程序...")
            self.emergency_shutdown = True
            
            # 关闭所有非必要系统
            await self.controller.set_signal("heating_system", False, SignalType.DIGITAL)
            await self.controller.set_signal("cooling_system", False, SignalType.DIGITAL)
            
            # 打开所有照明
            await self.controller.set_signal("main_lights", True, SignalType.DIGITAL)
            await self.controller.set_signal("garden_lights", True, SignalType.DIGITAL)
            
            print("🚨 应急程序已激活: 关闭非必要系统，开启所有照明")
    
    # 定期任务
    async def _monitor_temperature(self):
        """温度监控任务"""
        # 模拟温度传感器读取
        if not self.emergency_shutdown:
            # 添加一些随机波动
            variation = random.uniform(-0.5, 0.5)
            new_temp = self.sensor_data['indoor_temp'] + variation
            
            # 限制温度范围
            new_temp = max(15.0, min(35.0, new_temp))
            
            if abs(new_temp - self.sensor_data['indoor_temp']) > 0.1:
                await self.controller.set_signal("indoor_temperature", new_temp, 
                                                SignalType.ANALOG, channel=0)
    
    async def _monitor_security(self):
        """安全监控任务"""
        if self.security_armed and not self.emergency_shutdown:
            # 随机模拟安全事件
            if random.random() < 0.1:  # 10% 概率
                print("🔍 安全系统扫描中...")
    
    async def _system_status_report(self):
        """系统状态报告任务"""
        if not self.emergency_shutdown:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"📋 [{current_time}] 系统状态正常，所有系统运行中...")
    
    async def cleanup(self):
        """清理资源"""
        print("\n🧹 清理系统资源...")
        await self.controller.stop()
        print("✅ 系统已安全关闭")


async def main():
    """主函数"""
    print("🏠 智能家居异步电路控制器演示")
    print("=" * 60)
    print("这个演示展示了异步电路控制器在智能家居系统中的应用")
    print("包括温度控制、安全监控、智能照明和应急响应等功能")
    print("=" * 60)
    
    # 创建智能家居控制器
    smart_home = SmartHomeController()
    
    # 设置信号处理器（用于优雅关闭）
    def signal_handler(signum, frame):
        print("\n🛑 接收到停止信号，正在安全关闭系统...")
        raise KeyboardInterrupt()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 设置和运行演示
        await smart_home.setup()
        await smart_home.run_demo()
        
    except Exception as e:
        print(f"❌ 演示过程中发生错误: {e}")
    finally:
        await smart_home.cleanup()
        print("\n👋 感谢使用智能家居异步电路控制器演示!")


if __name__ == "__main__":
    # 运行演示
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 演示已结束!")
    except Exception as e:
        print(f"\n❌ 启动演示时发生错误: {e}")