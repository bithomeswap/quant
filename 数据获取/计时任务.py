import time
import sched
import subprocess
# 创建scheduler对象
schedule = sched.scheduler(time.time, time.sleep)
# 要延迟执行的任务


def task1():
    print("延迟执行的任务")
    # 任务执行结束后，调用另一个Python文件中的程序
    subprocess.call(["python", "数据获取\\get股票拼接.py"])


def task2():
    print("延迟执行的任务")
    # 任务执行结束后，调用另一个Python文件中的程序
    subprocess.call(["python", "数据获取\\tradelist.py"])


def task3():
    print("延迟执行的任务")
    # 任务执行结束后，调用另一个Python文件中的程序
    subprocess.call(["python", "数据获取\\单指标排名收益分布.py"])


def task4():
    print("延迟执行的任务")
    # 任务执行结束后，调用另一个Python文件中的程序
    subprocess.call(["python", "数据获取\\多指标排名收益分布.py"])


# 计算执行时间
now = time.time()
target_time = now + 7200  # 一小时后，3600秒
# 安排任务
schedule.enterabs(target_time, 1, task1, [])
# 运行scheduler
schedule.run()
# 安排任务
schedule.enterabs(target_time, 1, task2, [])
# 运行scheduler
schedule.run()
# 安排任务
schedule.enterabs(target_time, 1, task3, [])
# 运行scheduler
schedule.run()
# 安排任务
schedule.enterabs(target_time, 1, task4, [])
# 运行scheduler
schedule.run()
