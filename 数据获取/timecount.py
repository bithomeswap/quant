import time
import sched
import subprocess

# 创建scheduler对象
schedule = sched.scheduler(time.time, time.sleep)

# 要延迟执行的任务


def task():
    print("延迟执行的任务")
    # 任务执行结束后，调用另一个Python文件中的程序
    subprocess.call(['python', "数据获取\\tradelist.py"])


# 计算执行时间
now = time.time()
target_time = now + 10000 # 一小时后，3600秒

# 安排任务
schedule.enterabs(target_time, 1, task, [])

# 运行scheduler
schedule.run()
