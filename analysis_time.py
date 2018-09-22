import time
import os
import sched

cmd_str1 = "spark-submit --class edu.bupt.iot.spark.common.RecentData /home/spark/iot.jar"
cmd_str2 = "spark-submit --class edu.bupt.iot.spark.common.RecentDevice /home/spark/iot.jar"

# 初始化sched模块的scheduler类
# 第一个参数是一个可以返回时间戳的函数，第二个参数可以在定时未到达之前阻塞。
schedule = sched.scheduler(time.time, time.sleep)

# 被周期性调度触发的函数
def execute_command(cmd1, cmd2, inc):
    os.system(cmd1)
    os.system(cmd2)
    schedule.enter(inc, 0, execute_command, (cmd1, cmd2, inc))

def main(cmd1, cmd2, inc=3600 * 24):
    # enter四个参数分别为：间隔事件、优先级（用于同时间到达的两个事件同时执行时定序）、被调用触发的函数，  
    # 给该触发函数的参数（tuple形式)
    while(True):
        hour = time.localtime(time.time()).tm_hour
        if hour == 22:
            schedule.enter(0, 0, execute_command, (cmd1, cmd2, inc))
            schedule.run()
        else:
            time.sleep(2400)

# 每60秒查看下网络连接情况
if __name__ == '__main__':
    main(cmd_str1, cmd_str2, 3600 * 24)