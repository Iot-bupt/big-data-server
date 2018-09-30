import time
import os
import sched

cmd_str1 = "spark-submit --class edu.bupt.iot.spark.common.RecentData /home/spark/iot.jar"
cmd_str2 = "spark-submit --class edu.bupt.iot.spark.common.RecentDevice /home/spark/iot.jar"

# cmd_str1 = "ls"
# cmd_str2 = "ls -lh"

# 初始化sched模块的scheduler类
# 第一个参数是一个可以返回时间戳的函数，第二个参数可以在定时未到达之前阻塞。
schedule = sched.scheduler(time.time, time.sleep)

def get_wait_time(now_time):
    return ((now_time // (3600 * 24) + 1) * (3600 * 24) - (3600 * 8) - now_time)  % (3600 * 24)

# 被周期性调度触发的函数
def execute_command(cmd1, cmd2):
    os.system(cmd1)
    os.system(cmd2)
    time.sleep(60)
    now_time = int(time.time())
    #print((now_time // (3600 * 24) + 1) * (3600 * 24) - now_time)
    schedule.enter(get_wait_time(now_time), 0, execute_command, (cmd1, cmd2))
    #schedule.enter(2, 0, execute_command, (cmd1, cmd2))

def main(cmd1, cmd2):
    # enter四个参数分别为：间隔事件、优先级（用于同时间到达的两个事件同时执行时定序）、被调用触发的函数，
    # 给该触发函数的参数（tuple形式)
    now_time = int(time.time())
    schedule.enter(get_wait_time(now_time), 0, execute_command, (cmd1, cmd2 ))
    schedule.run()

if __name__ == '__main__':
    #print(time.time())
    main(cmd_str1, cmd_str2)