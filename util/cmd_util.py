import subprocess

def cmd(cmd_arg_list):
    """
    :param cmd_arg_list:参数列表
    :return: statue code 等待执行结束, 返回状态码
    """
    return subprocess.call(cmd_arg_list)

def exec_cmd(cmd_arg_list):
    """
    :param cmd_arg_list:  参数列表
    :return: 实时返回stdout与stderr
    """
    popen = subprocess.Popen(cmd_arg_list, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    while True:
        line = popen.stdout.readline().strip().decode('utf-8') # 获取内容
        if line:
            yield (line)
        else:
            break

if __name__ == "__main__":
    cmd_arg_list = ['spark-submit', '--class', 'spark.ReadCSV', 'MySpark.jar']
    exec_cmd(cmd_arg_list)