import os, sys, time

USER = os.path.expanduser('~').split('/')[2]
PROJECT_BASE = os.path.expanduser('~/projects/quant')


def start_tmp(tmp_file):
    '''启动临时任务'''
    os.system(f'cd {PROJECT_BASE}')
    ret = os.system(f'nohup python3 ./{tmp_file}.py >> ./src/log/tmp_jobs.log &')
    if ret == 0:
        print('tmp tasks start successed!')
    else:
        raise Exception('tmp tasks start failed!')

def start_xxljob():
    # admin：java -jar /home/baixianzheng/projects/java/xxl-job-2.1.2/xxl-job-admin/target/xxl-job-admin-2.1.2.jar
    # executor：java -jar /home/baixianzheng/projects/java/xxl-job-2.1.2/xxl-job-executor-samples/xxl-job-executor-sample-springboot/target/xxl-job-executor-sample-springboot-2.1.2.jar
    cmd_admin = 'java -jar ./cmd/xxl-job-admin-2.1.2.jar'      # 默认端口 8080 
    cmd_executor = 'java -jar ./cmd/xxl-job-admin-2.1.2.jar'   # 默认端口 8081

    os.system(f'cd {PROJECT_BASE}')
    os.system(cmd_admin)
    os.system(cmd_executor)

def main():
    os.system(f'cd {PROJECT_BASE}')
    arg = sys.argv[1]
    if arg == 'jupyter':
        if USER == 'ark':
            cmd = 'nohup jupyter notebook --port 8889 --ip 10.254.254.111 >> ./src/log/jupyter.log &'
        else:
            cmd = 'nohup jupyter notebook --port 8888 --ip 10.254.254.110 >> ./src/log/jupyter.log &'
        ret = os.system(cmd)
    elif arg == 'lf':
        cmd = 'nohup python3 update.py -T lf >> ./src/log/main_lf.log &'
        ret = os.system(cmd)
    elif arg == 'hf':
        cmd = 'nohup python3 update.py -T hf >> ./src/log/main_lf.log &'
        ret = os.system(cmd)
    elif arg == 'tmp1':
        cmd = 'nohup python3 ./tmp1.py >> ./src/log/tmp_jobs.log &'
        ret = os.system(cmd)
    elif arg == 'tmp2':
        cmd = 'nohup python3 ./tmp2.py >> ./src/log/tmp_jobs.log &'
        ret = os.system(cmd)
    elif arg == 'xxl-job':
        cmd_admin = 'nohup java -jar ./cmd/xxl-job-admin-2.1.2.jar >> ./src/log/xxl-admin.log &'   
        cmd_executor = 'nohup java -jar ./cmd/xxl-job-executor-sample-springboot-2.1.2.jar >> ./src/log/xxl-executor.log &'   
        ret = os.system(cmd_admin)
        time.sleep(5)
        ret = os.system(cmd_executor)
    else:
        raise Exception(f'argv: {arg} is invalid!')

    if ret == 0:
        print(f'{arg} start successed!')
    else:
        raise Exception(f'{arg} start failed!')


if __name__ == '__main__':
    main()