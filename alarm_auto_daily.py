import subprocess
import json
import datetime
import pymysql
import sys
import logging
import re
import os

DNA_USER = 'TsRog'
DNA_KEY = 'rPGDCveGGpjY'
DNA = f'/usr/local/bin/dna-shell -c {DNA_USER} -k {DNA_KEY}'

# 设置日期格式
date_fmt = "%Y-%m-%d"
date_hour = "%H"
# 创建日志保存目录
log_dir = f"/root/py/log/daily/"
os.makedirs(log_dir, exist_ok=True)

# 设置日志文件名
log_file = os.path.join(log_dir, f"{datetime.datetime.now().strftime(date_fmt)}.log")

# 配置logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        #输出到日志
        logging.FileHandler(log_file),
        #输出到控制台
        logging.StreamHandler()
    ]
)

print("start")
now = datetime.datetime.now()
seven_days_ago = now - datetime.timedelta(hours=12)
formatted_time = seven_days_ago.strftime("%Y-%m-%d")
#formatted_time = seven_days_ago.strftime("%Y-%m-%d")
timestamp = int(seven_days_ago.timestamp())
#timestamp = '1693587606'
#formatted_time = '2023-09-02 01:00:06'

#输出带时间的提示信息打印方法
def print_info(*info):
    print("[",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"]"," ",*info)

#shell命令执行函数
def shell_run(commands="init", stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=20):
    result = subprocess.run(commands, stderr=stderr, stdout=stdout, timeout=timeout, shell=True)
    return {'result': result.stdout.decode().strip('\n'), 'error': result.stderr.decode().strip('\n')}

#获取一体化近七天历史执行记录
def get_axe_info(startTime,status,templateName='',productId='143'):
    param = [{"templateName":f"{templateName}","startTime":f"{startTime}","status":list(status),"productId":f"{productId}"}]
    cmd = f"{DNA} -ac axeops -st AxeOpsFlowService -sv 1 -mn queryFlowList -e product -si '{json.dumps(param)}'"
    logging.info(cmd)
    res = json.loads(shell_run(cmd)['result'])
    if res["code"] != '200':
        logging.info("接口调用失败: %s",res)
        sys.exit(1)
    return res

#入参操作流ID，获取操作流的执行日志
def Query_flow_log(id):
    param = [f"{id}"]
    param = json.dumps(param)
    cmd = f"/usr/local/bin/dna-shell -c TsRog -k rPGDCveGGpjY -ac axeops -st FlowService -sv 1 -mn queryFlowLog -e product -si '{param}'"
    resp = shell_run(cmd)['result']
    resp = json.loads(resp)
    resp = resp["flowInfo"]["actions"][0]
    if resp['status'] == '1':
        resp = resp["stdout"]
    else:
        resp = resp["stderr"]
    return resp

#插入基础sql数据
def insert_sql(res,templateName):
    flowList = res["flowList"]
    for i in flowList:
        flowName = i["flowName"]
        flow_id = i["flowId"].strip()
        sql_cmd=f"INSERT INTO {templateName} (time, process_num) VALUES ('{formatted_time}', 1) ON DUPLICATE KEY UPDATE process_num = COALESCE(process_num, 0) + 1;"
        sql(sql_cmd)
        print(i["executeStatus"])
        if i["executeStatus"] == '1':
            logging.info("%s-执行成功数量+1",flowName)
            sql_cmd=f"update {templateName} set process_num_suc = COALESCE(process_num_suc, 0)  + 1  where time = '{formatted_time}';"
            sql(sql_cmd)
        elif i["executeStatus"] == '2':
            logging.info("%s-执行失败数量+1",flowName)
            sql_cmd=f"update {templateName} set process_num_fail = COALESCE(process_num_fail, 0)  + 1   where time = '{formatted_time}';"
            logging.info(sql_cmd)
            sql(sql_cmd)
        

#数据库函数
def sql(cmd):
    logging.info("插入sql数据")
    # 建立数据库连接
    conn = pymysql.connect(host='124.71.10.189', port=3306, user='root', password='caiyadong1997', db='alarm_auto')
    # 创建游标对象
    cursor = conn.cursor()
    logging.info(cmd)
    # 执行SQL
    
    cursor.execute(cmd)
    # 提交
    cursor.connection.commit()
    # 关闭连接
    conn.close()


class alarm():
    def CPU(self):
        CPU = get_axe_info(timestamp,'12','CPU_High_Temperature_Auto')
        insert_sql(CPU,"CPU_High_Temperature_Auto")
        ip_need_deal_num = 0
        ip_need_deal = ''
        flow_id = '-'
        if len(CPU["flowList"]) != 0:
            for i in CPU["flowList"]:
                flow_id = i["flowId"].strip()
                res = Query_flow_log(flow_id)
                # 匹配IP地址的正则表达式
                pattern = r'后需处理IP信息：\n(.*?)\n\[INFO\]'
                ip = re.findall(pattern,res)
                if len(ip) !=0 :
                    ip_need_deal_num = len(ip[0].strip().split(" "))
                    ip_need_deal = ip[0].strip().replace(" ",";")
                    logging.info("需处理IP：%s",ip_need_deal)
        sql_cmd = f"update CPU_High_Temperature_Auto set ip_need_deal_num = COALESCE(ip_need_deal_num, 0)  + {ip_need_deal_num} , ip_need_deal = CONCAT(IFNULL(ip_need_deal, ''), '{ip_need_deal}') ,flow_id = CONCAT(IFNULL(flow_id, ''), '{flow_id}') where time = '{formatted_time}';"
        sql(sql_cmd)


    def MEM(self):
        MEM = get_axe_info(timestamp,'12','gdp_hardware_serstatus_pavo')
        insert_sql(MEM,"gdp_hardware_serstatus_pavo")
        denggu = denggu_fail = []
        ip_need_deal_num = 0
        ip_need_deal_suc = ''
        if len(MEM["flowList"]) != 0:
            for i in MEM["flowList"]:
                flow_id = i["flowId"].strip()
                res = Query_flow_log(flow_id)
                mem_err_nums = re.findall(r"valuesSize\":(\d+)\}", res)
                if len(mem_err_nums) != 0:
                    ip_need_deal_num = mem_err_nums[0]
                    logging.info("当日内存报错IP数量：%s",mem_err_nums)
                hw_suc = re.findall(r"IP-流水ID:#([^\n]+)#", res)
                if len(hw_suc) != 0:
                    for i in hw_suc:
                        denggu.append(i.replace('#','-'))
                    logging.info(i)
                rmp_suc = re.findall(r"\]([^ ]+)-RMP接口登记故障成功:", res)
                if len(rmp_suc) != 0:
                    denggu_fail.append(rmp_suc[0])
                    logging.info(rmp_suc[0])
        logging.info("登记降级故障成功IP：%s",denggu)
        logging.info("登记降级故障失败IP：%s",denggu_fail)
        ip_need_deal_suc_num = len(denggu)
        for i in denggu:
            ip_need_deal_suc = i + ";" + ip_need_deal_suc 
        sql_cmd = f"update gdp_hardware_serstatus_pavo set ip_need_deal_num = COALESCE(ip_need_deal_num, 0)  + {ip_need_deal_num},ip_need_deal_suc_num = COALESCE(ip_need_deal_suc_num, 0)  + {ip_need_deal_suc_num} , ip_need_deal_suc = CONCAT(IFNULL(ip_need_deal_suc, ''), '{ip_need_deal_suc}'),flow_id = CONCAT(IFNULL(flow_id, ''), '{flow_id}') where time = '{formatted_time}';"
        sql(sql_cmd)
    
    def run(self):
        self.CPU()
        #self.MEM()

run = alarm()
run.run()

