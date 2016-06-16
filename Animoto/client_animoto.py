
# coding: utf-8

# In[1]:

import Queue
import json
import sys
import threading
import time
import getopt
import boto.sqs
import boto.dynamodb
from boto.sqs.message import Message


def readtask(filename):
    task_queue=Queue.Queue()

    with open(filename) as f:
        task_list=f.readlines()

    for i in task_list:
        task_queue.put(i)
    return task_queue;

def readtaskSQS(filename, queue_name, process_queue):
    aws_conn=boto.sqs.connect_to_region("us-east-1", aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}')
    dynamo_conn = boto.dynamodb.connect_to_region("us-east-1", aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}')
    SQS_queue=aws_conn.get_queue(queue_name)
    SQS_process_queue=aws_conn.get_queue(process_queue)
    task_id = 0;


    with open(filename) as f:
        task_list=f.readlines()

    for i in task_list:
        msg = Message()
        json_msg = {}
        json_msg["task_id"] = task_id
        json_msg["task"] = i
        msg.set_body(json.dumps(json_msg))
        SQS_queue.write(msg);
        task_id=task_id+1

    return SQS_queue, SQS_process_queue, task_id;


class initializethread(threading.Thread):
    def __init__(self,data):
        threading.Thread.__init__(self);
        self.data=data

    def run(self):
        task1=self.data.split(' ')
        task2=task1[0]
        task3=float(task1[1])
        try:
            ex1 = 'time.'+(task2)+'('+str(float(task3/1000))+')'
            exec ex1
        except Exception as e:
            print e;

def processlocalthread(task_queue, no_of_threads):
    threads=[]
    c=0;
    while not task_queue.empty():
        for a in range(no_of_threads):
            t1=task_queue.get()
            thread=initializethread(t1)
            threads.append(thread)
    for xx in threads:
        xx.start();
    for xx in threads:
        xx.join();
    return('Complete')


def main(argv):
    no_of_threads = ''
    is_local = ''
    try:
        opts, args = getopt.getopt(argv,"s:t:w:",["is_local=","no_of_threads=","filename="])
    except getopt.GetoptError:
        print 'client.py -s LOCAL/QUEUE NAME -t <N> -w <WORKLOAD_FILE>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-w':
            filename= arg
        elif opt == '-t':
            no_of_threads= int(arg)
        elif opt == '-s':
            is_local= arg


    print 'Animoto Client Started:'
    print "Start Time: ", time.strftime("%H:%M:%S")
    start_time = time.time()
    SQS_queue, SQS_process_queue, no_of_tasks=readtaskSQS(filename, is_local, 'PROCESS_QUEUE');
    while True:
        if(SQS_queue.count()==0):
            process_list = SQS_process_queue.count()*60
            if(process_list == no_of_tasks):
	        end_time=time.time()
		print "Tasks Completed: ", process_list
		print "End Time : ", time.strftime("%H:%M:%S")
                break;
    print "Total Time:", end_time-start_time;


if __name__ == "__main__":
    main(sys.argv[1:])



# In[ ]:
