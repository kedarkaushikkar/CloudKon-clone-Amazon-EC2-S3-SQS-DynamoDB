
# coding: utf-8

# In[1]:

import Queue
import json
import sys
import threading
import time
import getopt
import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb

def initqueue(queue_name, pqueue_name):
    aws_conn=boto.sqs.connect_to_region("us-east-1", aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}')
    dynamo_conn = boto.dynamodb.connect_to_region("us-east-1", aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}')
    SQS_queue=aws_conn.get_queue(queue_name)
    SQS_process_queue=aws_conn.get_queue(pqueue_name)
    Dynamo_table = dynamo_conn.get_table('Dynamo_Table')
    return SQS_queue, SQS_process_queue,Dynamo_table ;

class initializethread(threading.Thread):
    def __init__(self,data):
        threading.Thread.__init__(self);
        self.data=data
        #self.thread = threading.Thread(target=self.run)

    def run(self):
        task1=self.data["task"].split(' ')
        task2=task1[0]
        task3=float(task1[1])
        try:
            ex1 = 'time.'+(task2)+'('+str(float(task3/1000))+')'
            exec ex1
            #print "Execute task : " , ex1
        except Exception as e:
            print e;

def getandprocesstask(task_queue, process_queue, no_of_threads, table):
    is_dup=0
    try:
        threads=[]
        msg=task_queue.get_messages()
        task_msg=msg[0].get_body()
        json_msg=json.loads(task_msg)
        task_queue.delete_message(msg[0])
        key = str(json_msg["task_id"])
        try:
            element = table.get_item(hash_key = key)
            is_dup=1;
        except Exception:
            is_dup=0

        if is_dup==0:
            element_data = {'Body': 'True'}
            element = table.new_item(hash_key = key, attrs = element_data)
            element.put()
            for a in range(no_of_threads):
                t1=json_msg
                thread=initializethread(t1)
                threads.append(thread)
            for xx in threads:
                xx.start();
            for xx in threads:
                xx.join();
            processtask(json_msg, process_queue)
        else:
            print 'This is duplicate task. Task ID : ' + key

    except Exception as e:
        print "listening to queue";
        exec 'time.sleep(1)'
        getandprocesstask(task_queue, process_queue, no_of_threads, table)



def processtask(json_msg, process_queue):
    try:
        msg = Message()
        process_msg = {}
        process_msg["task_id"] = json_msg["task_id"]
        process_msg["task"] = 1
	#writting process files in output files.
	f = open('process_task.txt','a')
	f.write(str(process_msg["task_id"]) + ' Successful\n' )
        msg.set_body(json.dumps(process_msg))
        process_queue.write(msg);
    except Exception as e:
        print e;



def main(argv):
    no_of_threads = ''
    is_local = ''
    opts, args = getopt.getopt(argv,"s:t:",["is_local=","no_of_threads="])
    for opt, arg in opts:
        if opt == '-t':
            no_of_threads= int(arg)
        elif opt == '-s':
            queue_name= arg

    SQS_queue, SQS_process_queue, Dynamo_table =initqueue(queue_name, 'PROCESS_QUEUE');
    while True:
        getandprocesstask(SQS_queue,SQS_process_queue, no_of_threads, Dynamo_table)



if __name__ == "__main__":
    main(sys.argv[1:])



# In[ ]:
