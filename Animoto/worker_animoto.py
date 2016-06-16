
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
import boto.s3.connection
import os
from boto.s3.key import Key

def initqueue(queue_name, pqueue_name):
    aws_conn=boto.sqs.connect_to_region("us-east-1", aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}')
    dynamo_conn = boto.dynamodb.connect_to_region("us-east-1", aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}')
    s3_conn = boto.connect_s3( aws_access_key_id='{aws_access_key_id}', aws_secret_access_key = '{aws_secret_access_key}', is_secure=False, calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    SQS_queue=aws_conn.get_queue(queue_name)
    SQS_process_queue=aws_conn.get_queue(pqueue_name)
    Dynamo_table = dynamo_conn.get_table('Dynamo_Table')
    try:
        s3_bucket=s3_conn.create_bucket('animoto_bucket')
    except Exception as e:
        s3_bucket=s3_conn.get_bucket('animoto_bucket')

    return SQS_queue, SQS_process_queue,Dynamo_table, s3_bucket ;



def getandprocesstask(task_queue, process_queue, table, s3_bucket, no_of_images):
    is_dup=0
    if(no_of_images < 60):
    	try:
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
                exe = 'wget ' + str(json_msg["task"])
                os.system(exe)
            else:
                print 'Duplicate task: ' + key
        except Exception as e:
            print "Listening";
            exec 'time.sleep(1)'
            getandprocesstask(task_queue, process_queue, table, s3_bucket, no_of_images)


    if(no_of_images==60):
        os.system("./setup.sh")
        tempcommand = 'mv output.mpg 0.mpg'
        os.system(tempcommand)
        k = Key(s3_bucket)
        k.Key = "0.mpg"
        k.set_contents_from_filename("0.mpg")
        k.set_canned_acl('public-read')
        video_url = k.generate_url(0, query_auth=False, force_http=True)
        processtask(0, process_queue, video_url)
	tempcommand = 'rm -rf 0.mpg'
        os.system(tempcommand)


def processtask(json_msg, process_queue, video_url):
    try:
        msg = Message()
        process_msg = {}
        process_msg["task_id"] = json_msg
        process_msg["task"] = video_url
        msg.set_body(json.dumps(process_msg))
        process_queue.write(msg);
    except Exception as e:
        print e;



def main(argv):
    no_of_threads = ''
    is_local = ''
    try:
        opts, args = getopt.getopt(argv,"s:",["is_local="])
    except getopt.GetoptError:
        print 'workera.py -s LOCAL'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-s':
            queue_name= arg

    SQS_queue, SQS_process_queue, Dynamo_table, s3_bucket =initqueue(queue_name, 'PROCESS_QUEUE');
    no_of_images = 0
    no_of_iterations = 0
    while no_of_iterations < 160:
	no_of_images = 0;
	while no_of_images <= 60:
            getandprocesstask(SQS_queue,SQS_process_queue, Dynamo_table, s3_bucket, no_of_images)
    	    no_of_images = no_of_images + 1
	no_of_iterations = no_of_iterations + 1



if __name__ == "__main__":
    main(sys.argv[1:])



# In[ ]:
