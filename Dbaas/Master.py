#!/usr/bin/env python
import pika
import json
import pymongo

myclient = pymongo.MongoClient("mongodb://192.168.0.107:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]



connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()
channel.queue_declare(queue='rpc_queue_write')

response = ''
corr_id = ''



def WriteToDB(data,rawdata):
    returnCode = 200
    if (data["method"] == "write"):
        collection = data["table"]
        insert_data = data["data"]
        if collection == "userDB":
            userDB.insert_one(insert_data)
        elif collection == "rideDB":
            rideDB.insert_one(insert_data)
        else:
            returnCode = 400
        returnCode = 200
    elif (data["method"] == "delete"):
        collection = data["table"]
        delete_data = data["data"]
        if collection == "userDB":
            userDB.delete_many(delete_data)
        elif collection == "rideDB":
            rideDB.delete_many(delete_data)
        else:
            returnCode = 400
        returnCode = 200
    elif (data["method"] == "update"):
        collection = data["table"]
        if collection == "userDB":
            userDB.update_one(data["query"],data["insert"])
        elif collection == "rideDB":
            rideDB.update_one(data["query"],data["insert"])
        else:
            returnCode = 400
        returnCode = 200
    else:
        returnCode = 400
    #Write to sync queue and return returnCode
    if returnCode == 200 :
        syncChannel = connection.channel()
        #syncChannel.queue_declare(queue='SYNCQ')
        syncChannel.exchange_declare(exchange='logs',exchange_type='fanout')
        print("SYNCQ DECLARE")
        print(data,type(data))
        syncChannel.basic_publish(exchange='logs',routing_key='',body=rawdata)
        print("SYNCQ PUBLISH")
    return returnCode


def on_request(ch, method, props, body):
    n1 = body
    n = json.loads(body.decode("utf-8"))
    print("n : ",n)
    res = WriteToDB(n,n1)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=str(res))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue_write', on_message_callback=on_request)
channel.start_consuming()
