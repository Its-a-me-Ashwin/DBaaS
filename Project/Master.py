#!/usr/bin/env python
import pika
import json
import pymongo
import threading
import time


lock = threading.Lock()
print("MASTER STARTED", flush = True)
myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]
userDB.create_index("username", unique = True)
rideDB.create_index("rideId", unique = True)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()
channel.queue_declare(queue='rpc_queue_write')


response = ''
corr_id = ''





def update():
    print("\nThread Has Started\n",flush=True)
    newconnection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
    syncChannel = newconnection.channel()
    while True:
        print("\n\n\nSyncing New Slaves\n\n\n",flush=True)
        syncChannel.exchange_declare(exchange='logs',exchange_type='fanout')
        tuple = {"method" : "write","table" : "userDB","data" : {"username":"xyz","password":"abc"}}
        newtp = {}
        newtp["method"] = "write"
        newtp["table"] = "userDB"
        newtp["data"] = {}
        #lock.acquire()
        for tuple in userDB.find() :
            newtp["data"]["username"] = tuple["username"]
            newtp["data"]["password"] = tuple["password"]
            syncChannel.basic_publish(exchange='logs',routing_key='',body=json.dumps(newtp))
        #lock.release()
        newtp["table"] = "rideDB"
        #lock.acquire()
        for tuple in rideDB.find() :
            newtp["data"]["rideId"] = tuple["rideId"]
            newtp["data"]["created_by"] = tuple["created_by"]
            newtp["data"]["timestamp"] = tuple["timestamp"]
            newtp["data"]["source"] = tuple["source"]
            newtp["data"]["destination"] = tuple["destination"]
            newtp["data"]["users"] = tuple["users"]
            syncChannel.basic_publish(exchange='logs',routing_key='',body=json.dumps(newtp))
        #lock.release()
        time.sleep(90)

updatethread = threading.Thread(target=update)
updatethread.start()


def WriteToDB(data,rawdata):
    print("MASTER WRITING")
    returnCode = 200
    if (data["method"] == "write"):
        collection = data["table"]
        insert_data = data["data"]
        if collection == "userDB":
            try:
                userDB.insert_one(insert_data)
            except:
                print("probably duplicate key error",flush=True)
        elif collection == "rideDB":
            try:
                rideDB.insert_one(insert_data)
            except:
                print("probably duplicate key error",flush=True)
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
        global syncChannel
        syncChannel = connection.channel()
        syncChannel.exchange_declare(exchange='logs',exchange_type='fanout')
        print(data,type(data))
        #lock.aquire()
        syncChannel.basic_publish(exchange='logs',routing_key='',body=rawdata)
        #lock.release()
        print("SYNCQ PUBLISH")

    #return "WROTE DATA"
    reslist = []
    for x in userDB.find() :
        	reslist.append(x)
    return reslist


def on_request(ch, method, props, body):
    print("MASTER ON REQUEST",end="\n")
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
