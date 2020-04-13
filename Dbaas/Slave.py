#!/usr/bin/env python
import pika
import json
import pymongo
from flask import jsonify

myclient = pymongo.MongoClient("mongodb://192.168.0.107:27018/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]


connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()
channel.queue_declare(queue='rpc_queue')

syncChannel = connection.channel()
syncChannel.exchange_declare(exchange='logs',exchange_type='fanout')

syncResult = syncChannel.queue_declare(queue='',exclusive=True)
queue_name = syncResult.method.queue
syncChannel.queue_bind(exchange='logs',queue=queue_name)

def ReadFromDB(data):
    print("Data got",data)
    collection = data["table"]
    columns = data["columns"]
    where = data["where"]
    query = dict()
    for q in where :
        query[q.split('=')[0]] = q.split('=')[1]
    query_result = None
    if collection == "userDB":
        print("Entered userDB")
        query_result = userDB.find(query)
        try:
            print("query result: ",query_result[0])
        except:
            print("No data")
    elif collection == "rideDB":
        print("Entered rideDB")
        query_result = rideDB.find(query)
    else:
        return jsonify({})
    if False:
        for i in userDB.find({}): print(i)
    try:
        num = 0
        res_final = dict()
        for ret in query_result:
            result = dict()
            for key in columns:
                ##################### FIX this by perging the data base ##################
                try:
                    result[key] = ret[key]
                except:
                    pass
                print("ret : ",ret[key])
            res_final[num] = result
            num += 1
    except:
        print("While slicing bad Keys")
        return jsonify({})
    json.dumps(res_final)
    return json.dumps(res_final)


def on_request(ch, method, props, body):
    data = json.loads(body.decode("utf-8"))
    res = ReadFromDB(data)
    print("Read from db : ", res)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=str(res))
    print("Publish Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("ACK done")

def on_sync(ch, method, props, body):
    print("SYNCING DATABASE")
    data = json.loads(body.decode("utf-8"))
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
    print("SYNC WRITE CODE : ",returnCode)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
syncChannel.basic_consume(queue=queue_name, on_message_callback=on_sync)

print("DONE!!!")
print(" [x] Awaiting RPC requests")
channel.start_consuming()
syncChannel.start_consuming()
