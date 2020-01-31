# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 10:58:24 2020

@author: 91948
"""

from flask import Flask, render_template,jsonify,request,abort
from datetime import datetime
import pandas as pd
import sys
import pymongo
import random
import json
import requests

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]
#import mysql.connector

userDB.insert_one({"name" : "abcd"})
userDB.insert_one({"name1" : "abcde"})
userDB.insert_one({"name2" : "abcdef"})
userDB.insert_one({"bois": "pass"})
userDB.insert_one({"username" : "abcde"})

rideDB.insert_one({"ride_id":str(random.getrandbits(256)),
                   "source":"C",
                   "destination":"A",
                   "timestamp":"2019",
                   "created_by":"name1",
                   "users":["A","name"]})


app = Flask(__name__)


userDB.insert_one({"name" : '111', "bois" : '3456789'})

def getRidesSQL (ridesId):
    return {1:1,2:2}

Users = {}
Users['FirstUser'] = "Password"

hex_digits = set("0123456789abcdef")




# api 1
'''
{
    "username" : "a",
    "password" : "1234567890abcdef"
}
'''
@app.route("/api/v1/users",methods=["PUT"])
def AddUser():
    data = request.get_json()
    username = data["username"]
    password = str(data["password"]).lower()
    for c in password :
        if c not in hex_digits :
            abort(400)
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    print(data)
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    print(ret.status_code)
    if ret.status_code == 204:
        data_part2 = {
                "table" : "userDB",
                "data" : {"username":username,"password":password}
                }
        ret_part2 = requests.post("http://127.0.0.1:5000/api/v1/db/write",json = data_part2)
        if ret_part2.status_code == 200:    return jsonify({"correctly":"done"}),200
        else : return jsonify({"error" : "wrting probs"}),404
    elif ret.status_code == 400:
        return jsonify({"eror":"bad request (Table not found)"}),400
    elif ret.status_code == 200:
        return jsonify({"error" : "data alredy present"}),200    
    else:
        return jsonify(),500


    


# api 2 delete 
@app.route("/api/v1/users/<username>",methods = ["DELETE"])
def DeleteUser(username):
    print(username)
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    print(ret.status_code)
    if ret.status_code == 204:
        return jsonify({"error":"bad request(no data present)"}),400
    elif ret.status_code == 400:
        return jsonify({"error":"bad request (you give wrong data)"}),400
    elif ret.status_code == 200:
        print("Data present")
        ## put delete here
        #######################################################################
        del_query = {
                    "username" : str(username)
                }
        userDB.delete_one(del_query)
        return jsonify({"found" : "data"}),200    
    else:
        return jsonify(),500


# 3 Create New Ride
@app.route("/api/v1/rides",methods = ["POST"])
def makeRide():
    data = request.get_json()
    username = data["created_by"]
    timestamp = data["timestamp"]
    source = data["source"]
    destination = data["destination"]
    print("Data got",username,timestamp,source,destination)
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    print(ret.status_code)
    if ret.status_code == 204:
        return jsonify({"error":"bad request(no data present)"}),400
    elif ret.status_code == 400:
        return jsonify({"error":"bad request (you give wrong data)"}),400
    elif ret.status_code == 200:
        print("Data present")
        ## create ride
        data_part2 = {
                "created_by" : username,
                "timestamp" : timestamp,
                "source" : source,
                "destination" : destination,
                "ride_id" : str(random.getrandbits(256)),
                "users":[username]
                }
        write_query = {"table" : "rideDB", "data" : data_part2 }
        print(write_query)
        ret = requests.post("http://127.0.0.1:5000/api/v1/db/write", json = write_query)
        if ret.status_code == 200:
            return jsonify(),200
        else:
            return jsonify(),str(ret.status_code)
        return jsonify({"found" : "data"}),200    
    else:
        return jsonify(),500


#api 4
'''
input = /api/v1/rides?source=C&destination=A 
'''
@app.route("/api/v1/rides",methods=["GET"])
def findRides():
    src = request.args.get("source")
    dist = request.args.get("destination")
    #print(src,dist)
    data = {
            "table" : "rideDB",
            "columns" : ["ride_id","createdby","timestamp"],
            "where" : ["source="+src,"destination="+dist]
            }
    print("DATA...",data)
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 200:
        print(ret.text)
        return jsonify(ret.text),200
    elif ret.status_code == 400:
        return jsonify({"error":"bad request"}),400
    elif ret.status_code == 204:
        return jsonify(),204
    return jsonify(),500


#api 5
'''
/api/v1/rides/123
'''
@app.route("/api/v1/rides/<rideId>",methods = ["GET"])
def findRideDetails (rideId):
    query = {
                    "table" : "rideDB",
                    "columns" : ["ride_id","source","destination","timestamp","createdby","users"],
                    "where" : ["ride_id="+rideId]
                }
    print(query)
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read", json = query)
    if ret.status_code == 200:
        print("Final",jsonify(ret.text))
        return jsonify(ret.text),200
    elif ret.status_code == 400:
        return jsonify({"error":"bad request"}),400
    elif ret.status_code == 204:
        return jsonify(),204
    return jsonify(),500

# api 6
'''
/api/v1/rides/<rideId>
{
    "username" : "bro"
}
'''
@app.route("/api/v1/rides/<rideId>",methods = ["POST"])
def joinRide(rideId):
    data = request.get_json()
    username = data["username"]
    print(rideId,data)
    data = {
            "table" : "rideDB",
            "columns" : ["ride_id","users"],
            "where" : ["ride_id="+str(rideId)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 204:
        return jsonify({"error":"bad request(no data present)"}),400
    elif ret.status_code == 400:
        return jsonify({"error":"bad request (you give wrong data)"}),400
    elif ret.status_code == 200:
        print("Ride Present")
        print(ret.status_code,ret.text)
        data = {
                "table" : "userDB",
                "columns" : ["username"],
                "where" : ["username="+str(username)]
                }
        ret1 = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
        print(ret1.status_code)
        if ret1.status_code == 204:
            return jsonify({"error":"bad request(no data present)"}),400
        elif ret1.status_code == 400:
            return jsonify({"error":"bad request (you give wrong data)"}),400
        elif ret1.status_code == 200:
            #### update here #######
            ret_json = json.loads(ret.text)
            list_of_users = list(ret_json["0"]["users"])
            list_of_users.append(str(username))
            print("Data present",list_of_users)
            query = {
                    "ride_id" : str(rideId)
                    }
            up_query = {
                    "$set" : {
                                "users" : list_of_users
                            }
                    }
            rideDB.update_one(query,up_query)
            return jsonify({"found" : "data"}),200    
        else:
            return jsonify(),500
        return jsonify({"found" : "data"}),200    
    else:
        return jsonify(),500

# api 7
'''
/api/v1/rides/12334546484
'''
@app.route("/api/v1/rides/<rideId>",methods = ["DELETE"])
def DeleteRides(rideId):
    print(rideId)
    data = {
            "table" : "rideDB",
            "columns" : ["ride_id"],
            "where" : ["ride_id="+str(rideId)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    print(ret.status_code)
    if ret.status_code == 204:
        return jsonify({"error":"bad request(no data present)"}),400
    elif ret.status_code == 400:
        return jsonify({"error":"bad request (you give wrong data)"}),400
    elif ret.status_code == 200:
        print("Data present")
        ## put delete here
        #######################################################################
        del_query = {
                    "ride_id" : str(rideId)
                }
        rideDB.delete_one(del_query)
        return jsonify({"found" : "data"}),200    
    else:
        return jsonify(),500




# api9      
'''
input {
       "table" : "table name",
       "columns" : ["col1","col2"],
       "where" : ["col=val","col=val"]
}
'''
@app.route("/api/v1/db/read",methods=["POST"])
def ReadFromDB():
    data = request.get_json()
    print("Data got",data)
    collection = data["table"]
    columns = data["columns"]
    where = data["where"]
    query = dict()
    for q in where:
        query[q.split('=')[0]] = q.split('=')[1]
    query_result = None
    if collection == "userDB":
        query_result = userDB.find(query)
    elif collection == "rideDB":
        query_result = rideDB.find(query)
    else:
        return jsonify({"eror":"bad request (Table not found)"}),400
    ### check if NULL is returnned
    #print(query_result[0])
    try:
        print("Check",query_result[0])
    except IndexError:
        print("No data")
        return jsonify({"error" : "no data found"}),204
    try:
        num = 0
        res_final = dict()
        for ret in query_result:
            result = dict()
            for key in columns:
                ##################### FIX this by perging the data base
                try:
                    result[key] = ret[key]
                except:
                    pass
            res_final[num] = result
            num += 1
    except KeyError:
        print("While slicing")
        return jsonify({"eror":"bad request (Column)"}),400
    json.dumps(res_final)
    print(json.dumps(res_final))
    return json.dumps(res_final),200





# api 8
'''
input {
       "table" : "table :name",
       "data" : {"col1":"val1","col2":"val2"}
}
'''    
@app.route("/api/v1/db/write",methods=["POST"])
def WriteToDB():
    data = request.get_json()
    collection = data["table"]
    insert_data = data["data"]
    if collection == "userDB":
        userDB.insert_one(insert_data)
    elif collection == "rideDB":
        rideDB.insert_one(insert_data)
    else:
        return jsonify({"eror":"bad request (Table not found)"}),400
    print(collection,insert_data)
    return jsonify(),200


def getDate ():
    yyyy,mm,dd = str(str(datetime.now()).split(' ')[0]).split('-')
    h,m,s = str(datetime.now()).split(' ')[1].split(':')
    s = s.split(".")[0]
    return dd + "-" + mm + "-" + yyyy + ":" + s + "-" + m + "-" + h
    

if __name__ == '__main__':
    app.debug=True
    app.run()
    
    
    
'''
    

'''    
