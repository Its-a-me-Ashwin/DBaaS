# -*- coding: utf-8 -*-
"""
Created on Sat Feb  1 09:34:44 2020

@author: Atul Anand
"""

from flask import Flask, render_template,jsonify,request,abort
from datetime import datetime
import pandas as pd
import sys
import pymongo
import random
import json
import requests
import csv

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]
#placeDB = mydb["places"]
#import mysql.connector

app = Flask(__name__)
hex_digits = set("0123456789abcdef")

places = []
file = csv.reader(open('AreaNameEnum.csv'), delimiter=',')
for line in file:
    if(line[1]!="Area Name"):
        places.append(line[1])


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
    password = str(data["password"])
    password_check = password.lower()
    if len(password_check) < 40:
        return jsonify({}),400
    for c in password_check :
        if c not in hex_digits :
            return jsonify({}),400
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 204:
        data_part2 = {
                "table" : "userDB",
                "data" : {"username":username,"password":password}
                }
        ret_part2 = requests.post("http://127.0.0.1:5000/api/v1/db/write",json = data_part2)
        if ret_part2.status_code == 200:    return jsonify({}),201
        else : return jsonify({}),400
    elif ret.status_code == 400:
        return jsonify({}),400
    elif ret.status_code == 200:
        return jsonify({}),400    
    else:
        return jsonify({}),500
    
#api 2
@app.route("/api/v1/users/<username>",methods = ["DELETE"])
def DeleteUser(username):
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 204:
        return jsonify({}),400
    elif ret.status_code == 400:
        #When does this happen?
        return jsonify({}),400
    elif ret.status_code == 200:
        del_query = {
                    "username" : str(username)
                }
        userDB.delete_one(del_query)
        return jsonify({}),200    
    
#api 3
@app.route("/api/v1/rides",methods = ["POST"])
def makeRide():
    data = request.get_json()
    username = data["created_by"]
    timestamp = data["timestamp"]
    source = data["source"]
    destination = data["destination"]
    #######################This is for users##################################
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 204:
        return jsonify({}),400
    elif ret.status_code == 400:
        return jsonify({}),400
    ######################source and destination check########################
    flagS = 0
    flagD = 0
    if source in places:
        flagS = 1
    else:
        return jsonify({}),400
    if destination in places:
        flagD = 1
    else:
        return jsonify({}),400
    ##########################################################################
    if ret.status_code == 200 and flagS == 1 and flagD == 1:
        x =  str(random.getrandbits(256))
        ## create ride
        data_part2 = {
                "created_by" : username,
                "timestamp" : timestamp,
                "source" : source,
                "destination" : destination,
                "ride_id" : x,
                "users":[username]
                }
        write_query = {"table" : "rideDB", "data" : data_part2 }
        ret = requests.post("http://127.0.0.1:5000/api/v1/db/write", json = write_query)
        if ret.status_code == 200:
            return jsonify({}),201
        else:
            return jsonify({}),str(ret.status_code)
        
#api 4
@app.route("/api/v1/rides",methods=["GET"])
def findRides():
    src = request.args.get("source")
    dist = request.args.get("destination")
    flagS = 0
    flagD = 0
    if src in places:
        flagS = 1
    else:
        return jsonify({}),400
    if dist in places:
        flagD = 1
    else:
        return jsonify({}),400
    if(flagS==1 and flagD==1):
        data = {
                "table" : "rideDB",
                "columns" : ["ride_id","createdby","timestamp"],
                "where" : ["source="+src,"destination="+dist]
                }
        ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
        if ret.status_code == 200:
            return json.loads(ret.text),200
        elif ret.status_code == 400:
            return jsonify({}),400
        elif ret.status_code == 204:
            return jsonify({}),204


#api 5
@app.route("/api/v1/rides/<rideId>",methods = ["GET"])
def findRideDetails (rideId):
    query = {
                    "table" : "rideDB",
                    "columns" : ["ride_id","source","destination","timestamp","createdby","users"],
                    "where" : ["ride_id="+rideId]
                }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read", json = query)
    if ret.status_code == 200:
        return json.loads(ret.text),200
    elif ret.status_code == 400 or ret.status_code == 204:
        return jsonify({}),204
    
#api 6
@app.route("/api/v1/rides/<rideId>",methods = ["POST"])
def joinRide(rideId):
    data = request.get_json()
    username = data["username"]
    data = {
            "table" : "rideDB",
            "columns" : ["ride_id","users"],
            "where" : ["ride_id="+str(rideId)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 204:
        return jsonify({}),204
    elif ret.status_code == 400:
        return jsonify({}),204
    elif ret.status_code == 200:
        data = {
                "table" : "userDB",
                "columns" : ["username"],
                "where" : ["username="+str(username)]
                }
        ret1 = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
        if ret1.status_code == 204:
            return jsonify({}),204
        elif ret1.status_code == 400:
            return jsonify({}),204
        elif ret1.status_code == 200:
            #### update here #######
            ret_json = json.loads(ret.text)
            list_of_users = list(ret_json["0"]["users"])
            if(str(username) not in list_of_users):
                list_of_users.append(str(username))
                query = {
                        "ride_id" : str(rideId)
                        }
                up_query = {
                        "$set" : {
                                    "users" : list_of_users
                                }
                        }
                rideDB.update_one(query,up_query)
                return jsonify({}),200
            else:
                return jsonify({}),204
            
#api 7
@app.route("/api/v1/rides/<rideId>",methods = ["DELETE"])
def DeleteRides(rideId):
    data = {
            "table" : "rideDB",
            "columns" : ["ride_id"],
            "where" : ["ride_id="+str(rideId)]
            }
    ret = requests.post("http://127.0.0.1:5000/api/v1/db/read",json = data)
    if ret.status_code == 200:
        del_query = {
                    "ride_id" : str(rideId)
                }
        rideDB.delete_one(del_query)
        return jsonify({}),200    

        
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
        return jsonify({}),400
    ### check if NULL is returnned
    try:
        print("Check",query_result[0])
    except IndexError:
        return jsonify({}),204
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
        return jsonify({}),400
    json.dumps(res_final)
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
        return jsonify({}),400
    return jsonify({}),200


def getDate ():
    yyyy,mm,dd = str(str(datetime.now()).split(' ')[0]).split('-')
    h,m,s = str(datetime.now()).split(' ')[1].split(':')
    s = s.split(".")[0]
    return dd + "-" + mm + "-" + yyyy + ":" + s + "-" + m + "-" + h
    

if __name__ == '__main__':
    app.debug=True
    app.run()