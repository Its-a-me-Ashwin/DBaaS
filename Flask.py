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

rideDB.insert_one({"ride_id":str(random.getrandbits(256)),
                   "source":"C",
                   "destination":"A",
                   "timestamp":"2019",
                   "createdby":"name1",
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


    


# 2 Remove User
@app.route("/api/v1/users/<username>",methods = ["DELETE"])
def DeleteUser(username):
    data = request.get_json()
    print(data)
    return "delete"

# 3 Create New Ride
@app.route("/api/v1/rides/<rideId>",methods = ["GET"])
def getRides (rideId):
    if not str(rideId).isdigit():
        return jsonify(),405
    result = getRidesSQL(rideId)
    if (len(result) > 0):
        return jsonify(result),200
    if (len(result) == 0):
        return jsonify(),204


# 6 Join ride
@app.route("/api/v1/rides/<rideId>", methods = ["POST"])
def join_route (rideId):
    username = request.get_json()["username"]
    #print(requests.get('http://www.google.com'))
    return jsonify(),200


#user = ["A","B"]
'''


myride = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myride["RideShare"]
sched = mydb["SchedRide"]
mydb2 = myride["Users"]
usercol = mydb2["UserData"]
mydb3 = myride["Places"]
placecol = mydb3["PlaceName"]

unames = [
        { "name": "John", "password": "Highway 37" },
        { "name": "Doe", "password": "Highway 38" }
        ]


places = [
        {"name":"Sarjapur"},
        {"name":"Whitefield"}
        ]

usercol.insert_many(unames)
placecol.insert_many(places)

catalog={}
catalog["book1"]=5
catalog["book2"]=4
'''

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
            "columns" : ["ride_id","createdby","timestamp",],
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




@app.route("/api/v1/rides",methods=["POST"])
def schedule_ride():
	#access book name sent as JSON object 
	#in POST request body
    x = request.get_json()
    uname=x["created_by"]
    time=x["timestamp"]
    sou=x["source"]
    dest=x["destination"]
    myquery = { "name": uname }
    myquery1 = { "name":sou}
    myquery2 = {"name":dest}
    #mydoc = usercol.find(myquery)
    if(usercol.find(myquery) is not None and  placecol.find(myquery1)is not None and placecol.find(myquery2) is not None and sou!=dest):
        sched.insert_one(x)
        return jsonify(" yduwq"),200
    else:
        abort(400)
    #return "abc"
  

# api9      
'''
input {
       "table" : "table name",
       "columns" : ["col1",col2],
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
        return jsonify({"error" : "no data found"}),204
    try:
        result = list()
        for ret in query_result:
            for key in columns:
                result.append((key,ret[key]))
    except KeyError:
        return jsonify({"eror":"bad request (Column)"}),400
    return jsonify(result),200





# api 8
'''
input {
       "table" : "table name",
       "data" : ["col1":"val1","col2":"val2"]
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
