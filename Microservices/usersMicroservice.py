# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 09:28:21 2020

@author: 91948
"""

from flask import Flask, render_template,jsonify,request,abort
from datetime import datetime
import sys
import pymongo
import random
import json
import requests
import csv

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
#rideDB = mydb["rides"]
#import mysql.connector

app = Flask(__name__)
hex_digits = set("0123456789abcdef")

places = []

file = csv.reader(open('AreaNameEnum.csv'), delimiter=',')
for line in file:
    if(line[0]!="Area No."):
        places.append(line[0])


#ip = "172.31.82.178"
ipUser = "127.0.0.1" #  The user ip
ipRide = "127.0.0.1" # The ip the aws system (the thing u put in postman)
portUser = "8080" # Dont change
portRide = "8000" # Dont Change
addrrUser = ipUser+':'+portUser
addrrRide = ipRide+':'+portRide
countFile = "countUser.json"




# for keeping track of number of calls
def incrementCount ():
    with open(countFile,'r+') as jsonFile:
        data = json.load(jsonFile)
        data["count"] = str(int(data["count"])+1)
        jsonFile.seek(0)
        json.dump(data,jsonFile)
        jsonFile.truncate()


# resets the count
def resetCount ():
    with open(countFile,'r+') as jsonFile:
        data = json.load(jsonFile)
        data["count"] = str(0)
        jsonFile.seek(0)
        json.dump(data,jsonFile)
        jsonFile.truncate()


# api count number of requests
'''
    gets the count
'''
@app.route("/api/v1/_count",methods = ["GET"])
def countRequest():
    with open(countFile,'r+') as jsonFile:
        data = json.load(jsonFile)
        count = int(data["count"])
    return json.dumps([count]),200


# api to reset the count
@app.route("/api/v1/_count",methods = ["DELETE"])
def countRequestReset():
    resetCount()
    return jsonify(),200


# api 1
'''
{
    "username" : "a",
    "password" : "1234567890abcdef"
}
'''
@app.route("/api/v1/users",methods=["PUT"])
def AddUser():
    if request.remote_addr != addrrUser or request.remote_addr != addrrRide:
        incrementCount()
    data = request.get_json()
    username = data["username"]
    password = str(data["password"])
    password_check = password.lower()
    ## Here check constrains on password ##
    if len(password_check) < 40:
        return jsonify({"Error":"Bad Request. Password Constraint Failed"}),400
    for c in password_check :
        if c not in hex_digits :
            return jsonify({"Error":"Bad Request. Password Constraint Failed"}),400
    ## Make API JSON  Check if user exisits ##
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ## Send the request ##
    
    ## OLD
    ret = requests.post("http://"+addrrUser+"/api/v1/db/read",json = data)
    ## NEW
    
    ret_new = requests.get("http://"+addrrUser+"/api/v1/user");
    
    test_presence = list(map(lambda x: x.strip('"'),ret_new.text.strip('][').split(', ')))
    if username in test_presence:
        return jsonify({"Error" : "Bad Request. Data alredy present P2"}),400
    
    
    ## Send request for Write the user ##
    if ret.status_code == 204:
        data_part2 = {
                "method" : "write",
                "table" : "userDB",
                "data" : {"username":username,"password":password}
                }
        ret_part2 = requests.post("http://"+addrrUser+"/api/v1/db/write",json = data_part2)
        if ret_part2.status_code == 200:    return jsonify({}),201
        else : return jsonify({"Error" : "Bad Request. Writing problem"}),400
    elif ret.status_code == 400:
        return jsonify({"Error":"Bad request"}),400
    elif ret.status_code == 200:
        return jsonify({"Error" : "Bad Request. Data alredy present"}),400




# api 2 delete
@app.route("/api/v1/users/<username>",methods = ["DELETE"])
def DeleteUser(username):
    ## Check if user exists ##
    if request.remote_addr != addrrUser or request.remote_addr != addrrRide:
        incrementCount()
    data = {
            "table" : "userDB",
            "columns" : ["username"],
            "where" : ["username="+str(username)]
            }
    ret = requests.post("http://"+addrrUser+"/api/v1/db/read",json = data)

    #ret_new = requests.get("http://"+addrrRide+"/api/v1/rides");
    
    #print("Gay",username,username in ret_sex.text.strip('][').split(', '))
    
    try:
        ret_new = requests.get("http://"+addrrUser+"/api/v1/users");
        #print("http://"+addrrUser+"/api/v1/user")
        test_presence = list(map(lambda x: x.strip('"'),ret_new.text.strip('][').split(', ')))
        print(username in test_presence,username,test_presence)
        if username not in test_presence:
            return jsonify({"Error" : "Bad Request. Data Not present P2"}),400
    except:
        pass
    if ret.status_code == 204:
        return jsonify({"Error":"Bad request. No data present."}),400
    elif ret.status_code == 400:
        return jsonify({"Error":"Bad request"}),400
    elif ret.status_code == 200:

        ## Delete user from the User data base##
        #######################################################################
        del_query = {
                    "username" : str(username)
                }
        data_part2 = {
                "method" : "delete",
                "table" : "userDB",
                "data" : {"username" : str(username)}
                }
        ret2 = requests.post("http://"+addrrUser+"/api/v1/db/write",json = data_part2)
        
        ## Delete user from Rides data Base ##
        if ret2.status_code == 200:
            data_pat3 = {
                    "method" : "delete",
                    "table" : "rideDB",
                    "data" : {"created_by" : str(username)}
                    }
            ret3 = requests.post("http://"+addrrRide+"/api/v1/db/write",json = data_pat3)
            if ret3.status_code == 200:
                ###################### del for users ###############################
                ret69 = requests.get("http://"+addrrRide+"/api/v1/rides/all")
                #print ("Data",json.loads(ret69.text))                    
                try:
                    for key in json.loads(ret69.text).keys():
                        data = json.loads(ret69.text)[key]
                        rideId = data["rideId"]
                        if str(username) in data["users"]:
                            ## delete the user from the list of users in rides data base ##
                            list_of_users = data["users"]
                            list_of_users.remove(str(username))
                            query = {
                            "rideId" : str(rideId)
                            }
                            up_query = {
                                    "$set" : {
                                                "users" : list_of_users
                                            }
                                    }
                            data_part3 = {
                                    "method" : "update",
                                    "table" : "rideDB",
                                    "query" : query,
                                    "insert" : up_query
                                    }
                            ret4 = requests.post("http://"+addrrRide+"/api/v1/db/write",json = data_part3)
                            if ret4.status_code == 200:
                                return jsonify(),200
                            else:
                                return jsonify({"error":"usernot foumd"}),400
                except ValueError:
                    pass
                return jsonify({}),200
            else:
                return jsonify({"error":"del with created by failed"}),400
        else:
            return jsonify({"Error":"Bad Request"}),400



# 2.1 List all Users
@app.route("/api/v1/users",methods = ["GET"])
def listAllUsers():
    if request.remote_addr != addrrUser or request.remote_addr != addrrRide:
        incrementCount()
    data = {
       "table" : "userDB",
       "columns" : ["username"],
       "where" : []
           }
    ret = requests.post("http://"+addrrUser+"/api/v1/db/read",json = data)
    if ret.status_code == 204:
        return jsonify({"Error":"Bad request. No data present."}),204
    elif ret.status_code == 400:
        return jsonify({"Error":"Bad request"}),405
    elif ret.status_code == 200:
        result = list()
        for out in json.loads(ret.text).values():
            try:
                result.append(out["username"])
            except KeyError:
                pass
        #print("Start",result)
        #print(type(result))
        return json.dumps(result),200
    else:
        return jsonify(),400;
    
    
# 2.2 Clear DB  USER
@app.route("/api/v1/db/clear",methods = ["POST"])
def clearUserDB ():
    if request.remote_addr != addrrUser or request.remote_addr != addrrRide:
        incrementCount()
    data = {
            "method" : "delete",
           "table" : "userDB",
           "data" : {}
            }
    
    ret = requests.post("http://"+addrrUser+"/api/v1/db/write",json = data)

    ret2 = requests.post("http://"+addrrRide+"/api/v1/db/clear")

    if ret.status_code == 204 or ret2.status_code == 400:
        return jsonify({"Error":"Bad request. No data present."}),400
    elif ret.status_code == 400 or ret2.status_code == 400:
        return jsonify({"Error":"Bad request"}),400
    elif ret.status_code == 200 or ret2.status_code == 200:
        return jsonify(),200
     
      

# api9
'''
input 
{
       "table" : "table name",
       "columns" : ["col1","col2"],
       "where" : ["col=val","col=val"]
}
'''
@app.route("/api/v1/db/read",methods=["POST"])
def ReadFromDB():
    if request.remote_addr != addrrUser or request.remote_addr != addrrRide:
        incrementCount()
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
        return jsonify({}),400
    ### check if NULL is returnned
    #print(query_result[0])
    
    ## Testting the output ##
    if False:
        for i in userDB.find({}): print(i)
    try:
        print("Check",query_result[0])
    except IndexError:
        print("No data")
        return jsonify({}),204
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
            res_final[num] = result
            num += 1
    except KeyError:
        print("While slicing bad Keys")
        return jsonify({}),400
    json.dumps(res_final)
    #print(json.dumps(res_final))
    return json.dumps(res_final),200





# api 8
'''
input {
       "method" : "write"
       "table" : "table :name",
       "data" : {"col1":"val1","col2":"val2"}
}
{
       "method" : "delete"
       "table" : "table :name",
       "data" : {"col1":"val1","col2":"val2"}
}
{
       "method" : "update"
       "table" : "table :name",
       "query" : {"col1":"val1","col2":"val2"},
       "insert" : {"$set" :
                   {
                           "b" : "c"
                   }
           }
}
'''
@app.route("/api/v1/db/write",methods=["POST"])
def WriteToDB():
    if request.remote_addr != addrrUser or request.remote_addr != addrrRide:
        incrementCount()
    data = request.get_json()
    if (data["method"] == "write"):
        collection = data["table"]
        insert_data = data["data"]
        if collection == "userDB":
            userDB.insert_one(insert_data)
        elif collection == "rideDB":
            rideDB.insert_one(insert_data)
        else:
            return jsonify({}),400
        print(collection,insert_data)
        return jsonify(),200
    elif (data["method"] == "delete"):
        collection = data["table"]
        delete_data = data["data"]
        if collection == "userDB":
            userDB.delete_many(delete_data)
        elif collection == "rideDB":
            rideDB.delete_many(delete_data)
        else:
            return jsonify({}),400
        return jsonify(),200
    elif (data["method"] == "update"):
        collection = data["table"]
        if collection == "userDB":
            userDB.update_one(data["query"],data["insert"])
        elif collection == "rideDB":
            rideDB.update_one(data["query"],data["insert"])
        else:
            return jsonify({}),400
        return jsonify(),200
    else:
        return jsonify(),400
  

def getDate ():
    yyyy,mm,dd = str(str(datetime.now()).split(' ')[0]).split('-')
    h,m,s = str(datetime.now()).split(' ')[1].split(':')
    s = s.split(".")[0]
    return dd + "-" + mm + "-" + yyyy + ":" + s + "-" + m + "-" + h


if __name__ == '__main__':
    app.debug=True
    app.run(host = ipUser, port = portUser)