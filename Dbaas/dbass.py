# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 16:07:42 2020

@author: 91948
"""


# import libraries
from flask import Flask,jsonify,request
import pymongo
import json

# set up the DB
# runs on port 27017
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]

# set up collections (tables)
#set up the nmes of the collections
userDB = mydb["userDB"] # can be removed 
rideDB = mydb["rideDB"] # can be removed


app = Flask(__name__)

#global declarations
port = 8000
ip = '127.0.0.1'


# Read API
'''
input {
       "table" : "table name",
       "columns" : ["col1","col2"],
       "where" : ["col=val","col=val"]
}
'''
@app.route("/api/v1/db/read",methods=["POST"])
def ReadFromDB():
    # get the input query
    data = request.get_json()
    # decode the query
    collection = data["table"]
    columns = data["columns"]
    where = data["where"]
    query = dict()
    for q in where:
        query[q.split('=')[0]] = q.split('=')[1]
    query_result = None
    # select the correct collection and apply the query
    try:
        query_result = mydb[collection].find(query)
    except:
        print("Table Not Pressent");
        return jsonify({}),400
    ## print the contents of the data
    if True:
        for i in mydb[collection].find({}): print(i)
    try:
        # data present
        query_result[0]
    except IndexError:
        # no data present
        return jsonify({}),204
    # format the output (slice the data)
    try:
        num = 0
        res_list = list()
        for ret in query_result:
            result = dict()
            for key in columns:
                try:
                    result[key] = ret[key]
                except:
                    pass
            res_list.append(result)
            num += 1
    except KeyError:
        print("One of the coulumns given was not present in the data base")
        return jsonify({}),400
    # return the result
    return json.dumps(res_list),200




# write api
'''
input {
       "method" : "write"
       "table" : "table_name",
       "data" : {"col1":"val1","col2":"val2"}
}
{
       "method" : "delete"
       "table" : "table_name",
       "data" : {"col1":"val1","col2":"val2"}
}
{
       "method" : "update"
       "table" : "table_name",
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
    data = request.get_json()
    if (data["method"] == "write"):
        # insert method
        collection = data["table"]
        insert_data = data["data"]
        try:
            mydb[collection].insert_one(insert_data)
        except:
             print("Table Not Pressent");
             return jsonify({}),400
        return jsonify(),200
    elif (data["method"] == "delete"):
        # delete method
        collection = data["table"]
        delete_data = data["data"]
        try:
            mydb[collection].delete_many(delete_data)
        except:
            print("Table not Present")
            return jsonify({}),400
        return jsonify(),200
    elif (data["method"] == "update"):
        # update method
        collection = data["table"]
        try:
            mydb[collection].update_one(data["query"],data["insert"])
        except:
            print("Table not present")
            return jsonify({}),400
        return jsonify(),200
    else:
        # bad method
        return jsonify({}),400




# start the application
if __name__ == '__main__':
    app.debug=True
    app.run(host = ip, port = port)

