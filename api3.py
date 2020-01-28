# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 13:41:09 2020

@author: Atul Anand
"""

#/usr/bin/python

from flask import Flask, render_template,jsonify,request,abort
import pymongo

app=Flask(__name__)

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
#user = ["A","B"]
'''
catalog={}
catalog["book1"]=5
catalog["book2"]=4
'''

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
    
if __name__ == '__main__':	
	app.debug=True
	app.run()
   