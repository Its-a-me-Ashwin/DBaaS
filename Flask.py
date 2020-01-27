# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 10:58:24 2020

@author: 91948
"""

from flask import Flask, render_template,jsonify,request,abort
import string
from datetime import datetime
import pandas as pd
import sys
import mysql.connector


app = Flask(__name__)

mydb = mysql.connector.connect(
  host="localhost",
  user="me",
  passwd="921921"
)

print(mydb)


def getRidesSQL (ridesId):
    return {1:1,2:2}

Users = {}
Users['FirstUser'] = "Password"

hex_digits = set("0123456789abcdef")
#api for adding Users
@app.route("/api/v1/users",methods=["PUT"])
def AddUser():
    data = request.get_json()
    username = data["username"]
    password = str(data["password"]).lower()
    print(data)
    if (username in Users.keys()) or len(password)!=40:
        abort(400)
    for c in password :
        if c not in hex_digits :
            abort(400)
    Users[username] = password
    return Users,201

@app.route("/api/v1/users/<username>",methods = ["DELETE"])
def DeleteUser(username):
    data = request.get_json()
    print(data)
    return "delete"


@app.route("/api/v1/rides/<rideId>",methods = ["GET"])
def getRides (rideId):
    if not str(rideId).isdigit():
        return jsonify(),405
    result = getRidesSQL(rideId)
    if (len(result) > 0):
        return jsonify(result),200
    if (len(result) == 0):
        return jsonify(),204

def getDate ():
    yyyy,mm,dd = str(str(datetime.now()).split(' ')[0]).split('-')
    h,m,s = str(datetime.now()).split(' ')[1].split(':')
    s = s.split(".")[0]
    #“DD-MM-YYYY:SS-MM-HH
    return dd + "-" + mm + "-" + yyyy + ":" + s + "-" + m + "-" + h
    

if __name__ == '__main__':
    yyyy,mm,dd = str(str(datetime.now()).split(' ')[0]).split('-')
    h,m,s = str(datetime.now()).split(' ')[1].split(':')
    s = s.split(".")[0]
    #“DD-MM-YYYY:SS-MM-HH
    date_format = dd + "-" + mm + "-" + yyyy + ":" + s + "-" + m + "-" + h
    #app.debug=True
	#app.run()