from flask import Flask, render_template,jsonify,request,abort
import string
import pymongo

from bson import json_util, ObjectId
import json

#mongodb client
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

#create/access a database called RideShare: db is created if it does not exist beforehand
mydb = myclient["RideShare"]

#create/access a table (called a collection in mongodb) called UserData: db is created if it does not exist beforehand
usercol = mydb["UserData"]
ridescol = mydb["Rides"]

#init flask
app = Flask(__name__)

hex_digits = set("0123456789abcdef")


#API: 1 , api for adding a username and password into the database
@app.route("/api/v1/users",methods=["PUT"])
def AddUser():
    data = request.get_json()
    username = data["username"]
    password = str(data["password"]).lower()

    if len(password)!=40:
        print("Password Not 40 characters")
        abort(400)
    for c in password :
        if c not in hex_digits :
            print("Password not in hexadecimal")
            abort(400)
    try:
        usercol.insert_one(data)
    except :
        print("Duplicate Error")
        abort(400)
    return jsonify({}),201

#API: 2, api for deleting a given username and password tuple in the database
@app.route("/api/v1/users/<user>",methods=["DELETE"])
def DeleteUser(user) :
    myquery = { "username": user }
    mydoc = usercol.find(myquery)
    #print(mydoc.count())
    if mydoc.count()==0 :
        abort(400)
    try:
        usercol.delete_one(myquery)
    except:
        print("Delete Failed")
        abort(400)

    return jsonify({}),200

#api for returning all available rides API 4
# still have to check if username exists
@app.route("/api/v1/rides",methods=["GET"])
def ListRides():
    source = request.args.get('source')
    destination = request.args.get('destination')
    if source is None or destination is None:
        print("Error Info not given")
        abort(400)
    AllRides = []
    for ride in ridescol.find() :
        temp = dict()
        if ride["source"] == source and ride["destination"] == destination  :
            temp['rideId'] = ride['rideId']
            temp['username'] = ride['created_by']
            temp['timestamp'] = ride['timestamp']
            AllRides.append(temp)
    if len(AllRides) == 0 :
        return {},204
    return jsonify(AllRides),200

@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def ListDetails(rideId):
    print(int(rideId))
    rideId = int(rideId)
    myquery = { "rideId":rideId }
    mydoc = ridescol.find(myquery)
    if mydoc.count() == 0:
        abort(400)
    return json_util.dumps(mydoc),200


if __name__ == '__main__':
	app.debug=True
	app.run()
