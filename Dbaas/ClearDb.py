import pymongo

myclient = pymongo.MongoClient("mongodb://192.168.0.107:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]
userDB.drop()
rideDB.drop()

myclient = pymongo.MongoClient("mongodb://192.168.0.107:27018/")
mydb = myclient["mydatabase"]
userDB = mydb["users"]
rideDB = mydb["rides"]
userDB.drop()
rideDB.drop()
