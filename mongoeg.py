import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
userDB = mydb["rides"]
DB = mydb["users"]
usercol.drop()
DB.drop()

