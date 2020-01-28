import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["RideShare"]
ridescol = mydb["Rides"]
ridescol.create_index("created_by")
ridescol.create_index("timestamp")
ridescol.create_index("source")
ridescol.create_index("destination")
ridescol.create_index("rideId",unique=True)
testdata = {"created_by":"Mr Robot","timestamp":"27-01-2020:30-32-21","source":"Yeshwanthpura","destination":"Mattikere","rideId":1234}
try:
    ridescol.insert_one(testdata)
except:
    print("Error Did Not Insert")
for x in ridescol.find():
  print(x)
