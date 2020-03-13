

#Global Variable For Counter

RequestCount = 0


# Return Number of Request made to microservice
@app.route("/api/v1/_count",methods=["GET"])
def ReturnNoOfRequests():
	retval = list(RequestCount)
	return jsonify(rteval),200


# Reset Counter
@app.route("/api/v1/_count",methods=["DELETE"]):
	RequestCount = 0
	return jsonify({}),200



    # Adding Increment Counter
    IncrementCounter()
