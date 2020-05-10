from flask import Flask, render_template,jsonify,request,abort
import pika
import uuid
import json
import time
import docker
import threading
import os
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging



client = docker.from_env()
zk = KazooClient(hosts='zoo:2181')
logging.basicConfig()


apiCounter = 0
Master = []
Slaves = []



connection = pika.BlockingConnection(pika.ConnectionParameters(host="rmq"))
channel = connection.channel()
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue

response = ''
corr_id = ''

app = Flask(__name__)
def on_response(ch, method, props, body):
	global response
	if corr_id == props.correlation_id:
		response = body
		print("RESPONSE!!!")

channel.basic_consume(queue=callback_queue,on_message_callback=on_response,auto_ack=True)


@app.route("/api/v1/db/read",methods=["POST"])
def ReadFromDB():
	global apiCounter
	apiCounter += 1
	print("GOT A READ REQUEST", flush = True)
	data = request.get_json()
	print(data)
	global response,corr_id
	response = None
	corr_id = str(uuid.uuid4())
	channel.basic_publish(
		exchange='',
		routing_key='rpc_queue',
		properties=pika.BasicProperties(
			reply_to=callback_queue,
			correlation_id=corr_id,
		),
		body=json.dumps(data))
	while response is None:
		connection.process_data_events()
	x = response
	return x,200

@app.route("/api/v1/db/write",methods=["POST"])
def writeFromDB():
	print("GOT A WRITE REQUEST", flush = True)
	data = request.get_json()
	global response,corr_id
	response = None
	corr_id = str(uuid.uuid4())
	print(data)
	channel.basic_publish(
		exchange='',
		routing_key='rpc_queue_write',
		properties=pika.BasicProperties(
			reply_to=callback_queue,
			correlation_id=corr_id,
		),
		body=json.dumps(data))
	while response is None:
		connection.process_data_events()
	x = response
	return x

def resetCounter() :
	global apiCounter
	global Slaves
	dnetwork = "ccstage42_default"
	slaveimg = "ccstage42_slave:latest"
	rmqimg = "ccstage42_rmq_1"
	print("counter before : ", apiCounter, flush = True)
	neededSlaveNo = max(1,int(apiCounter/1))
	if neededSlaveNo > len(Slaves) :
		for no in range(neededSlaveNo - len(Slaves)) :
			slave = client.containers.run(slaveimg, ["python3","worker.py"],detach=True,links={rmqimg:"rmq",},network=dnetwork)#, environment = {"TYPE":"SLAVE"})
			Slaves.append(slave.id)
	elif neededSlaveNo < len(Slaves)  :
		for no in range(len(Slaves) - neededSlaveNo) :
			slaveToKill = Slaves[-1]
			#name = slaveToKill.name
			pid = client.containers.get(slaveToKill).attrs["State"]["Pid"]
			client.containers.get(slaveToKill).remove(force=True)
			Slaves.pop()
			#zk.delete("/slave/{}".format(pid))

	print("No of slaves needed : ",neededSlaveNo,flush = True)
	apiCounter = 0
	print("counter after : ", apiCounter, flush = True)

'''
@app.route("/api/v1/crash/master",methods=["POST"])
def crashMaster() :
	#pid = 200
	#data = request.get_json()
	global Master
	masterContainer = client.containers.get(Master[0])
	maspid = masterContainer.attrs["State"]["Pid"]
	#maspid.remove(force=True)
	#Master = []
	zk.delete("/master/{}".format(maspid))
	#electMaster()
	return str(maspid)
'''
@app.route("/api/v1/crash/slave",methods=["POST"])
def crashSlave() :
	global Slaves
	lst = workerListLocal()
	print("list of workers : ",lst,flush=True)
	print(type(max(lst)), flush = True)
	slv = lst.index(max(lst))
	slaveContainer = client.containers.get(Slaves[slv])
	slavepid = slaveContainer.attrs["State"]["Pid"]
	zk.get("/slave/{}".format(slavepid), watch = demo_func )
	zk.delete("/slave/{}".format(slavepid))
	slaveContainer.remove(force=True)
	Slaves.remove(Slaves[slv])
	slavepid = str(slavepid)
	print("type of pid : ",type(slavepid),flush=True)
	return jsonify(str(slavepid))


def workerListLocal() :
	global Master, Slaves, client
	wrlist = []
	for worker in Slaves :
			while client.containers.get(worker).attrs["State"]["Running"]!=True:
				time.sleep(0.5)
			wrlist.append(int(client.containers.get(worker).attrs["State"]["Pid"]))
	return wrlist



def demo_func(event):
	# Create a node with data
	#print(" event : ",event, flush = True)
	#print("WATCH HAS BEEN CALLED ",flush=True)
	#children = zk.get_children("/slave")
	#print("There are %s children with names %s" % (len(children), children))
	global Slaves
	dnetwork = "ccstage42_default"
	masterimg = "ccstage42_master:latest"
	slaveimg = "ccstage42_slave:latest"
	rmqimg = "ccstage42_rmq_1"
	new = client.containers.run(slaveimg, ["python3","worker.py"],detach=True,links={rmqimg:"rmq",},network=dnetwork)
	while client.containers.get(new.id).attrs["State"]["Running"] != True :
			time.sleep(0.01)
	print("Created new slave aftre crash : ",new.name, flush = True)
	Slaves.append(new)

@app.route("/api/v1/worker/list",methods=["GET"])
def workerList() :
	global Master, Slaves, client
	wrlist = []
	for worker in Master :
			while client.containers.get(worker).attrs["State"]["Running"]!=True:
				time.sleep(0.5)
			wrlist.append(client.containers.get(worker).attrs["State"]["Pid"])
	for worker in Slaves :
			while client.containers.get(worker).attrs["State"]["Running"]!=True:
				time.sleep(0.5)
			wrlist.append(client.containers.get(worker).attrs["State"]["Pid"])
	wrlist = sorted(wrlist)
	return wrlist,200


def elect(event) :
	global Master
	masterContainer = client.containers.get(Master[0])
	maspid = masterContainer.attrs["State"]["Pid"]
	masterContainer.remove(force=True)
	election = zk.Election('/slave', 'test-election')
	election.run(findslave)

def findslave() :
	global Slaves, Master
	#children = zk.get_children("/slave")
	#print(children, flush = True)
	children = [int(client.containers.get(i).attrs["State"]["Pid"]) for i in Slaves]
	winner = min(children)
	wincontainer = client.containers.get(Slaves[0])
	for slave in Slaves :
		print(client.containers.get(slave).attrs["State"]["Pid"],type(client.containers.get(slave).attrs["State"]["Pid"]),winner)
		if client.containers.get(slave).attrs["State"]["Pid"] == str(winner) :
			wincontainer = client.containers.get(slave)
			break
	print("exec output", wincontainer.exec_run("python3 setenv.py"), flush = True)
	print("Setting flag : ",wincontainer.attrs["Config"]["Env"], flush = True)
	zk.delete("/slave/{}".format(wincontainer.attrs["State"]["Pid"]))
	Slaves.remove(wincontainer.id)
	zk.create("/master/{}".format(wincontainer.attrs["State"]["Pid"]))
	Master.append(wincontainer.id)


@app.route("/node")
def nodes():
	return [zk.get_children("/master"), zk.get_children("/slave")]

def timer(f_stop):
	resetCounter()
	#f_stop.is_set will be called every 60 seconds
	if not f_stop.is_set():
		threading.Timer(60, timer, [f_stop]).start()

def initMasterSlave() :
	global Slaves, Master, zk
	dnetwork = "ccstage42_default"
	masterimg = "ccstage42_master:latest"
	slaveimg = "ccstage42_slave:latest"
	rmqimg = "ccstage42_rmq_1"
	firstmaster = client.containers.run(masterimg, ["python3","worker.py"],detach=True,links={rmqimg:"rmq",},network=dnetwork, environment = {"TYPE":"MASTER"})
	firstslave = client.containers.run(slaveimg, ["python3","worker.py"],detach=True,links={rmqimg:"rmq",},network=dnetwork)#, environment = {"TYPE":"SLAVE"} )
	Slaves.append(firstslave.id)
	Master.append(firstmaster.id)
	print("Waiting: ",flush=True,end=" ")
	while client.containers.get(firstslave.id).attrs["State"]["Running"]!=True or client.containers.get(firstmaster.id).attrs["State"]["Running"]!=True :
		print(".",flush=True,end=" ")
		time.sleep(0.2)
	print()
	print("create zookeeper nodes ",flush=True)
	# create Zoo zookeeper nodes
	zk.ensure_path("/slave")
	zk.ensure_path("/master")
	zk.create("/slave/{}".format(client.containers.get(firstslave.id).attrs["State"]["Pid"]),b"Slave Node")
	zk.create("/master/{}".format(client.containers.get(firstmaster.id).attrs["State"]["Pid"]),b"Master Node")
	children = zk.get_children("/slave")
	print("There are %s slave children with names %s" % (len(children), children),flush=True)
	children = zk.get_children("/master", watch = elect)
	print("There are %s master children with names %s" % (len(children), children),flush=True)
	#print("Killing the child",flush=True)
	#time.sleep(5)
	#zk.delete("/slave/{}".format(client.containers.get(firstslave.id).attrs["State"]["Pid"]))



if __name__ == '__main__':
	print("zk start ", flush = True)
	zk.start()
	zk.delete("/master", recursive=True)
	zk.delete("/slave", recursive=True)
	initMasterSlave()
	f_stop = threading.Event()
	timer(f_stop)
	app.run(host = "0.0.0.0", port = "2000", use_reloader = False, debug = True)
