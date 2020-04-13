from flask import Flask, render_template,jsonify,request,abort
import pika
import uuid
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
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
	data = request.get_json()
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
	data = request.get_json()
	global response,corr_id
	response = None
	corr_id = str(uuid.uuid4())
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
	return x,200


if __name__ == '__main__':
    app.debug=True
    app.run(host = "0.0.0.0", port = "2000")
