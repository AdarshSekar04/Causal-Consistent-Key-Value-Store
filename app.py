###################
# Course: CSE 138
# Date: Fall 2020
# Assignment: #4
# Author: Jonathan Amar, Spencer Gurley, Adarsh Sekar, Nick Shekelle

###################
from flask import Flask, request, jsonify
import requests
import socket
import copy
import logging

import json
import os

app = Flask(__name__)
primary = False

#########################################################
# ----------------------BEGIN SETUP----------------------
os.environ["PYTHONHASHSEED"] = "0"


try:
    view_string = os.environ["VIEW"]
    nodes = view_string.split(",")  # Creates the list of nodes in the current View
except KeyError:
    raise Exception("VIEW required")
try:
    REPL = int(os.environ["REPL_FACTOR"])
except KeyError:
    raise Exception("No REPL FACTOR provided")
try:
    ADDRESS = os.environ["ADDRESS"]
except KeyError:
    raise Exception("No address provided")

# TODO does len(nodes) need to be divisible by REPL?
if len(nodes) % REPL != 0:
    raise Exception("Not sure if this is allowed")

# create the VIEW dict
VIEW = {}
for i in range(len(nodes) // REPL):
    VIEW[str(i)] = nodes[i * REPL : (i + 1) * REPL]

kvs = {}

VECTOR_CLOCK = {}

# ----------------------END SETUP------------------------
#########################################################

# TODO task 1 logically sound but needs testing
@app.route("/kvs/keys/<string:key>", methods=["GET"])
def get_key(key):
    shard_ID = get_shard_for_key(key, VIEW)
    ip_list = VIEW[shard_ID]
    if ADDRESS in ip_list:
        if key not in kvs:
            return {
                "doesExist": False,
                "error": "Key does not exist",
                "message": "Error in GET",
                "causal-context": json.dumps(VECTOR_CLOCK),
            }, 404
        else:
            return {
                "doesExist": True,
                "message": "Retrieved successfully",
                "value": "%s" % (kvs[key]),
                "causal-context": json.dumps(VECTOR_CLOCK),
            }, 200
    else:
        try:
            for ip in ip_list:
                r = requests.get(f"http://{ip}/kvs/keys/{key}")
                return r.content, r.status_code
        except:
            return {
                "error": "Unable to connect to shard",
                "message": "Error in GET",
                "causal-context": json.dumps(VECTOR_CLOCK),
            }, 503


# TODO task 1 logically sound but needs testing
@app.route("/kvs/key-count", methods=["GET"])
def get_key_count():
    key_count = len(kvs)
    return {
        "message": "Key count retrieved successfully",
        "key-count": str(key_count),
        "shard-id": get_my_shard_id(),
    }, 200


# TODO task 2
# Need to change so that we decide which shard to send the PUT to, instead of which Node
# Need to broadcast PUT to all nodes in the shard (use get_my_shard_id to get current nodes shard id)
# Need to maintain causality. Need to return causal history
# Need to add a variable to let shard know it is a broadcasted from some address, not a request from client.
# Need someway to maintain total order. Eg: Two different nodes in the same shard get different requests,
# we will have to enforce a total order over those requests. Can do Leader Election.
@app.route("/kvs/keys/<string:key>", methods=["PUT"])
def put_key(key):
	# Try reading the data. If theres an error, return error message
	data = json.loads(request.get_data())
	try:
	    val = data["value"]
	except KeyError:
	    return {"message": "Error in PUT", "error": "Value is missing", "causal-context": VECTOR_CLOCK}, 400
	if len(key) > 50:
	    return {"message": "Error in PUT", "error": "Key is too long", "address": ADDRESS, "causal-context": VECTOR_CLOCK}, 400
	#At this point, we have a valid put
	shard_to_PUT = get_shard_for_key(key, VIEW)
	curr_shard = get_my_shard_id()
	if shard_to_PUT == curr_shard: #or "internal" in data or broadcast in data:
		passed_VC = None
		if "causal-context" in data:
			passed_VC = data["causal-context"]
		#end if
		status_code = localStore(key, val, curr_shard, passed_VC)
		#Now we've stored it locally, need to check if we need to broadcast
		if "broadcast" not in data:
			broadcast_to_shard(key, value, curr_shard, VECTOR_CLOCK)
		#
		replaced = True
		if status_code == 201:
			replaced = False
			return {"message": "Added successfully", "replaced": replaced, "address": ADDRESS, "causal-context": VECTOR_CLOCK}, status_code
		else:
			return {"message": "Updated successfully", "replaced": replaced, "causal-context": VECTOR_CLOCK}, status_code
	else:
		#Forward to some address in the different shard
		forward_IP = VIEW[shard_to_PUT][0]
		resp = requests.put(f"http://{forward_IP}/kvs/keys/{key}", data=request.data)
		return resp.content, resp.status_code


# TODO task 3
@app.route("/kvs/shards", methods=["GET"])
def get_shards():
    shards = []
    for key in VIEW:
        shards.append(str(key))
    return {
           "message"       : "Shard membership retrieved successfully",
           "shards"        : shards
        }, 200


# TODO task 3
@app.route("/kvs/shards/<string:id>", methods=["GET"])
def get_shard_by_id(id):
    if id == get_my_shard_id(): 
        return {
           "message"       : "Shard information retrieved successfully",
           "shard-id"      : id,
           "key-count"     : len(kvs),
           "replicas"      : VIEW[id]
        }, 200
    else:
        if id in VIEW:
            resp = requests.get(f"http://{VIEW[id][0]}/kvs/shards/{id}", data=request.data)
            return resp.content, resp.status_code
        else:
            return {
                "error": "Shard id does not exist",
                "message": "Error in GET",
                "causal-context": json.dumps(VECTOR_CLOCK),
            }, 404
    return

#This function will store a key and value locally. Returns status code 200 if key was already in dictionary,
#and 201 if key is new
def localStore(key, value, curr_shard, new_clock=None):
	toReturn = 201
	if key in kvs:
		toReturn = 200
		key_clock = kvs[key][1]
		VECTOR_CLOCK[curr_shard] += 1 #Increment vector clock for shard
		#This if checks for concurrent request cases
		if key_clock == new_clock and new_clock is not None:
			smaller = min(value, kvs[key][0]) #Select smaller string always
			kvs[key] = smaller
			return toReturn
	kvs[key] = value
	return toReturn

#This function will broadcast the request to the entire shard. 
def broadcast_to_shard(key, value, shard_ID, new_clock):
	for address in VIEW[shard_ID]:
		#Skip the current address,
		if address == ADDRESS:
			continue
		#Otherwise, forward message
		resp = requests.put(f"http://{address}/kvs/keys/{key}", data=json.dumps({"value": value, "causal-context": VECTOR_CLOCK, "broadcast": True}))

# TODO task 4
def rehash_keys():
    global kvs
    keys_to_delete = []
    for key in kvs:
        new_ip = get_shard_for_key(key, VIEW)
        if new_ip != ADDRESS:
            response = requests.put(
                f"http://{new_ip}/kvs/keys/{key}",
                data=json.dumps({"value": kvs[key], "internal": True}),
            )
            keys_to_delete.append(key)

    for key in keys_to_delete:
        del kvs[key]

    return "Success"


# TODO task 4
@app.route("/kvs/view-change", methods=["PUT"])
def perform_view_change():
    global VIEW
    # recieve data from request
    json_dict = json.loads(request.get_data())

    if "rebalance" in json_dict:
        # we know the request came from a node in the network
        VIEW = json_dict["view"]
        return rehash_keys()

    # broadcast view to all other nodes
    for node in VIEW:
        if node != ADDRESS:
            requests.put(
                f"http://{node}/kvs/view-change",
                data=json.dumps(
                    {"view": json_dict["view"].split(","), "rebalance": True}
                ),
            )

    # update view
    VIEW = json_dict["view"].split(",")

    # rehash our local keys
    rehash_keys()

    # get the key_count from each node
    response_json = {"message": "View change successful", "shards": []}
    for node in VIEW:
        if node != ADDRESS:
            response = requests.get(f"http://{node}/kvs/key-count")
            count = response.json()["key-count"]
        else:
            count = len(kvs)

        response_json["shards"].append({"address": node, "key-count": count})

    return response_json


# optional TODO
@app.route("/kvs/keys/<string:key>", methods=["DELETE"])
def delete_key(key):
    IP_to_DELETE = get_shard_for_key(key, VIEW)
    if IP_to_DELETE == ADDRESS:
        if key not in kvs:
            return {
                "doesExist": False,
                "error": "Key does not exist",
                "message": "Error in DELETE",
            }, 404
        else:
            del kvs[key]
            return {
                "doesExist": True,
                "message": "Deleted successfully",
                # "address": "%s" % (ADDRESS), if node holds the key, 'address' should not be included in return
            }, 200
    else:
        try:
            r = requests.delete(f"http://{IP_to_DELETE}/kvs/keys/{key}")
        except:
            return {
                "error": "Unable to connect to shard",
                "message": "Error in DELETE",
            }, 503
        return r.content, r.status_code
    return {
        "error": "function not implemented: Method %s, URL %s, Key: %s, View %s,"
        % (request.method, request.full_path, key, json.dumps(VIEW))
    }, 500


# returns the shard ID for the given key
def get_shard_for_key(key, view):
    # hash key
    hashed_key = hash(key)
    # take mod length(view)
    view_index = hashed_key % (len(view) - 1)
    return str(view_index)


# returns the shard ID of the current node
def get_my_shard_id():
    for key in VIEW:
        if ADDRESS in VIEW[key]:
            return key


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=13800, debug=True)
