###################
# Course: CSE 138
# Date: Fall 2020
# Assignment: #3
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

# Grab and store keys containing the intial view

os.environ["PYTHONHASHSEED"] = "0"

try:
    VIEW_STRING = os.environ["VIEW"]
    VIEW = VIEW_STRING.split(",")  # Creates the list of nodes in the current View
except KeyError:
    VIEW = []
try:
    ADDRESS = os.environ["ADDRESS"]
except KeyError:
    raise Exception("No address provided")
if ADDRESS not in VIEW:
    raise Exception("Address not included in view")


HEADER = {"Content-Type": "application/json"}
kvs = dict()

# ------------------------------ #
# For all key-value operations:

#     The node that directly receives the request from the client should verify the key.

#     The node that directly receives the request from the client should verify the value (Insert and update).

#     If a node receiving a request acts as a proxy, its response should include the address of the correct storage node.

#     If a node receiving a request does not act as a proxy (it is the correct storage node for the requested key), its response should not include the address of the storage node (should not include its own address).

# For the below operation descriptions, it is assumed that node1 (10.10.0.4:13800) does not store the key, sampleKey, and that node2 (10.10.0.5:13800) does store the key.
# ------------------------------ #


# insert a key named sampleKey, send a PUT request to /kvs/keys/sampleKey.

#    If no value is provided for the new key, the key-value store should respond with status code 400 and JSON: {"error":"Value is missing","message":"Error in PUT"}.

#    If the value provided for the new key has length greater than 50, the key-value store should respond with status code 400 and JSON: {"error":"Key is too long","message":"Error in PUT"}.

#    On success, the key-value store should respond with status code 201 and JSON: {"message":"Added successfully","replaced":false,"address":"10.10.0.5:13800"}. This example assumes the receiving node (node1) does not have address "10.10.0.5:13800" and it acted as a proxy to the node (node2) with that address. (Remember the follower from assignment 2?)

# To update an existing key named sampleKey, send a PUT request to /kvs/keys/sampleKey.

#     If no updated value is provided for the key, the key-value store should respond with status code 400 and JSON: {"error":"Value is missing","message":"Error in PUT"}

#     The key-value store should respond with status code 200 and JSON: {"message":"Updated successfully","replaced":true}. This example assumes the receiving node (node2) stores the key, sampleKey.

# Returns
# {
#     "error":
#     "message":
#     "replaced":
# }

# To PUT a key, we simply hash the key, and write the key to the correct IP address
# If the IP address is the current one, we store it
# Otherwise, we forward the message to the correct IP address
@app.route("/kvs/keys/<string:key>", methods=["PUT"])
def put_key(key):
    # Try reading the data. If theres an error, return error message
    data = json.loads(request.get_data())
    try:
        val = data["value"]
    except ValueError:
        return {"error": "Value is missing", "message": "Error in PUT"}, 400
    if len(val) > 50:
        return {"error": "Key is too long", "message": "Error in PUT"}, 400
    IP_to_PUT = get_shard_for_key(key, VIEW)
    if IP_to_PUT == ADDRESS or "internal" in data:
        status_code = 200
        replaced = True
        if key not in kvs:
            status_code = 201
            replaced = False
        kvs[key] = val
        if status_code == 201:
            return {
                "message": "Added successfully",
                "replaced": replaced,
                "address": ADDRESS,
            }, status_code
        return {
            "message": "Updated successfully",
            "replaced": replaced,
            "address": ADDRESS,
        }, status_code
    else:
        resp = requests.put(f"http://{IP_to_PUT}/kvs/keys/{key}", data=request.data)
        return resp.content, resp.status_code


# To get an existing key named sampleKey, send a GET request to /kvs/keys/sampleKey.

#     If the key, sampleKey, does not exist, the key-value store should respond with status code 404 and the JSON: {"doesExist":false,"error":"Key does not exist","message":"Error in GET"}

#     On success, assuming the current value of sampleKey is sampleValue, the key-value store should respond with status code 200 and JSON: {"doesExist":true,"message":"Retrieved successfully","value":"sampleValue"}

# Returns
# {
#     "doesExist":
#     "error":
#     "message":
# }


@app.route("/kvs/keys/<string:key>", methods=["GET"])
def get_key(key):
    IP_to_GET = get_shard_for_key(key, VIEW)
    if IP_to_GET == ADDRESS:
        if key not in kvs:
            return {
                "doesExist": False,
                "error": "Key does not exist",
                "message": "Error in GET",
            }, 404
        else:
            return {
                "doesExist": True,
                "message": "Retrieved successfully",
                "value": "%s" % (kvs[key]),
            }, 200
    else:
        try:
            r = requests.get(f"http://{IP_to_GET}/kvs/keys/{key}")
        except:
            return {
                "error": "Unable to connect to shard",
                "message": "Error in GET",
            }, 503
        return r.content, r.status_code


# To delete an existing key named sampleKey, send a DELETE request to /kvs/keys/sampleKey.

#     If the key, sampleKey, does not exist, the key-value store should respond with status code 404 and JSON: {"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}

#     On success, the key-value store should respond with status code 200 and JSON: {"doesExist":true,"message":"Deleted successfully","address":"10.10.0.5:13800"}. This example assumes the receiving node (node1) does not have address "10.10.0.5:13800" and it acted as a proxy to the node (node2) with that address.

# Returns
# {
#     "doesExist":
#     "error":
#     "message":
#     "address":
# }
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


# To get the number of keys stored by a node, send a GET request to the endpoint, /kvs/key-count at any node.

# On success, the response should have status code 200 and JSON: {"message":"Key count retrieved successfully","key-count":<key-count>}.

# Returns
# {
#     "message":
#     "key-count":
# }
@app.route("/kvs/key-count", methods=["GET"])
def get_key_count():
    key_count = len(kvs)
    return {
        "message": "Key count retrieved successfully",
        "key-count": str(key_count),
    }, 200


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


# To change the view, or add a new node to the key-value store, send a PUT request to the endpoint, /kvs/view-change, with a JSON payload containing the list of addresses in the new view. For example, the JSON payload to add node3, with IP address 10.10.0.6:13800, to a view containing node1 and node2 would be: {"view":"10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800"}.

# View change request can be sent to any of the existing nodes.

# A view change requires two operations:

#     Propagating the view update to every node

#     Reshard of the keys (a re-partition of keys across nodes)

# On success, the response should have status code 200 and JSON: JSON { "message": "View change successful", "shards" : [ { "address": "10.10.0.4:13800", "key-count": 5 }, { "address": "10.10.0.5:13800", "key-count": 3 }, { "address": "10.10.0.6:13800", "key-count": 6 } ] } where each element in the "shards" list is a dictionary with two key-value pairs: the "address" key maps to the IP address of a node storing a shard, and the "key-count" key maps to the number of keys that are assigned to that node. For the above example, the node at address "10.10.0.6:13800" has 6 key-value pairs, meaning that after the view-change, 6 of the 14 keys in the key-value store were re-partitioned into the shard stored on node3.

# Your team have enough time (6 sec) to finish this request.

# Returns
# {
#     "message":
#     "shards": [{"address": , "key-count": }]
# }
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


# Adds a flag to a JSON object to signal that a request originates from internal node
def add_data_for_internel_request(data):
    r = dict.copy(data)
    r["internal"] = True
    return r


# Example of preparing data for internal communication
# def sendInternalData(data):
# internalPayload = addDataForInternalReq(retquest.data)
# r = requests.put('http://' + FORWARDING_ADDRESS + '/kvs/' + key,
#         headers = HEADER, data = internalPayload, timeout = 6)
#         ...


# Returns the IP address of the shard that should hold a given key
# Takes in the key to be hashed and the view
# Assumes VIEW contains ADDRESS (address of self)
# eg get_shard_for_key("foo", VIEW) -> 10.10.0.3:13800
def get_shard_for_key(key, view):
    view_copy = copy.deepcopy(view)
    view_copy.sort()
    # hash key
    hashed_key = hash(key)
    # take mod length(view)
    view_index = hashed_key % len(view)
    return view_copy[view_index]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=13800, debug=True)
