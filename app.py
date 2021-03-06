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
import sys

from time import sleep
import threading

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

VIEW_CHANGE_IN_PROGRESS = False  # Set to true while a view change is executing. While this is true, read/writes may not be correct


def gossiper():
    global kvs
    global VECTOR_CLOCK
    while True:
        if not VIEW_CHANGE_IN_PROGRESS:
            list_of_addresses_in_shard = VIEW[get_my_shard_id()]
            for address in list_of_addresses_in_shard:
                if address == ADDRESS:
                    continue
                try:
                    resp = requests.put(
                        f"http://{address}/kvs/gossip",
                        data=json.dumps({"kvs": kvs, "causal-context": VECTOR_CLOCK}),
                        timeout=0.5
                    )
                    if resp.status_code == 200:
                        kvs = resp.json()["kvs"]
                        VECTOR_CLOCK = resp.json()["causal-context"]
                except:
                    logging.warning("Attempting to connect to: " + address)
        sleep(1)


# ----------------------END SETUP------------------------
#########################################################

# TODO task 1 logically sound but needs testing
#Need to change GET, so that we check the causal history of each key
# def get_key(key):
#     #global output
#     shard_ID = get_shard_for_key(key, VIEW)
#     ip_list = VIEW[shard_ID]
#     #If the current address is in the target shard
#     data = {"causal-context": {}}
#     try:
#         data = json.loads(request.get_data())
#     except: 
#         print("foo")

#     if ADDRESS in ip_list:
#         #This if takes care of case where there is no causal context
#         # if (
#         #     (not request.get_data() or "causal-context" not in request.json) and key not in request.json["causal-context"]
#         # ):
#         if data == {} or "causal-context" not in data or data["causal-context"]  == {} or key not in data["causal-context"]: 
#         #  # check that there is no causal context for the key
#             # there is no causal history
#             if key not in kvs:
#                 return (
#                     json.dumps(
#                         {
#                             "doesExist": False,
#                             "error": "Key does not exist",
#                             "message": "Error in GET",
#                             "causal-context": data["causal-context"]
#                         }
#                     ),
#                     404,
#                 )
#             else:
#                 #We get the value of the key, and its causal context from our kvs
#                 key_val = kvs[key][0]
#                 key_causal_context = {}
#                 if len(kvs[key]) > 1:
#                     key_causal_context = kvs[key][1]
#                 #Now, we return that key with its causal context
#                 return (
#                     json.dumps(
#                         {
#                             "doesExist": True,
#                             "message": "Retrieved successfully",
#                             "value": str(key_val),
#                             "causal-context": merge_vector_clocks(key_causal_context, data["causal-context"]),
#                         }
#                     ),
#                     200,
#                 )

#         else:
#             # if there is causal history
#             # stall if the vector clock for the key is > local vector clock
#             data = json.loads(request.get_data())
#             if "causal-context" in data and key in data["causal-context"]:
#                 #Store the context of the key we are trying to find
#                 key_context = data["causal-context"]
#                 #If the key is not in our VECTOR_CLOCK, or the key_context is not equal to our VECTOR_CLOCK
#                 # TODO use compare_causal_context to compare causal context of the client with the causal context of the key trying to 
#                 if (key not in VECTOR_CLOCK or  compare_causal_context(key_context, kvs[key][1])) and ("forwarded" not in data):
#                     # reach out to all other nodes in the shard, to try to see if any has the correct VECTOR_CLOCK for that key

#                     #check concurrent case

#                     shard_ID = get_my_shard_id()
#                     for node in VIEW[shard_ID]:
#                         if ADDRESS == node:
#                             continue
#                         #Using a try block to handle timeouts. Timeouts take care of infinite looping
#                         try:
#                             forward_data = data
#                             forward_data["forwarded"] = True
#                             r = requests.get(f"http://{node}/kvs/keys/{key}", data= json.dumps(forward_data), timeout=2)
#                         except requests.exceptions.Timeout:
#                             continue
#                         #If the key does exist, we return the value associated with it
#                         if (
#                             r.json()["doesExist"]
#                             and (key in r.json()["causal-context"])
#                             and r.json()["causal-context"][key] > VECTOR_CLOCK[key]
#                         ):
#                             key_value = r.json()["value"]
#                             #Get the causal context of that key, and return it
#                             key_causal_context = r.json()["causal-context"][key]
#                             return (
#                                 json.dumps(
#                                     {
#                                         "doesExist": True,
#                                         "message": "Retrieved successfully",
#                                         "value": key_value,
#                                         "address": node,
#                                         "causal-context": merge_vector_clocks(key_causal_context, data["causal-context"]),
#                                     }
#                                 ),
#                                 200,
#                             )
#                     #If no node had the right value, we return
#                     return (json.dumps({
#                         "error": "Unable to satisfy request",
#                         "message": "Error in GET",
#                     }), 400)
#                 #Else if the key is not in VECTOR_CLOCK or the context is not equal to the clock for that key, and it's a forwarded message, return False
                
#                 elif not compare_causal_context( kvs[key][1], key_context) and not compare_causal_context(  key_context, kvs[key][1]):
#                     return (
#                                 json.dumps(
#                                     {
#                                         "doesExist": True,
#                                         "message": "Retrieved successfully",
#                                         "value": key_value,
#                                         "address": node,
#                                         "causal-context": merge_vector_clocks(key_causal_context, data["causal-context"]),
#                                     }
#                                 ),
#                                 200,
#                             )
#                 elif(key not in VECTOR_CLOCK or not compare_causal_context( kvs[key][1], key_context)) and ("forwarded" in data):
#                     return (
#                     json.dumps(
#                         {
#                             "doesExist": False,
#                             "error": "Key does not exist",
#                             "message": "Error in GET",
#                             "causal-context":  data["causal-context"],
#                         }
#                     ),
#                     404,
#                 )
#             #Else, we found the right key, so we return it along with it's causal context
#             key_val = kvs[key][0]
#             key_causal_context = kvs[key][1]
#             return (
#                 json.dumps(
#                     {
#                         "doesExist": True,
#                         "message": "Retrieved successfully",
#                         "value": key_val,
#                         "causal-context": merge_vector_clocks(key_causal_context, data["causal-context"]),
#                     }
#                 ),
#                 200,
#             )
#     #Else, if our address is not in the ip_list, the key should be in a different shard, so forward the query to all addresses in the shard till we get a response
#     else:
#         # TODO: Verify that tries to forward the request to all nodes that may contain the value (determined by hash of key)
#         # Only returns error in the case that all nodes in the target replica are unreachable

#         # data = json.loads(request.get_data())
#         for ip in ip_list:
#             try:
#                 r = requests.get(f"http://{ip}/kvs/keys/{key}", data=json.dumps(data),timeout=.2)
                
#                 # response_json = r.json()
#                 response_json = json.loads(r.content)
#                 response_json["address"] = ip
#                 return json.dumps(response_json), r.status_code
#             except requests.exceptions.Timeout:
#                 continue
#             except Exception as inst:
#                 return (json.dumps(
#                     {
#                         "error": f"Unable to connect to shard\n{inst}\n response, contents {r.content}",
#                         "message": f"Error in GET for {ip}",
#                         "causal-context":  data["causal-context"],
#                     }
#                 ), 504)

#         return (
#             json.dumps(
#                 {
#                     "error": "Unable to connect to shard",
#                     "message": "Error in GET",
#                     "causal-context":  data["causal-context"],
#                 }
#             ),
#             503,
#         )

@app.route("/kvs/keys/<string:key>", methods=["GET"])
def get_key(key):
    shardReached = False
    shard_ID = get_shard_for_key(key, VIEW)
    ip_list = VIEW[shard_ID]
    #If the current address is in the target shard
    data = {"causal-context": {}}
    try:
        data = json.loads(request.get_data())
    except: 
        print("foo")
    client_cc = data["causal-context"]

    # we are in the shard of the key
    if ADDRESS in ip_list:
        #either we have a higher vector clock than the client for the given key, or the clocks are concurrent
        if key in kvs and (compare_causal_context(kvs[key][1], client_cc) or (not compare_causal_context(kvs[key][1], client_cc) and not compare_causal_context( client_cc, kvs[key][1]))):
            return (
                json.dumps(
                    {
                        "doesExist": True,
                        "message": "Retrieved successfully",
                        "value": str(kvs[key][0]),
                        "causal-context": merge_vector_clocks(kvs[key][1], client_cc)
                    }
                ),
                200)
        
            
    
    #either we don't have the key, or we are in the past or not in the correct shard. We need to reach out to nodes in the shard that holds the key
    f_addr = ""
    for node in ip_list:
        if node != ADDRESS:
            try:
                r = requests.get(f"http://{node}/kvs/keys/{key}", data=json.dumps(data),timeout=2)
                response_json = json.loads(r.content)
                #response_json = r.json()
                response_json["address"] = node
                shardReached = True
                f_addr = node
                if r.status_code == 200:
                    return json.dumps(response_json), r.status_code

            except requests.exceptions.Timeout:
                continue
            except Exception as inst:
                return (json.dumps(
                    {
                        "error": f"Unable to connect to shard\n{inst}\n response, contents {r.content}",
                        "message": f"Error in GET for {node}",
                        "causal-context":  data["causal-context"]
                    }
                ), 504)

    if not shardReached:
        return (json.dumps({
                            "error": "Unable to satisfy request",
                            "message": "Error in GET"
                        }), 400)
    else:
        return (json.dumps(
                        {
                            "doesExist": False,
                            "error": "Key does not exist",
                            "message": "Error in GET",
                            "address": f_addr,
                            "causal-context":  data["causal-context"]
                        }
                    ),
                    404                 )

        


# TODO task 1 logically sound but needs testing
@app.route("/kvs/key-count", methods=["GET"])
def get_key_count():
    key_count = len(kvs)
    return (json.dumps({
        "message": "Key count retrieved successfully",
        "key-count": key_count,
        "shard-id": get_my_shard_id()
    }), 200)


# TODO task 2
# Need to change so that we decide which shard to send the PUT to, instead of which Node
# Need to broadcast PUT to all nodes in the shard (use get_my_shard_id to get current nodes shard id)
# Need to maintain causality. Need to return causal history
# Need to add a variable to let shard know it is a broadcasted from some address, not a request from client.
# Need someway to maintain total order. Eg: Two different nodes in the same shard get different requests,
# we will have to enforce a total order over those requests. Can do Leader Election.
@app.route("/kvs/keys/<string:key>", methods=["PUT"])
def put_key(key):
    #global output
    # Try reading the data. If theres an error, return error message
    data = json.loads(request.get_data())
    try:
        val = data["value"]
    except KeyError:
        return (
            json.dumps(
                {
                    "message": "Error in PUT",
                    "error": "Value is missing",
                    "causal-context": data["causal-context"]
                }
            ),
            400,
        )
    if len(key) > 50:
        return (
            json.dumps(
                {
                    "message": "Error in PUT",
                    "error": "Key is too long",
                    "causal-context": data["causal-context"]
                }
            ),
            400,
        )
    # At this point, we have a valid put
    shard_to_PUT = get_shard_for_key(key, VIEW)
    curr_shard = get_my_shard_id()
    if shard_to_PUT == curr_shard:  
        passed_VC = None
        if "causal-context" in data:
            passed_VC = data["causal-context"]
        # end if
        # First broadcast
        if "broadcast" not in data:
            broadcast_to_shard(key, val, curr_shard, passed_VC)
        #then store locally
        status_code = localStore(key, val, curr_shard, passed_VC)
        replaced = True
        if status_code == 201:
            replaced = False
            return (
                json.dumps(
                    {
                        "message": "Added successfully",
                        "replaced": replaced,
                        "causal-context": kvs[key][1]
                    }
                ),
                status_code,
            )
        else:
            return (
                json.dumps(
                    {
                        "message": "Updated successfully",
                        "replaced": replaced,
                        "causal-context": kvs[key][1]
                    }
                ),
                status_code,
            )
    else:
        # Forward to some address in the different shard
        forward_IPS = VIEW[shard_to_PUT]
        success = False
        resp = None
        forwarded = None
        #Try all address's in the correct shard. If one returns a value, we can break out of the loop. 
        for address in forward_IPS:
            try:
                resp = requests.put(
                    f"http://{address}/kvs/keys/{key}", data=request.data, timeout=2
                )
                success = True
                forwarded = address
                break
            except requests.exceptions.Timeout:
                continue
        #If we were not able to forward the message to any of the addresses in the correct shard, return an error
        if success == False:
            return (
                json.dumps(
                    {
                        "error": "Unable to satisfy request", 
                        "message": "Error in PUT"
                    }
                ),
                400
            )
        #Otherwise, return the data, with the forwarded address
        forwarded_data = json.loads(resp.content)
        return (
            json.dumps(
                {
                    "message": forwarded_data["message"],
                    "replaced": forwarded_data["replaced"],
                    "address": forwarded,
                    "causal-context": forwarded_data["causal-context"]
                }
            ),
            resp.status_code
        )


# TODO task 3
@app.route("/kvs/shards", methods=["GET"])
def get_shards():
    shards = []
    for key in VIEW:
        shards.append(str(key))
    return {"message": "Shard membership retrieved successfully", "shards": shards}, 200


# TODO task 3
@app.route("/kvs/shards/<string:id>", methods=["GET"])
def get_shard_by_id(id):
    if id == get_my_shard_id():
        return json.dumps({
            "message": "Shard information retrieved successfully",
            "shard-id": id,
            "key-count": len(kvs),
            "replicas": VIEW[id]
        }), 200
    else:
        if id in VIEW:
            for node in VIEW[id]:
                try:
                    resp = requests.get(
                        f"http://{node}/kvs/shards/{id}", data=request.data, timeout=2
                    )
                except requests.exceptions.Timeout:
                    continue
                if resp.status_code == 200:
                    return resp.content, resp.status_code
            return (
                json.dumps(
                    {
                        "error": "Unable to reach any node in shard",
                        "message": "Error in GET",
                        "causal-context": VECTOR_CLOCK
                    }
                ),
                500,
            )
        else:
            return (
                json.dumps(
                    {
                        "error": "Shard id does not exist",
                        "message": "Error in GET",
                        "causal-context": VECTOR_CLOCK
                    }
                ),
                404,
            )
    return


# This function will store a key and value locally. Returns status code 200 if key was already in dictionary,
# and 201 if key is new
def localStore(key, value, curr_shard, new_clock):
    toReturn = 201
    #If key not in kvs
    if new_clock is None:
        new_clock = {}
    if key in kvs:
        toReturn = 200
        #Merge all vector clocks
        tempMerged = merge_vector_clocks(new_clock, VECTOR_CLOCK)
        mergedVC = merge_vector_clocks(tempMerged, kvs[key][1])
        mergedVC[key] += 1
        #I just take the causal context
        #Everything we know happened before, locally for our key + causal context
        kvs[key] = [value, mergedVC]
        VECTOR_CLOCK[key] = mergedVC[key]  # Increment vector clock for share
        return toReturn
    
    # If the key was not in our kvs
    VECTOR_CLOCK[key] = 0
    #Increment key clock
    mergedVC = merge_vector_clocks(new_clock, VECTOR_CLOCK)
    mergedVC[key] += 1
    kvs[key] = [value, mergedVC]
    VECTOR_CLOCK[key] = mergedVC[key]
    return toReturn


# This function will broadcast the request to the entire shard.
def broadcast_to_shard(key, value, shard_ID, new_clock):
    for address in VIEW[shard_ID]:
        # Skip the current address,
        if address == ADDRESS:
            continue
        # Otherwise, forward message
        try:
            resp = requests.put(
                f"http://{address}/kvs/keys/{key}",
                data=json.dumps(
                    {"value": str(value), "causal-context": new_clock, "broadcast": True}
                ),
                timeout=2,
            )
        except requests.exceptions.Timeout:
            continue


# TODO task 4
# TODO: Decide if keys of kvs, vector clock, and VIEW are strings or numbers
def rehash_keys(total_KVS, total_VC, num_shards):
    global kvs
    global ADDRESS
    # initialize the kvs_set and vc_set that will hold the kvs for each shard
    kvs_set = {}
    #vc_set = {}
    for i in range(num_shards):
        kvs_set[str(i)] = {}
        #vc_set[str(i)] = {}

    # Add each key to the corresponding kvs it belongs to.
    # Set the vector clock to 0 since causality does not need to be preserved between view changes
    for key in total_KVS:
        shard_num = get_shard_for_key(key, VIEW)
        kvs_set[shard_num][key] = total_KVS[key]
        #vc_set[str(shard_num)] = total_VC  # init vector clock  0's because we don't have to preserve causality between view changes

    for shard_num in VIEW:
        for address in VIEW[shard_num]:
            # if address ==  ADDRESS:
            #     kvs = kvs_set[shard_num]
            resp = requests.put(
                f"http://{address}/kvs/update-view",
                data=json.dumps(
                    {
                        "updated-kvs": kvs_set[shard_num],
                        "updated-vc": total_VC,
                        "updated-view": VIEW,
                        "updated-nodes": nodes
                    }
                ),
            )

    # assemble "shards" object
    shard_list = []
    for shard_num in VIEW:
        shard_obj = {}
        shard_obj["shard-id"] = str(shard_num)
        shard_obj["key-count"] = len(kvs_set[shard_num])
        shard_obj["replicas"] = []
        for address in VIEW[shard_num]:
            shard_obj["replicas"].append(str(address))

        shard_list.append(shard_obj)

    return shard_list


# TODO task 4
#
# Steps:
# 0. Trigger a gossip (ensure that this completes within a reasonable amount of time say ~2sec)
#     a. This should be a best effort approach to normalize the data and resolve conflicts that may exist.
#     b. This step is not mandatory, but can create better results
# Prepare for reshard
# 1. For each node in the current view retrieve it's KVS and store into memory
#   a. As long as we get at least 1 KVS per shard, it should be ok. However more is better
# 2. First check that each node in the new view is up (unsure if we can assume this, if but if this fails within ~3sec, error out)
#   a. Alternatively, we can check if at least 1 node in each shard is up
# 3. Combine all KVS retrieved
#   To get a list of keys, simply merge the dictionares, and discard duplicates
#   a. Create a  total KVS that will hold the most "current" value for each key
#   b. Create a  total "causal context" that will the hold the vector clock of each key
#   c. populate total kvs/causalcontext values
#       for each key in KVS
#           i. get the (vectorClock, value) pair from each kvs of each node
#           ii. Write the value with the maximum vectorClock to the total KVS and causal context
# 4. Reshard Keys
#   a. Create an empty KVS and causalcontext for each shard in the view (repl-factor)
#   b. for each key
#       i. hash key to find which shard it should belong to (get_shard_for_key(key, NEW_VIEW))
#       ii. Add key/value the shard's KVS and set causalcontext of the key for the shard to all 0's
#

# Notes
#  - "For a view change, we are guranteed that all the nodes from the new view are up"
#  - " you do not need to maintain casual history before/after a view change"
#  - There will be no other requests during a view change. Every request has the same time limit for 5 seconds.
#  - A lot of requests are happening, consider using threadPoolExecuter for better performance https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
#  - We may need to solve consensus if we can't gurantee that the node that receives the view change stays up during the entire view change
# To merge 2 dictionaries
# context = dict(dict0)
# context.update(dict1)
# context.update(dict2)
# repeat...


# TODO Deeper testing when GET/PUT are implemented
#Time to rework view change
@app.route("/kvs/view-change", methods=["PUT"])
def perform_view_change():
    # These variables will be changed over the course of performing a view change
    global nodes
    global view_string
    global VIEW
    global REPL
    global VECTOR_CLOCK
    global kvs
    # return {message: "Not Implemented :("}, 404
    # recieve data from request
    json_dict = json.loads(request.get_data())

    global VIEW_CHANGE_IN_PROGRESS
    VIEW_CHANGE_IN_PROGRESS = True

    if "rebalance" in json_dict:
        return json.dumps(
            {
                "message": "View change has begun, returning kvs and context",
                "kvs": kvs,
                "causal-context": VECTOR_CLOCK
            }
        )

    total_KVS = kvs
    total_vector_clock = VECTOR_CLOCK
    # Retrieve the KVS of each node in current view
    for shard_num in VIEW:
        #Iterate through each address in each shard
        for address in VIEW[shard_num]:
            if address != ADDRESS:
                try:
                    resp = requests.put(
                        f"http://{address}/kvs/view-change",
                        data=json.dumps({"rebalance": True}),
                        timeout=0.1,
                    )
                    #sys.stderr.write("resp completed")
                    resp_json_contents = json.loads(resp.content)
                    node_kvs = resp_json_contents["kvs"]
                    node_vector_clock = resp_json_contents["causal-context"]
                    sys.stderr.write(
                        "Received data from {address}, kvs: {node_kvs}, vc {node_vector_clock}"
                    )
                    for key in node_kvs:
                        #For each key, if the key is not already in total_KVS, we add it to total_KVS
                        if (key not in total_KVS):  # this is a new key, add it to the total kvs and its corresponding vector clock to the total vector clock
                            total_KVS[key] = node_kvs[key]
                            total_vector_clock[key] = node_vector_clock[key] #Not sure about this line
                        else:  # Key is already in total kvs, compare vector clocks of keys, and decide which one is more recent
                            #Compare causal context of incoming key, and current key in kvs
                            causal_compare = compare_causal_context(
                                node_kvs[key][1], total_KVS[key][1]
                            )
                            if (causal_compare == True):  #set total_KVS[key] = kvs[key]
                                total_KVS[key] = node_kvs[key]
                                total_vector_clock[key] = node_kvs[key][1][key]
                            # in all cases, merge the two vector clocks
                            # total_vector_clock[key] = merge_vector_clocks(
                            #     total_vector_clock[key], total_vector_clock[key]
                            # )

                except TimeoutError:
                    # Node isn't up, so move on to the next.
                    # Add logic here to ensure that each shard's data is retrieved at least once.

                    sys.stderr.write(
                        f"Timeout error occured for address {address}. Node may not be available"
                    )

                except Exception as inst:
                    sys.stderr.write(
                        f"unknown error for address {address}, exception {inst}"
                    )

    # return {"message": "created total_kvs", "total_KVS": total_KVS, "total_vector_clock": total_vector_clock}

    # Apply the new view to this node

    view_string = json_dict["view"]
    nodes = view_string.split(",")

    REPL = int(json_dict["repl-factor"])
    VIEW = {}
    for i in range(len(nodes) // REPL):
        VIEW[str(i)] = nodes[i * REPL : (i + 1) * REPL]
    
    # assign the collected KVS to shards in new view to all nodes in new view.
    shard_list = rehash_keys(total_KVS, total_vector_clock, len(nodes) // REPL)

    # Return success message
    return json.dumps({"message": "View change successful", "shards": shard_list}), 200


# When a node receives a request to update their view, set global variables pertaining to view, kvs, and vector clock
@app.route("/kvs/update-view", methods=["PUT"])
def update_view():
    json_dict = json.loads(request.get_data())
    global kvs
    global VECTOR_CLOCK
    global VIEW_CHANGE_IN_PROGRESS
    global nodes
    global VIEW
    global ADDRESS
    kvs = json_dict["updated-kvs"]
    VECTOR_CLOCK = json_dict["updated-vc"]
    nodes = json_dict["updated-nodes"]
    VIEW = json_dict["updated-view"]
    if ADDRESS in VIEW:
        VIEW_CHANGE_IN_PROGRESS = False


# optional TODO
@app.route("/kvs/keys/<string:key>", methods=["DELETE"])
def delete_key(key):
    IP_to_DELETE = get_shard_for_key(key, VIEW)
    if IP_to_DELETE == ADDRESS:
        if key not in kvs:
            return {
                "doesExist": False,
                "error": "Key does not exist",
                "message": "Error in DELETE"
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
                "message": "Error in DELETE"
            }, 503
        return r.content, r.status_code
    return {
        "error": "function not implemented: Method %s, URL %s, Key: %s, View %s,"
        % (request.method, request.full_path, key, json.dumps(VIEW))
    }, 500


@app.route("/kvs/gossip", methods=["PUT"])
def gossip():
    global kvs
    global VECTOR_CLOCK
    global VIEW_CHANGE_IN_PROGRESS
    if VIEW_CHANGE_IN_PROGRESS:
        return json.dumps({"kvs": kvs, "causal-context": VECTOR_CLOCK}), 400
    #Load data
    json_dict = json.loads(request.get_data())

    #Store incoming kvs and VC in variables
    their_kvs = json_dict["kvs"]
    their_vc = json_dict["causal-context"]
    #mergeKVS and VC
    mergedKVS, mergedVC = merge_kvs(kvs, their_kvs, VECTOR_CLOCK, their_vc)

    kvs = copy.deepcopy(mergedKVS)
    VECTOR_CLOCK = copy.deepcopy(mergedVC)

    return json.dumps({"kvs": kvs, "causal-context": VECTOR_CLOCK}), 200


# returns the shard ID for the given key
def get_shard_for_key(key, view):
    # hash key
    hashed_key = hash(key)
    # take mod length(view)
    view_index = hashed_key % (len(view))
    return str(view_index)


# returns the shard ID of the current node
def get_my_shard_id():
    for key in VIEW:
        if ADDRESS in VIEW[key]:
            return key


# Compare 2 vector clocks vc1 and vc2
# returns 0 if conccurrent, 1 if vc2 -> (happens before) v1 and -1 if vc1->vc2
def compare_vector_clock(vc1, vc2):
    if vc1 == vc2:
        return 0
    if vc2 < vc1:
        return 1
    if vc1 < vc2:
        return -1

# returns true if cc2 -> cc1
def compare_causal_context(cc1, cc2):
    # check for cc2 -> cc1
    seenStrictOrder = False
    
    for key in cc1:
        if key in cc2:
            # if any key in cc2 is greater than the key in cc1, break
            if cc2[key] > cc1[key]:
                return False
                break
            if cc2[key] < cc1[key]:
                seenStrictOrder = True
                continue
            if cc2[key] == cc1[key]:
                continue
    for key in cc2:
        if key not in cc1 and cc2[key] > 0:
            return False
    
    return True



# returns the pairwise maximum between each element of vector1 and vector2
def merge_vector_clocks(vector1, vector2):
    list_of_keys = list(vector1.keys()) + list(vector2.keys())
    set_of_keys = set(list_of_keys)
    for key in set_of_keys:
        if key in vector1 and key in vector2:
            vector1[key] = max(vector1[key], vector2[key])
        elif key in vector2:
            vector1[key] = vector2[key]
    return vector1


# selects the value with the lowest hash value
def choose_concurrent_value(value1, value2):
    return value1 if hash(value1) < hash(value2) else value2


# chooses next address
def choose_next_node(prev_nodes):
    for address in VIEW[get_my_shard_id()]:
        if address not in prev_nodes:
            return address
    return -1


# merges two KVS's based on Vector Clocks
def merge_kvs(kvs1, kvs2, vc1, vc2):
    mergedKVS = {}
    #For keys in the first kvs
    for key in kvs1:
        #If the key isn't in the second kvs, we store it in our mergedKVS
        if key not in kvs2:
            mergedKVS[key] = kvs1[key]
        #Otherwise, we compare the vector clocks for both KVS, and take the one that is higher
        else:
            vc_compared_result1 = compare_causal_context(kvs1[key][1], kvs2[key][1])
            if(vc_compared_result1 == False):
                vc_compared_result2 = compare_causal_context(kvs2[key][1], kvs1[key][1])
                if vc_compared_result1 == False and vc_compared_result2 == False: # we are looking at concurrent keys
                    keyToChoose = choose_concurrent_value(kvs1[key][0], kvs2[key][0])
                    merged_vc = merge_vector_clocks(kvs1[key][1], kvs2[key][1])
                    mergedKVS[key] = [keyToChoose, merged_vc]
                    continue
                # kvs2 -> kvs1 for a given key
                keyToChoose = kvs1[key][0]
                merged_vc = merge_vector_clocks(kvs1[key][1], kvs2[key][1])
                mergedKVS[key] = [keyToChoose, merged_vc]
                continue


            else:
                keyToChoose = kvs2[key][0]
                merged_vc = merge_vector_clocks(kvs1[key][1], kvs2[key][1])
                mergedKVS[key] = [keyToChoose, merged_vc]
                continue
            
    #For key in kvs2, we add it to our mergedKVS if it isn't in kvs1. Otherwise, we don't worry about it because we already added all common keys
    for key in kvs2:
        if key not in kvs1:
            mergedKVS[key] = kvs2[key]
        else:
            vc_compared_result1 = compare_causal_context(kvs1[key][1], kvs2[key][1])
            if(vc_compared_result1 == False):
                vc_compared_result2 = compare_causal_context(kvs2[key][1], kvs1[key][1])
                if vc_compared_result1 == False and vc_compared_result2 == False: # we are looking at concurrent keys
                    keyToChoose = choose_concurrent_value(kvs1[key][0], kvs2[key][0])
                    merged_vc = merge_vector_clocks(kvs1[key][1], kvs2[key][1])
                    mergedKVS[key] = [keyToChoose, merged_vc]
                    continue
                # kvs2 -> kvs1 for a given key
                keyToChoose = kvs1[key][0]
                merged_vc = merge_vector_clocks(kvs1[key][1], kvs2[key][1])
                mergedKVS[key] = [keyToChoose, merged_vc]
                continue


            else:
                keyToChoose = kvs2[key][0]
                merged_vc = merge_vector_clocks(kvs1[key][1], kvs2[key][1])
                mergedKVS[key] = [keyToChoose, merged_vc]
                continue    
    #Now, we merge the vector clocks, and return our mergedKVS and mergedVC
    mergedVC = merge_vector_clocks(vc1, vc2)
    return mergedKVS, mergedVC


if __name__ == "__main__":
    gossip_thread = threading.Thread(target=gossiper)
    gossip_thread.start()
    app.run(host="0.0.0.0", port=13800, debug=True)
    


