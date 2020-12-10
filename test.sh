putTest(){
	echo "============================================"
	echo "Testing PUT\n\n"
	#First testing errors
	echo "Testing missing key"
	curl --request   PUT --header    "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"causal-context":{}}' http://127.0.0.1:13802/kvs/keys/sampleKey
    echo "Testing with key that is too long"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"sampleValue","causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/loooooooooooooooooooooooooooooooooooooooooooooooong

    #Now testing Valid PUTS, with no causal context
    echo "Testing Valid PUT with new key, without causal-context"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"sampleValue","causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/sampleKey
    echo "Testing Valid PUT with new key 2, without causal-context"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"sampleValue_2","causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/sampleKey_2
    echo "Testing Valid PUT updating key, without causal-context"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"new sampleValue","causal-context":{}}' \
       http://127.0.0.1:13802/kvs/keys/sampleKey
    echo "Testing Valid PUT updating key 2, without causal-context"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"new sampleValue_2","causal-context":{}}' \
       http://127.0.0.1:13802/kvs/keys/sampleKey_2
}


key_count_test(){
	echo "Testing GET key-count"
	curl --request   GET                                        \
       --header    "Content-Type: application/json"           \
       --write-out "%{http_code}\n"                           \
       http://127.0.0.1:13800/kvs/key-count
}

get_shard_test(){
	echo "Testing GET shards"
	curl --request   GET                                        \
       --header    "Content-Type: application/json"           \
       --write-out "%{http_code}\n"                           \
       http://127.0.0.1:13800/kvs/shards
}

getTest(){
	echo "============================================"
	echo "Testing GET\n\n"
	#First testing for invalid GET's
	echo "Testing missing key GET"
	curl --request   GET                                        \
       --header    "Content-Type: application/json"           \
       --write-out "%{http_code}\n"                           \
       --data      '{"causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/missingKey

    #Testing Valid GET
    echo "Testing Valid GET for existing key"
    curl --request   GET                                        \
       --header    "Content-Type: application/json"           \
       --write-out "%{http_code}\n"                           \
       --data      '{"causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/sampleKey
    key_count_test
    get_shard_test
}

#Tests concurrent PUTS, and PUTS and GETS together to check if we get expected values
putGetTest(){
	echo "============================================"
	echo "PUT and GET together\n\n"
	#Now going to test PUT's with causal-context, emulating concurrent requests
    echo "Testing Valid PUT, with equal VC as end nodes VC"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"changed sampleValue","causal-context":{"sampleKey": 2}}' \
       http://127.0.0.1:13802/kvs/keys/sampleKey
    echo "Testing GET, to see what value is there for sampleKey"
    curl --request   GET                                        \
       --header    "Content-Type: application/json"           \
       --write-out "%{http_code}\n"                           \
       --data      '{"causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/sampleKey
    echo "Testing Valid PUT with causal-context, with VC less than end nodes VC"
    curl --request   PUT                                                              \
       --header    "Content-Type: application/json"                                 \
       --write-out "%{http_code}\n"                                                 \
       --data      '{"value":"wrong sampleValue","causal-context":{"sampleKey": 1}}' \
       http://127.0.0.1:13802/kvs/keys/sampleKey
    echo "Testing GET, to see what value is there for sampleKey"
    curl --request   GET                                        \
       --header    "Content-Type: application/json"           \
       --write-out "%{http_code}\n"                           \
       --data      '{"causal-context":{}}' \
       http://127.0.0.1:13800/kvs/keys/sampleKey
}



test(){
	putTest
	getTest
	putGetTest
}

test
