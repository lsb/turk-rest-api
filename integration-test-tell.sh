# Cheap integration test.

epochseconds=`date +%s`
verb=ask
host="localhost:7000"
params="instructions=Color+survey&question=%7B%22Radio%22%3A%7B%22questionText%22%3A%22Do+you+like+the+color+blue%3F%22%2C%22chooseOne%22%3A%5B%22y%22%2C%22n%22%5D%7D%7D&distinctUsers=1&cost=1&uniqueAskId=$epochseconds"
echo Starting integration test at $epochseconds for 8c
curl -X PUT -d '' "${host}/ask?${params}"
curl -X PUT -d '' "${host}/tell?${params}&injectedWorker=werq&injectedAnswer=hella&injectionBatch=b"
echo We should have results
curl "${host}/ask?${params}"
