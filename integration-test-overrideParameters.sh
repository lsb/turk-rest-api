# Cheap integration test.

epochseconds=`date +%s`
testurl="localhost:7000/ask?instructions=Color+survey&question=%7B%22Radio%22%3A%7B%22questionText%22%3A%22Do+you+like+the+color+blue%3F%22%2C%22chooseOne%22%3A%5B%22y%22%2C%22n%22%5D%7D%7D&distinctUsers=2&cost=4&uniqueAskId=$epochseconds&overrideParameters=%7B%22QualificationRequirement.1.IntegerValue%22%3A%2287%22%2C%22QualificationRequirement.2.IntegerValue%22%3A%225%22%7D"
echo Starting integration test at $epochseconds for 8c
curl -X PUT -d '' $testurl
echo Sleeping for 5 minutes...
echo Check that this is only for 87th percentile turkers who have answered over 5 questions.
sleep 300
echo We should have results for that money by this time.
curl $testurl
