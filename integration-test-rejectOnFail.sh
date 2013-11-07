# Cheap integration test.

for i in true false
do
  echo Starting integration test for rejectOnFail, trying $i
  testurl="localhost:7000/ask?instructions=Survey&question=%7B%22Radio%22%3A%7B%22questionText%22%3A%22Do+you+like+the+color+blue%3F%22%2C%22chooseOne%22%3A%5B%22y%22%2C%22n%22%5D%7D%7D&distinctUsers=1&cost=4&uniqueAskId=ROF&knownAnswerQuestions=%7B%22percentCorrect%22%3A100%2C%22rejectOnFail%22%3A${i}%2C%22answeredQuestions%22%3A%5B%7B%22match%22%3A%7B%22Exact%22%3A%22oops%22%7D%2C%22question%22%3A%7B%22Radio%22%3A%7B%22questionText%22%3A%221%2B1%3D%22%2C%22chooseOne%22%3A%5B%220%22%2C%222%22%5D%7D%7D%7D%5D%7D"
  echo $testurl
  curl -X PUT -d '' $testurl
  echo Sleeping for 5 minutes...
  echo Ensure your answers have or have not been rejected, with the appropriate message.
  sleep 300
  echo We should have results for that money by this time.
  curl $testurl
done

