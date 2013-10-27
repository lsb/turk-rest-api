while :
do
  curl -X POST -d '' localhost:7000/i
  curl -X POST -d '' localhost:7000/o
  sleep 10
done
