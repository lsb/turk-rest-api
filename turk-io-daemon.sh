while :
do
  curl -X POST -d '' localhost:6000/i
  curl -X POST -d '' localhost:6000/o
  sleep 10
done
