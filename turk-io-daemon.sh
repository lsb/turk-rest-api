while :
do
  curl -X POST -d '' localhost:4567/i
  curl -X POST -d '' localhost:4567/o
  sleep 10
done
