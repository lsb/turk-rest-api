while :
do
 for i in {1..4} ; do
  curl -X POST -d '' localhost:7000/i
  curl -X POST -d '' localhost:7000/o
  sleep 10
 done
 curl -X POST -d '' localhost:7000/d
done
