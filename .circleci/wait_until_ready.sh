while [ "$(docker logs verdictdb-impala 2> /dev/null | grep -c "Impala is Started, Enjoy")" -le 0 ]
do
  echo "waiting for Impala"
  sleep 2
done