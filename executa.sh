#/bin/bash

./carrega_arquivos.sh

docker cp covid19.py jupyter-spark:/root
docker exec -it jupyter-spark spark-submit --master local[*] --executor-cores 2 --driver-memory 4G --num-executors 4 /root/covid19.py
