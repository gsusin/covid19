#/bin/bash

rm -rf entrada/*
docker exec -it namenode rm -rf /root/entrada
docker exec -it namenode hdfs dfs -rm -r /user/giancarlo/covid19/entrada
docker exec -it namenode hdfs dfs -rm -r /user/giancarlo/covid19/saida
docker exec -it namenode hdfs dfs -rm -r /user/hive/warehouse/covid19_municipio
