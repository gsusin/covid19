#/bin/bash

#Pasta do HDFS
PASTA=/user/giancarlo/covid19

#Arquivo no padrão de https://covid.saude.gov.br/
ARQUIVO=04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar
#ARQUIVO=HIST_PAINEL_COVIDBR_02dez2022.rar

rar e $ARQUIVO entrada/
docker cp entrada namenode:/root
docker exec -it namenode hdfs dfs -mkdir -p $PASTA
docker exec -it namenode hdfs dfs -put /root/entrada $PASTA
docker exec -it namenode hdfs dfs -ls $PASTA/entrada
