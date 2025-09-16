#!/bin/bash
data_rodada="$1"
echo ${data_rodada}
container_name="pconjunto_$(date +%s)"
trap 'echo "Parando container..."; docker stop $container_name; docker rm $container_name; exit' SIGINT
docker run --rm --name $container_name \
    -v ./Arq_Entrada:/app/Arq_Entrada/ \
    -v ./Arq_Saida:/app/Arq_Saida/ \
    -e data_rodada=${data_rodada}\
    pconjunto:latest