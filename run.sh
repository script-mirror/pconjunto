#!/bin/bash

docker run --rm -v ./Arq_Entrada:/app/Arq_Entrada/ -v ./Arq_Saida:/app/Arq_Saida/ pconjunto:latest 