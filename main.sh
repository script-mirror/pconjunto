#!/bin/bash

set -e

data_rodada="${data_rodada:-$(date +%Y-%m-%d)}"

echo "=== Iniciando Pipeline PCONJUNTO para data: $data_rodada ==="

# Etapa 1: Baixar dados da S3
echo "=== [1/4] Baixando dados da S3 ==="
python3 /app/setup_modelos.py
if [ $? -ne 0 ]; then
    echo "ERRO: Falha ao executar setup_modelos"
    exit 1
fi

# Etapa 2: Processar entrada
echo "=== [2/4] Processando entrada ==="
python3 /app/main.py process_input "${data_rodada}"
if [ $? -ne 0 ]; then
    echo "ERRO: Falha ao executar process_input"
    exit 1
fi

# Etapa 3: Executar modelo PCONJUNTO (R)
echo "=== [3/4] Executando modelo PCONJUNTO ==="
Rscript /app/Codigos_R/Roda_Conjunto_V3.4.R
if [ $? -ne 0 ]; then
    echo "ERRO: Falha ao executar script R"
    exit 1
fi

# Etapa 4: Processar saída
echo "=== [4/4] Processando saída ==="
python3 /app/main.py process_output "${data_rodada}"
if [ $? -ne 0 ]; then
    echo "ERRO: Falha ao executar process_output"
    exit 1
fi

echo "=== Pipeline PCONJUNTO finalizado com sucesso! ==="