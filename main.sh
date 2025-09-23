#!/bin/bash

set -e

data_rodada="$1"

# Executa o primeiro comando Python e verifica se houve erro
python3 main.py process_input "${data_rodada}"
if [ $? -ne 0 ]; then
    echo "Erro ao executar process_input. Encerrando execução."
    exit 1
fi

# Executa o script R
Rscript ./Codigos_R/Roda_Conjunto_V3.4.R
if [ $? -ne 0 ]; then
    echo "Erro ao executar script R. Encerrando execução."
    exit 1
fi

# Executa o segundo comando Python
python3 main.py process_output "${data_rodada}"
if [ $? -ne 0 ]; then
    echo "Erro ao executar process_output. Encerrando execução."
    exit 1
fi