#!/bin/bash

data_rodada="$1"

python3 main.py process_input "${data_rodada}"

Rscript ./Codigos_R/Roda_Conjunto_V3.4.R

python3 main.py process_output "${data_rodada}"
