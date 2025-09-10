FROM rocker/r-ver:latest

WORKDIR /app

USER root

RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libcurl4-openssl-dev \
    libssl-dev \
 && rm -rf /var/lib/apt/lists/*

RUN R -e "install.packages(c('lpSolve', 'readxl', 'minpack.lm'), repos='https://cloud.r-project.org/')"

COPY . .

CMD ["Rscript", "./Codigos_R/Roda_Conjunto_V3.4.R"]
