FROM rocker/r-ver:latest

WORKDIR /app

USER root

COPY .env /root/.env

RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    python3 \
    python3-pip \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN R -e "install.packages(c('lpSolve', 'readxl', 'minpack.lm'), repos='https://cloud.r-project.org/')"

COPY requirements.txt .

ARG GIT_USERNAME
ARG GIT_TOKEN

RUN sed -i "s/\${GIT_USERNAME}/${GIT_USERNAME}/g" requirements.txt && \
    sed -i "s/\${GIT_TOKEN}/${GIT_TOKEN}/g" requirements.txt

RUN git config --global credential.helper store && \
    echo "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com" > ~/.git-credentials

RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY . .

RUN chmod +x /app/main.sh

RUN mkdir -p /app/Arq_Entrada/ETA40 && \
    mkdir -p /app/Arq_Entrada/ECMWF && \
    mkdir -p /app/Arq_Entrada/GEFS && \
    mkdir -p /app/Arq_Entrada/Observado && \
    mkdir -p /app/Arq_Saida

ENV data_rodada=""

CMD ["/app/main.sh"]