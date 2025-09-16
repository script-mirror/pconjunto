import os
import sys
import datetime
import requests
import pandas as pd
from middle.utils import get_auth_header, setup_logger

logger = setup_logger()

def get_postos():
    res = requests.get(
        "https://tradingenergiarz.com/api/v2/rodadas/subbacias",
        headers=get_auth_header(),
    )
    df = pd.DataFrame(res.json())
    return df


def post_chuva(df: pd.DataFrame):
    logger.info(f'POST modelo {df['modelo'].unique()[0]}')
    return None
    res = requests.post(
        "https://tradingenergiarz.com/api/v2/rodadas/chuva/previsao/modelos",
        json=df.to_dict(orient='records'),
        headers=get_auth_header(),
    )
    res.raise_for_status()
    return res.json()


def get_chuva(
    nome_modelo: str,
    data_rodada: datetime.date,
):
    res = requests.get(
        "https://tradingenergiarz.com/api/v2/rodadas/chuva/previsao",
        params={
            "nome_modelo":nome_modelo,
            "dt_hr_rodada":data_rodada.strftime('%Y-%m-%dT00:00:00'),
        },
        headers=get_auth_header(),
    )
    res.raise_for_status()
    df = pd.DataFrame(res.json())
    df['dt_prevista'] = pd.to_datetime(df['dt_prevista']).dt.date
    df['dt_rodada'] = pd.to_datetime(df['dt_rodada']).dt.date
    return df[['cd_subbacia', 'dt_prevista', 'vl_chuva', 'dt_rodada', 'modelo']].copy()


def write_date_input(data_rodada: datetime.date):
    logger.info(f"Data de entrada: {data_rodada}")
    with open("Arq_Entrada/data.txt", "w") as f:
        f.write(data_rodada.strftime('%d/%m/%Y'))
        f.write("\n")


def clear_previous_output():
    logger.info("Limpando arquivos de saida anteriores...")
    os.popen("rm -rf Arq_Saida/*")


def process_input(data_rodada: datetime.date):
    write_date_input(data_rodada)
    clear_previous_output()


def process_remvies_models(data_rodada: datetime.date):
    postos = get_postos()[['nome', 'id']].rename(columns={'nome': 'subbacia', 'id': 'cd_subbacia'})
    modelos = ['ECMWF', 'ETA40', 'GEFS']
    for modelo in modelos:
        df = pd.read_fwf(f"Arq_Saida/{modelo}_rem_vies.dat", header=None)
        df.columns = ['subbacia', 'lon', 'lat', *[data_rodada + datetime.timedelta(days=x+1) for x in range(len(df.columns)-3)]]

        df = df.merge(postos, on='subbacia', how='left')
        df.drop(columns=['subbacia', 'lon', 'lat'], inplace=True)
        df = df.melt(id_vars=['cd_subbacia'], var_name='dt_prevista', value_name='vl_chuva')
        df['dt_rodada'] = f"{data_rodada}T00:00:00"
        df['modelo'] = f'{modelo}-REMVIES-ONS'
        post_chuva(df)


def process_pmedia(data_rodada: datetime.date):
    df_completo = pd.DataFrame(columns=['cd_subbacia', 'dt_prevista', 'vl_chuva', 'dt_rodada', 'modelo'])
    postos = get_postos()[['nome', 'id', 'vl_lon', 'vl_lat']].rename(columns={'nome': 'subbacia', 'id': 'cd_subbacia'})
    for i in range(14):
        data_prevista = data_rodada + datetime.timedelta(i+1)
        pmedia_diario = f"Arq_Saida/PMEDIA_p{data_rodada.strftime('%d%m%y')}a{data_prevista.strftime('%d%m%y')}.dat"
        df = pd.read_fwf(pmedia_diario, header=None)
        df.columns = ["vl_lon", "vl_lat", "vl_chuva"]
        df = df.merge(postos, on=['vl_lon', 'vl_lat'], how='left')
        df = df[['cd_subbacia', 'vl_chuva']]
        df['dt_prevista'] = f"{data_prevista}"
        df_completo = pd.concat([df_completo, df], ignore_index=True)
    df_completo['dt_rodada'] = f"{data_rodada}T00:00:00"
    df_completo['modelo'] = "PMEDIA-ONS"
    post_chuva(df_completo)
    return df_completo


def generate_model_sensitivity(
    modelo: str,
    data_rodada: datetime.date,
    df_pmedia: pd.DataFrame,
    proxima_quinta: datetime.date,
    nome_modelo_saida: str,
):
    df_pmedia = df_pmedia[df_pmedia['dt_prevista'] >= proxima_quinta]
    df_chuva = get_chuva(modelo, data_rodada)
    df_chuva = df_chuva[df_chuva['dt_prevista'] < proxima_quinta]
    df_pconjunto = pd.concat([df_chuva, df_pmedia], ignore_index=True)
    df_pconjunto = df_pconjunto.sort_values(['cd_subbacia', 'dt_prevista'])
    df_pconjunto['modelo'] = nome_modelo_saida
    post_chuva(df_pconjunto)


def generate_derived_models(data_rodada: datetime.date, df_pmedia: pd.DataFrame):
    df_pmedia['dt_rodada'] = pd.to_datetime(df_pmedia['dt_rodada']).dt.date
    df_pmedia['dt_prevista'] = pd.to_datetime(df_pmedia['dt_prevista']).dt.date
    data_rodada = df_pmedia['dt_rodada'].unique()[0]
    proxima_quinta = data_rodada + datetime.timedelta(days=(3-data_rodada.weekday()+7)%7)
    generate_model_sensitivity("GEFS-ONS", data_rodada, df_pmedia, proxima_quinta, "PCONJUNTO-ONS")
    generate_model_sensitivity("ECMWF-ENS-ONS", data_rodada, df_pmedia, proxima_quinta, "PCONJUNTO2-ONS")


def process_output(data_rodada: datetime.date):
    process_remvies_models(data_rodada)
    df_pmedia = process_pmedia(data_rodada)
    generate_derived_models(data_rodada, df_pmedia)


if __name__ == "__main__":
    if len(sys.argv) <= 2:
        data_rodada = datetime.date.today() - datetime.timedelta(1)
    else:
        data_rodada = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    if 'process_input' in sys.argv:
        process_input(data_rodada)
    elif 'process_output' in sys.argv:
        process_output(data_rodada)
    else:
        raise ValueError("Comando invalido. 'process_input' ou 'process_output'.")