import os
import pdb
import sys
import glob
import zipfile
import datetime
import requests
import pandas as pd
from middle.utils import get_auth_header, setup_logger, Constants

constants = Constants()

logger = setup_logger()

def get_postos():
    res = requests.get(
        f"{constants.BASE_URL}/api/v2/rodadas/subbacias",
        headers=get_auth_header(),
    )
    df = pd.DataFrame(res.json())
    return df


def post_chuva(df: pd.DataFrame):
    logger.info(f'POST modelo {df['modelo'].unique()[0]}')

    res = requests.post(
        f"{constants.BASE_URL}/api/v2/rodadas/chuva/previsao/modelos",
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
        f"{constants.BASE_URL}/api/v2/rodadas/chuva/previsao",
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


def send_pmedia_file(data_rodada: datetime.date):
    arquivos = glob.glob("Arq_Saida/PMEDIA_p*.dat")
    path_zip = f'Arq_Saida/PMEDIA_{data_rodada}.zip'
    with zipfile.ZipFile(path_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for arquivo in arquivos:
            zipf.write(arquivo, arcname=arquivo.split('/')[-1])
    upload_raw_rain_map(
        file_path=path_zip,
        modelo='PMEDIA-ONS',
        rodada=f"{data_rodada}T00:00:00",
    )

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
    send_pmedia_file(data_rodada)
    return pd.DataFrame(df_completo)

def send_sensitivity_file(df_sensibilidade: pd.DataFrame, data_rodada: datetime.date, modelo: str):
    arquivos = []
    for data_prevista in df_sensibilidade['dt_prevista'].unique():
        df_dia = df_sensibilidade[df_sensibilidade['dt_prevista'] == data_prevista]
        path_arquivo = f'Arq_Saida/{modelo}_p{data_rodada.strftime("%d%m%y")}a{data_prevista.strftime("%d%m%y")}.dat'
        arquivos.append(path_arquivo)
        df_dia[['vl_lon', 'vl_lat', 'vl_chuva']].to_csv(path_arquivo, index=False, header=False, sep=' ')
    
    path_zip = f'Arq_Saida/{modelo}_{data_rodada}.zip'
    with zipfile.ZipFile(path_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for arquivo in arquivos:
            zipf.write(arquivo, arcname=arquivo.split('/')[-1])
    upload_raw_rain_map(
        file_path=path_zip,
        modelo=modelo,
        rodada=f"{data_rodada}T00:00:00",
    )


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
    df_sensibilidade = pd.concat([df_chuva, df_pmedia], ignore_index=True)
    df_sensibilidade = df_sensibilidade.sort_values(['cd_subbacia', 'dt_prevista'])
    df_sensibilidade['modelo'] = nome_modelo_saida
    df_postos = get_postos()[['id', 'vl_lat', 'vl_lon']].rename(columns={'id': 'cd_subbacia'})
    post_chuva(df_sensibilidade)
    send_sensitivity_file(df_sensibilidade.merge(df_postos, on='cd_subbacia'), data_rodada, nome_modelo_saida)


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


def upload_raw_rain_map(
    file_path: str,
    modelo: str,
    rodada: str,
) -> dict:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    headers = get_auth_header()
    data = {
        'modelo': modelo,
        'rodada': rodada
    }
    
    try:
        with open(file_path, 'rb') as file:
            files = {'file': file}
            response = requests.post(f"{constants.BASE_URL}/pluv/api/raw-rain-map/", headers=headers, data=data, files=files)
            response.raise_for_status()
            logger.info(f'Upload realizado com sucesso: {response.text} Status Code: {response.status_code}')
            return {
                'success': True,
                'status_code': response.status_code,
                'data': response.json(),
                'message': 'Upload realizado com sucesso'
            }
            
    except requests.exceptions.HTTPError as e:
        return {
            'success': False,
            'status_code': response.status_code,
            'error': f'Erro HTTP: {e}',
            'response_text': response.text
        }
    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'status_code': None,
            'error': f'Erro na requisição: {e}',
            'response_text': None
        }
    except Exception as e:
        return {
            'success': False,
            'status_code': None,
            'error': f'Erro inesperado: {e}',
            'response_text': None
        }



if __name__ == "__main__":
    if len(sys.argv) <= 2:
        data_rodada = datetime.date.today()
    else:
        data_rodada = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    if 'process_input' in sys.argv:
        process_input(data_rodada)
    elif 'process_output' in sys.argv:
        process_output(data_rodada)
    else:
        raise ValueError("Comando invalido. 'process_input' ou 'process_output'.")