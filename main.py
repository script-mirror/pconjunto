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
    logger.info("Buscando dados dos postos/subbacias...")
    res = requests.get(
        f"{constants.BASE_URL}/api/v2/rodadas/subbacias",
        headers=get_auth_header(),
    )
    df = pd.DataFrame(res.json())
    logger.info(f"Obtidos {len(df)} postos/subbacias")
    return df


def post_chuva(df: pd.DataFrame):
    modelo = df['modelo'].unique()[0]
    logger.info(f'Enviando dados de chuva para o modelo {modelo} ({len(df)} registros)')
    df['dt_prevista'] = pd.to_datetime(df['dt_prevista']).dt.strftime('%Y-%m-%d')
    df['dt_rodada'] = pd.to_datetime(df['dt_rodada']).dt.strftime('%Y-%m-%dT00:00:00')
    res = requests.post(
        f"{constants.BASE_URL}/api/v2/rodadas/chuva/previsao/modelos",
        json=df.to_dict(orient='records'),
        headers=get_auth_header(),
    )

    res.raise_for_status()
    logger.info(f'Dados de chuva enviados com sucesso para o modelo {modelo}')
    return res.json()


def get_chuva(
    nome_modelo: str,
    data_rodada: datetime.date,
):
    logger.info(f"Buscando dados de chuva do modelo {nome_modelo} para a rodada {data_rodada}")
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
    logger.info(f"Obtidos {len(df)} registros de chuva para o modelo {nome_modelo}")
    return df[['cd_subbacia', 'dt_prevista', 'vl_chuva', 'dt_rodada', 'modelo']].copy()


def write_date_input(data_rodada: datetime.date):
    logger.info(f"Escrevendo data de entrada: {data_rodada}")
    with open("Arq_Entrada/data.txt", "w") as f:
        f.write(data_rodada.strftime('%d/%m/%Y'))
        f.write("\n")
    logger.info("Arquivo de data criado com sucesso")


def clear_previous_output():
    logger.info("Limpando arquivos de saida anteriores...")
    os.popen("rm -rf Arq_Saida/*")
    logger.info("Limpeza concluida")


def verificar_arquivos_entrada(data_rodada: datetime.date):
    logger.info(f"Verificando arquivos de entrada para a data {data_rodada}")
    modelos = ['ECMWF', 'ETA40', 'GEFS']
    for modelo in modelos:
        pasta = f"Arq_Entrada/{modelo}"
        nome_arquivo = f"{modelo}_m_{data_rodada.strftime('%d%m%y')}.dat"
        
        logger.info(f"Verificando arquivo {nome_arquivo} na pasta {pasta}")
        
        result = os.popen(f"ls {pasta}").read().strip().split('\n')
        arquivos = [arq for arq in result if arq]
        
        if nome_arquivo not in arquivos:
            erro_msg = f"Arquivo {nome_arquivo} nao encontrado na pasta {pasta}."
            logger.error(erro_msg)
            raise FileNotFoundError(erro_msg)
        
        logger.info(f"Arquivo {nome_arquivo} encontrado com sucesso")
    
    data_observado = data_rodada - datetime.timedelta(days=1)
    pasta_observado = "Arq_Entrada/Observado"
    nome_arquivo_observado = f"psat_{data_observado.strftime('%d%m%Y')}.txt"
    
    logger.info(f"Verificando arquivo observado {nome_arquivo_observado} na pasta {pasta_observado}")
    
    result_obs = os.popen(f"ls {pasta_observado}").read().strip().split('\n')
    arquivos_obs = [arq for arq in result_obs if arq]
    
    if nome_arquivo_observado not in arquivos_obs:
        erro_msg = f"Arquivo observado {nome_arquivo_observado} nao encontrado na pasta {pasta_observado}."
        logger.error(erro_msg)
        raise FileNotFoundError(erro_msg)
    
    logger.info(f"Arquivo observado {nome_arquivo_observado} encontrado com sucesso")
    logger.info("Todos os arquivos de entrada verificados com sucesso")

def process_input(data_rodada: datetime.date):
    logger.info(f"Iniciando processamento de entrada para a data {data_rodada}")
    clear_previous_output()
    verificar_arquivos_entrada(data_rodada)
    write_date_input(data_rodada)
    logger.info("Processamento de entrada concluido")


def process_remvies_models(data_rodada: datetime.date):
    logger.info(f"Iniciando processamento dos modelos REMVIES para a data {data_rodada}")
    postos = get_postos()[['nome', 'id']].rename(columns={'nome': 'subbacia', 'id': 'cd_subbacia'})
    modelos = ['ECMWF', 'ETA40', 'GEFS']
    for modelo in modelos:
        logger.info(f"Processando modelo {modelo}")
        df = pd.read_fwf(f"Arq_Saida/{modelo}_rem_vies.dat", header=None).dropna(axis=1)
        df.columns = ['subbacia', 'lon', 'lat', *[data_rodada + datetime.timedelta(days=x+1) for x in range(len(df.columns)-3)]]

        df = df.merge(postos, on='subbacia', how='left')
        df.drop(columns=['subbacia', 'lon', 'lat'], inplace=True)
        df = df.melt(id_vars=['cd_subbacia'], var_name='dt_prevista', value_name='vl_chuva')
        df['dt_rodada'] = f"{data_rodada}T00:00:00"
        df['modelo'] = f'{modelo}-REMVIES-ONS'
        post_chuva(df)
        logger.info(f"Modelo {modelo} processado com sucesso")
    logger.info("Processamento dos modelos REMVIES concluido")


def send_pmedia_file(data_rodada: datetime.date):
    logger.info(f"Enviando arquivos PMEDIA para a data {data_rodada}")
    arquivos = glob.glob("Arq_Saida/PMEDIA_p*.dat")
    logger.info(f"Encontrados {len(arquivos)} arquivos PMEDIA para compactar")
    path_zip = f'Arq_Saida/PMEDIA_{data_rodada}.zip'
    with zipfile.ZipFile(path_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for arquivo in arquivos:
            zipf.write(arquivo, arcname=arquivo.split('/')[-1])
    logger.info(f"Arquivo compactado criado: {path_zip}")
    upload_raw_rain_map(
        file_path=path_zip,
        modelo='PMEDIA-ONS',
        rodada=f"{data_rodada}T00:00:00",
    )
    logger.info("Upload do arquivo PMEDIA concluido")

def process_pmedia(data_rodada: datetime.date):
    logger.info(f"Iniciando processamento PMEDIA para a data {data_rodada}")
    df_completo = pd.DataFrame(columns=['cd_subbacia', 'dt_prevista', 'vl_chuva', 'dt_rodada', 'modelo'])
    postos = get_postos()[['nome', 'id', 'vl_lon', 'vl_lat']].rename(columns={'nome': 'subbacia', 'id': 'cd_subbacia'})
    for i in range(14):
        data_prevista = data_rodada + datetime.timedelta(i+1)
        pmedia_diario = f"Arq_Saida/PMEDIA_p{data_rodada.strftime('%d%m%y')}a{data_prevista.strftime('%d%m%y')}.dat"
        logger.info(f"Processando arquivo PMEDIA para {data_prevista}: {pmedia_diario}")
        df = pd.read_fwf(pmedia_diario, header=None)
        df.columns = ["vl_lon", "vl_lat", "vl_chuva"]
        df = df.merge(postos, on=['vl_lon', 'vl_lat'], how='left')
        df = df[['cd_subbacia', 'vl_chuva']]
        df['dt_prevista'] = f"{data_prevista}"
        df_completo = pd.concat([df_completo, df], ignore_index=True)
    df_completo['dt_rodada'] = f"{data_rodada}T00:00:00"
    df_completo['modelo'] = "PMEDIA-ONS"
    logger.info(f"PMEDIA processado com {len(df_completo)} registros")
    post_chuva(df_completo)
    send_pmedia_file(data_rodada)
    logger.info("Processamento PMEDIA concluido")
    return pd.DataFrame(df_completo)

def send_sensitivity_file(df_sensibilidade: pd.DataFrame, data_rodada: datetime.date, modelo: str):
    logger.info(f"Enviando arquivo de sensibilidade para o modelo {modelo}")
    arquivos = []
    for data_prevista in df_sensibilidade['dt_prevista'].unique():
        data_prevista = datetime.datetime.strptime(str(data_prevista), '%Y-%m-%d').date()
        df_dia = df_sensibilidade[df_sensibilidade['dt_prevista'] == data_prevista]
        path_arquivo = f'Arq_Saida/{modelo}_p{data_rodada.strftime("%d%m%y")}a{data_prevista.strftime("%d%m%y")}.dat'
        arquivos.append(path_arquivo)
        df_dia[['vl_lon', 'vl_lat', 'vl_chuva']].to_csv(path_arquivo, index=False, header=False, sep=' ')
    
    path_zip = f'Arq_Saida/{modelo}_{data_rodada}.zip'
    logger.info(f"Criando arquivo compactado: {path_zip}")
    with zipfile.ZipFile(path_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for arquivo in arquivos:
            zipf.write(arquivo, arcname=arquivo.split('/')[-1])
    upload_raw_rain_map(
        file_path=path_zip,
        modelo=modelo,
        rodada=f"{data_rodada}T00:00:00",
    )
    logger.info(f"Upload do arquivo {modelo} concluido")


def generate_model_sensitivity(
    modelo: str,
    data_rodada: datetime.date,
    df_pmedia: pd.DataFrame,
    proxima_quinta: datetime.date,
    nome_modelo_saida: str,
):
    logger.info(f"Gerando modelo de sensibilidade {nome_modelo_saida} baseado em {modelo}")
    df_pmedia = df_pmedia[df_pmedia['dt_prevista'] >= proxima_quinta]
    df_chuva = get_chuva(modelo, data_rodada)
    df_chuva = df_chuva[df_chuva['dt_prevista'] < proxima_quinta]
    logger.info(f"Combinando {len(df_chuva)} registros de {modelo} com {len(df_pmedia)} registros PMEDIA")
    df_sensibilidade = pd.concat([df_chuva, df_pmedia], ignore_index=True)
    df_sensibilidade = df_sensibilidade.sort_values(['cd_subbacia', 'dt_prevista'])
    df_sensibilidade['modelo'] = nome_modelo_saida
    df_postos = get_postos()[['id', 'vl_lat', 'vl_lon']].rename(columns={'id': 'cd_subbacia'})
    post_chuva(df_sensibilidade)
    send_sensitivity_file(df_sensibilidade.merge(df_postos, on='cd_subbacia'), data_rodada, nome_modelo_saida)
    logger.info(f"Modelo de sensibilidade {nome_modelo_saida} processado com sucesso")


def generate_derived_models(data_rodada: datetime.date, df_pmedia: pd.DataFrame):
    logger.info(f"Iniciando geracao de modelos derivados para a data {data_rodada}")
    df_pmedia['dt_rodada'] = pd.to_datetime(df_pmedia['dt_rodada']).dt.date
    df_pmedia['dt_prevista'] = pd.to_datetime(df_pmedia['dt_prevista']).dt.date
    data_rodada = df_pmedia['dt_rodada'].unique()[0]
    proxima_quinta = data_rodada + datetime.timedelta(days=(3-data_rodada.weekday()+7)%7)
    logger.info(f"Próxima quinta-feira calculada: {proxima_quinta}")
    generate_model_sensitivity("GEFS-ONS", data_rodada, df_pmedia, proxima_quinta, "PCONJUNTO-ONS")
    generate_model_sensitivity("ECMWF-ONS", data_rodada, df_pmedia, proxima_quinta, "PCONJUNTO2-ONS")
    logger.info("Geracao de modelos derivados concluida")



def process_output(data_rodada: datetime.date):
    logger.info(f"Iniciando processamento de saida para a data {data_rodada}")
    process_remvies_models(data_rodada)
    df_pmedia = process_pmedia(data_rodada)
    generate_derived_models(data_rodada, df_pmedia)
    logger.info("Processamento de saida concluido")


def upload_raw_rain_map(
    file_path: str,
    modelo: str,
    rodada: str,
) -> dict:
    logger.info(f"Iniciando upload do arquivo {file_path} para o modelo {modelo}")
    if not os.path.exists(file_path):
        logger.error(f"Arquivo nao encontrado: {file_path}")
        raise FileNotFoundError(f"Arquivo nao encontrado: {file_path}")
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
        logger.error(f'Erro HTTP durante upload: {e} - Status: {response.status_code} - Response: {response.text}')
        return {
            'success': False,
            'status_code': response.status_code,
            'error': f'Erro HTTP: {e}',
            'response_text': response.text
        }
    except requests.exceptions.RequestException as e:
        logger.error(f'Erro na requisicao durante upload: {e}')
        return {
            'success': False,
            'status_code': None,
            'error': f'Erro na requisicao: {e}',
            'response_text': None
        }
    except Exception as e:
        logger.error(f'Erro inesperado durante upload: {e}')
        return {
            'success': False,
            'status_code': None,
            'error': f'Erro inesperado: {e}',
            'response_text': None
        }



if __name__ == "__main__":
    logger.info("Iniciando aplicacao PCONJUNTO")
    if len(sys.argv) <= 2:
        data_rodada = datetime.date.today()
        logger.info(f"Usando data atual: {data_rodada}")
    else:
        data_rodada = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
        logger.info(f"Data informada via parâmetro: {data_rodada}")
    
    if 'process_input' in sys.argv:
        logger.info("Executando processamento de entrada")
        process_input(data_rodada)
    elif 'process_output' in sys.argv:
        logger.info("Executando processamento de saida")
        process_output(data_rodada)
    else:
        logger.error("Comando inválido. Use 'process_input' ou 'process_output'.")
        raise ValueError("Comando invalido. 'process_input' ou 'process_output'.")
    
    logger.info("Aplicacao finalizada com sucesso")