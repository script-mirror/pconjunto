from middle.s3 import get_latest_webhook_product, handle_webhook_file
import datetime
import os
import zipfile
import concurrent.futures
from middle.utils import Constants
constants = Constants()
os.makedirs(constants.PATH_TMP, exist_ok=True)


def processar_item(item, nome_modelo):
    output = handle_webhook_file(item, constants.PATH_TMP)
    if '.zip' in output:
        with zipfile.ZipFile(os.path.join(constants.PATH_TMP, output), 'r') as zf:
            arquivo_para_extrair = [i for i in zf.namelist() if "_m_" in i.lower()][0]
            zf.extract(arquivo_para_extrair, f"./{nome_modelo}")
        os.remove(output)
    else:
        os.popen(f"mv {output} {nome_modelo}/")
    return f"Processado: {nome_modelo} {item['dataProduto']}"


def processar_webhook(nome_webhook: str, nome_modelo: str, date_range=200, max_workers=os.cpu_count()*4):
    items = get_latest_webhook_product(nome_webhook, datetime.datetime.now(), date_range=date_range)
    datas_disponiveis = get_datas_disponiveis(nome_modelo)
    items = [item for item in items if item['dataProduto'] not in datas_disponiveis]
    if not items:
        print(f"Nenhum novo item para processar em {nome_modelo}")
        return
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(processar_item, item, nome_modelo) for item in items]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(f"Erro no processamento: {e}")


def get_datas_disponiveis(path_modelo: str):
    arquivos = os.popen(f"ls {path_modelo}").read().split('\n')[:-1]
    datas = []
    for arquivo in arquivos:
        if 'observado' in path_modelo.lower():
            datas.append(datetime.datetime.strptime(arquivo[-12:][:-4], '%d%m%Y').strftime('%d/%m/%Y'))
        else:
            datas.append(datetime.datetime.strptime(arquivo[-10:][:-4], '%d%m%y').strftime('%d/%m/%Y'))
    return datas    
if __name__ == "__main__":
    processar_webhook("Modelo ETA", "./Arq_Entrada/ETA40")
    processar_webhook("Modelo ECMWF", "./Arq_Entrada/ECMWF")
    processar_webhook("Modelo GEFS", "./Arq_Entrada/GEFS")
    processar_webhook("Precipitação por Satélite – ONS", "./Arq_Entrada/Observado")
    processar_webhook("Precipitação por Satélite.", "./Arq_Entrada/Observado")
