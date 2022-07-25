import boto3
import os
import datetime
import time
import pandas as pd


AWS_ACCESS_KEY = os.environ['AWS_ACESSKEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET']
S3_STAGING_DIR = "s3://aws-athena-query-results-595949041525-us-east-1/tedbot/"
S3_BUCKET_NAME = "aws-athena-query-results-595949041525-us-east-1"
S3_OUTPUT_DIRECTORY = "tedbot"

athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1',
)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1',
)


def test_feature():
  data_dict = get_mpe_data_dict()
  return get_mpe_currentmonth_text(data_dict)

def run_athena_query(query, test=False):
  if not test:
    query_response = athena_client.start_query_execution(
      QueryString=query,
      QueryExecutionContext={"Database": "digital",
                            "Catalog": "AwsDataCatalog"},
      ResultConfiguration={
          "OutputLocation": S3_STAGING_DIR,
          "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
      },
    )
    while True:
      try:
        # This function only loads the first 1000 rows
        athena_client.get_query_results(
          QueryExecutionId=query_response["QueryExecutionId"]
        )
        return query_response
      except Exception as err:
        if "not yet finished" in str(err):
          time.sleep(0.001)
        else:
          raise err
  return 'Test Case'

def download_s3_result(query_response, test=False):
  if not test:
    s3_client.download_file(
      S3_BUCKET_NAME,
      f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
      "athena_query_results.csv",)
  return pd.read_csv("athena_query_results.csv")

def get_mpe_currentmonth_text(data_dict):
  
  data_dict = get_mpe_data_dict()
  
  text  = 'Hello @Skywalkers!\n'
  text += f'📈 Resumo Resultados 🇲 🇵 🇪 até {data_dict["dia"]}/{data_dict["mes"]} 📊\n'
  text += '-----------------------------------------------------------------------\n\n'
  text += '**Concessão**\n'
  text += f'Segmento: Real R${data_dict["con_seg_real"]}k | Forecast: R${data_dict["con_seg_cast"]}k\n'
  text += f'Digital: Real R${data_dict["con_dig_real"]}k ({data_dict["con_perc_dig"]}%)| Forecast: R${data_dict["con_dig_cast"]}k\n\n'
  text += '**Receita por canal Digital:**\n'
  text += f'Crosseling: R${data_dict["con_CROSSELING_real"]}k | Forecast: R${data_dict["con_CROSSELING_cast"]}k\n'
  text += f'Digital Empresas: R${data_dict["con_DIGITAL EMPRESAS_real"]}k | Forecast: R${data_dict["con_DIGITAL EMPRESAS_cast"]}k\n'
  text += f'ON: R${data_dict["con_ON_real"]}k | Forecast: R${data_dict["con_ON_cast"]}k\n'
  text += f'Portal de Vendas: R${data_dict["con_PORTAL DE VENDAS DIGITAL_real"]}k | Forecast: R${data_dict["con_PORTAL DE VENDAS DIGITAL_cast"]}k\n'
  text += f'Tech: R${data_dict["con_TECH_real"]}k | Forecast: R${data_dict["con_TECH_cast"]}k\n'
  text += f'Whatsapp Vendas: R${data_dict["con_WHATSAPP_real"]}k | Forecast: R${data_dict["con_WHATSAPP_cast"]}k\n\n\n'
  text += '**Expansão**\n'
  text += f'Segmento: Real R${data_dict["exp_seg_real"]}k | Forecast: R${data_dict["exp_seg_cast"]}k\n'
  text += f'Digital: Real R${data_dict["exp_dig_real"]}k ({data_dict["exp_perc_dig"]}%)| Forecast: R${data_dict["exp_dig_cast"]}k\n\n'
  text += '**Receita por canal Digital:**\n'
  text += f'Crosseling: R${data_dict["exp_CROSSELING_real"]}k | Forecast: R${data_dict["exp_CROSSELING_cast"]}k\n'
  text += f'Digital Empresas: R${data_dict["exp_DIGITAL EMPRESAS_real"]}k | Forecast: R${data_dict["exp_DIGITAL EMPRESAS_cast"]}k\n'
  text += f'ON: R${data_dict["exp_ON_real"]}k | Forecast: R${data_dict["exp_ON_cast"]}k\n'
  text += f'Portal de Vendas: R${data_dict["exp_PORTAL DE VENDAS DIGITAL_real"]}k | Forecast: R${data_dict["exp_PORTAL DE VENDAS DIGITAL_cast"]}k\n'
  text += f'Tech: R${data_dict["exp_TECH_real"]}k | Forecast: R${data_dict["exp_TECH_cast"]}k\n'
  text += f'Whatsapp Vendas: R${data_dict["exp_WHATSAPP_real"]}k | Forecast: R${data_dict["exp_WHATSAPP_cast"]}k\n\n\n'
  text += '**Consolidado**\n'
  text += f'Segmento: Real R${data_dict["seg_real"]}k | Forecast: R${data_dict["seg_cast"]}k\n'
  text += f'Digital: Real R${data_dict["dig_real"]}k ({data_dict["perc_dig"]}%)| Forecast: R${data_dict["dig_cast"]}k\n'
  text += '-----------------------------------------------------------------------\n'
  text += '🏆 Destaques 🏆\n'
  text += f'No forecast do MPE Consolidado, estamos com {data_dict["ating"]}% de atingimento da meta de participação do mês de {data_dict["mes"]}/{data_dict["ano"]} ({data_dict["meta"]}%).'
  return text
  
def get_mpe_data_dict(type='parcial'):
  now = datetime.datetime.now()
  data_dict = {}
  if type == 'parcial':
    query = get_mpe_query()
    query_response = run_athena_query(query,test=False)
    df = download_s3_result(query_response,test=False)

    canais_digitais = ['WHATSAPP','TECH','PORTAL DE VENDAS DIGITAL','ON','DIGITAL EMPRESAS','CROSSELING']
    df['flag_digital'] = 0
    df.loc[df['canal_resumo_atualizado'].isin(canais_digitais), 'flag_digital'] = 1
    df_con = df.loc[df['area_atu']=='CONCESSÃO']
    df_exp = df.loc[df['area_atu']=='EXPANSÃO']

    #informações Gerais
    data_dict['dia'] = now.day-1
    data_dict['mes'] = now.month
    data_dict['ano'] = now.year
    data_dict['seg_real'] = round(df.receita.sum()/1000)
    data_dict['seg_cast'] = data_dict['seg_real']
    data_dict['dig_real'] = round(df.loc[df['flag_digital']==1].receita.sum()/1000)
    data_dict['dig_cast'] = data_dict['dig_real']
    data_dict['perc_dig'] = round((data_dict['dig_real']/
                                   data_dict['seg_real'])*100,1)
    data_dict['meta'] = 20.3
    data_dict['ating'] = round((data_dict['perc_dig']/data_dict['meta'])*100,1)

    #informações Concessão
    data_dict['con_seg_real'] = round(df_con.receita.sum()/1000)
    data_dict['con_seg_cast'] = data_dict['con_seg_real']
    data_dict['con_dig_real'] = round(df_con.loc[df_con['flag_digital']==1].receita.sum()/1000)
    data_dict['con_dig_cast'] = data_dict['con_dig_real']
    data_dict['con_perc_dig'] = round((data_dict['con_dig_real']/
                                   data_dict['con_seg_real'])*100,1)
    for cna in canais_digitais:
      data_dict[f'con_{cna}_real'] = round(df_con.loc[(df_con['canal_resumo_atualizado']==cna)].receita.sum()/1000)
      data_dict[f'con_{cna}_cast'] = data_dict[f'con_{cna}_real']

    #informações Expansão
    data_dict['exp_seg_real'] = round(df_exp.receita.sum()/1000)
    data_dict['exp_seg_cast'] = data_dict['exp_seg_real']
    data_dict['exp_dig_real'] = round(df_exp.loc[df_exp['flag_digital']==1].receita.sum()/1000)
    data_dict['exp_dig_cast'] = data_dict['exp_dig_real']
    data_dict['exp_perc_dig'] = round((data_dict['exp_dig_real']/
                                   data_dict['exp_seg_real'])*100,1)
    for cna in canais_digitais:
      data_dict[f'exp_{cna}_real'] = round(df_exp.loc[(df_exp['canal_resumo_atualizado']==cna)].receita.sum()/1000)
      data_dict[f'exp_{cna}_cast'] = data_dict[f'exp_{cna}_real']
    
    return data_dict



def get_mpe_query(type='parcial'):
  if type == 'parcial':
    return """
            SELECT 
            area_atu,
            canal_resumo_atualizado,
            sum(eixo_receita) as receita
            FROM trusted_esteira_vendas 
            WHERE segmento = 'EMPRESARIAL'
            AND status_resumo = 'FECHADO'
            AND flag_alteracao = '1'
            AND area_atu in ('CONCESSÃO','EXPANSÃO')
            AND cast(month as int) = cast(month(current_date) as int)
            GROUP BY area_atu, canal_resumo_atualizado
            """
  elif type == 'fechamento':
    return """
            SELECT 
            area_atu,
            canal_resumo_atualizado,
            sum(eixo_receita) as receita
            FROM trusted_esteira_vendas 
            WHERE segmento = 'EMPRESARIAL'
            AND status_resumo = 'FECHADO'
            AND flag_alteracao = '1'
            AND cast(month as int) =  cast(month(current_date) as int)-1
            GROUP BY area_atu, canal_resumo_atualizado
            """
    