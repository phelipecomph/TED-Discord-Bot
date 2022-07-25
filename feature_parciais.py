import boto3
import os
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
  query = get_mpe_query()
  query_response = run_athena_query(query,test=False)
  df = download_s3_result(query_response,test=False)
  print(df['eixo_receita'].sum())

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
  
def get_mpe_query():
  return """
          SELECT
          stv.data_ref,
          stv.mes_ref,
          stv.cpf_cnpj_cli,
          stv.eixo_receita,
          stv.segmento,
          stv.area_atu,
          stv.regional_contrato,
          stv.regional_contrato_atualizada,
          stv.regional_vendas,
          stv.familia_produto,
          stv.familia_pdt,
          stv.familia_produto_atualizada,
          stv.produto,
          stv.velocidade_pdt,
          stv.status_resumo,
          stv.canal_venda,
          stv.canal_resumo_atualizado,
          stv.nome_membro,
          stv.tipo_pessoa,
          stv.flag_franquia,
          stv.projeto,
          stv.dsc_cnae,
          stv.meio_pgt,
          stv.mdo_envio
          FROM "digital"."trusted_esteira_vendas" stv
          WHERE segmento = 'EMPRESARIAL'
          AND status_resumo = 'FECHADO'
          AND flag_alteracao = '1'
          AND to_date(data_ref,'dd/mm/yyyy') > date_add('month', -1, current_date) 
          order by data_ref
          """