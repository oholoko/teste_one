from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
from airflow.models.dagrun import DagRun

from datetime import datetime, timedelta

from deltalake import DeltaTable
from deltalake.writer import write_deltalake

import json

import os

import pandas as pd
import pyarrow

import requests

main_page = 'https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/'

with DAG(dag_id="extract_diaria",
         start_date=datetime(2024,1,1),
         schedule_interval="0 * * * *",
         params={"date": '',"interval": 10 },
         catchup=False) as dag:
    
    @task
    def extract_coin_types():
        response = requests.get(main_page+'/Moedas')
        response_json = response.json()
        return response_json['value']

    @task
    def extract_exchange_data(coin_list=[],dag_run: DagRun | None = None,**context):
        total_interval = context["params"]['interval']
        if dag.params['date'] == '':
            date_end = dag_run.queued_at
        else:
            date_end = datetime.strptime(context["params"]['date'],'%m-%d-%Y')
        files = []

        while total_interval > 0:
            if total_interval >= 30:
                interval = 30
                total_interval -= 30
            else:
                interval = total_interval
                total_interval = 0

            date_start = date_end - timedelta(days=int(interval))

            date_end_new = date_start - timedelta(days=1)

            date_start = date_start.strftime('%m-%d-%Y')
            date_end = date_end.strftime('%m-%d-%Y')

            for coin in coin_list:
                params = "?%40moeda='{}'&%40dataInicial='{}'&%40dataFinalCotacao='{}'&%24format=json".format(coin['simbolo'],date_start,date_end)
                response = requests.get(main_page+'/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)'+params)
                file_name = './stg/stg_{}_{}_{}.json'.format(coin['simbolo'],date_start,date_end)
                os.makedirs(os.path.dirname(file_name), exist_ok=True)
                with open(file_name,'w') as temp_json:
                    files.append(file_name)
                    temp_json.write(str(response.json()['value']).replace("'",'"'))

            date_end = date_end_new

        return files

    @task
    def bronze_layer(coin_list=[],files=[]):
        for file_name in files:
            df = pd.read_json(file_name,orient='records')
            
            df['dataHoraCotacao'] = df['dataHoraCotacao'].astype('datetime64[ns]') 
            
            df['coinType'] = file_name.split('_')[1]
            try:
                dt = DeltaTable('./brz/')
                delta_df = pyarrow.Table.from_pandas(df, preserve_index=False)
                (
                    dt.merge(
                        source=delta_df,
                        predicate="s.dataHoraCotacao = t.dataHoraCotacao and s.coinType = t.coinType",
                        source_alias="s",
                        target_alias="t",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
            except:
                write_deltalake(
                    './brz/',
                    df,
                    mode="overwrite",
                    engine="rust",
                )

        for coin in coin_list:
            print(coin)
            break
        return 0




    coin_list = extract_coin_types()
    files = extract_exchange_data(coin_list=coin_list)
    bronze_layer(coin_list=coin_list,files=files)