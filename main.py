from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import json


app = FastAPI()
project_id = "sys-67738525349545571962304921"
cred={'type': 'service_account', 'project_id': 'sys-67738525349545571962304921', 'private_key_id': '91927d3950bfa5e5249a0cc4f42d4625a99f4503', 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1BaE36Via8sak\noerV17EZzGlXeBjgsscbVnLuyg6ACDlY7CrCljjdOfmn+4gKbe1491WwJtxuhr/0\ne4od5jt2HrKTfR5yiFb3EujfcZrl7kA8OPnSp7Z5StZoos4AHAMJBcgGGMfbHVA4\nQ+ZPjXXSIGWJdh8/SV0Es51VUQi+Lms3YK0fzufME4qceS9nybYmm3ccrZSG51zn\nhK7SNmRZZCZIfOQGmXa/hqZw17B1TFKIR9PefkYtWI8yTOvsZ6crzttiouQzulOl\n5DQwmbfOeEWMKT6HoXPTa+V381S7WRAK8YF98+DVqerjBPt3DND+/YL2ZwA/zqJz\ne5iz2D7jAgMBAAECggEADbW48ZabLtUPRV27/ukgkR8hpU3DuJThrojcGIi2E21M\nBpeQX39oHB0tctMCiSOtNhmpZDd1P2u2Mwp+Oeh7fWUyyifSPANmbr0AZRfiDuL9\n+3GnPhSUpdgMqA0Yg/qbIj5NWWTcEhTExBYkZccFct4gQopvMGhagqYl1tXVzy1t\nNpE0Ge+uishasM0AE0c6eGppyfm7zb/Df0Q6vDfmGHSiO4WCwLjUm9BayGS68Pt1\n7MTOqO3B5Eo4KTcahCY41+iXvu8K2EZ1DRRS9xvBs0pBqK68fSuQSMSUzF18CHzb\n5tg/OgLWQxiSs8jOLUHox6k4dkF2NejqYh3plCsnQQKBgQDYjtcAWlbCalDKWjX8\noV3YOuwY1LrhsF8IuCnynkAlKKrleZHmbFubrYN6C1GDKo4QPM5j7IuHRXEz8Pfv\nsym9VJPeOqE0NBy8HQV3NtLzwypaNUlGi250uSEpddSS7p0AL7p5mhSet9aTSdBq\n6cSVbm13GFkjoFvwir+kEFRCwQKBgQDV/ea10Npmf8IBKO23d5Is5xMvHry8MzZI\noVpFWPym5HS5XEA06APDd3ccyCndDcJQAJld6C3CLbTxZdWhg2WGSpWnBarlSVQh\nUcRawHAMvSR0VGeUjOuQvNecutpEx5u6qs2AaT7kj2fQyOnH3Ux1w+GbtytdhOfI\nyBjIgsc+owKBgGrlZ1+3OChTjnm0Of3wMYCw5SYErBMHmoGVVq96SjONdX48mjZh\nun6IEeRGff//G40MVtygQOeO8agwBFL/31Sj0THbQwOfzadVtAL6vvqwldFdiEQY\nQ3e+go4SqdG1ky4qYSPxWMhX+sVNpGGB7xXMIqCtFiMt3vRHqP11SgKBAoGBAIDk\n3Xl4YoTIwV+XepA+6oI3cVu5hO9LXZAj+E67CfuwsgoQYfA8LEApjkp82pJ2visY\nIUjqF93VUB7zOtl9XsKj3D5tcIGJSK6FJOOQ9C0IJJQZXwagVyeoR6r09ZHmNYwb\nY4rMWgCrzFl7Gy2yw2JP6W20x98dtcs/k4X7F+5HAoGAXLQcxSEn3yDqYcqqihTd\nCwwj/C8PCVtExuY2nbd5K72G6DeJOzqq5XRAYM+ONPlofSBioVXtxTMvdKDTR8xY\njnuU/XunKfMBJfboGxXkdHL7LJNuqE8DRG3El+TFGA/aCjFSLAKZyYGmD7H2FXr4\nVYxLh0cycwfaBLeKUC9Sg2A=\n-----END PRIVATE KEY-----\n', 'client_email': 'python-2@sys-67738525349545571962304921.iam.gserviceaccount.com', 'client_id': '116817448104344134353', 'auth_uri': 'https://accounts.google.com/o/oauth2/auth', 'token_uri': 'https://oauth2.googleapis.com/token', 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs', 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/python-2%40sys-67738525349545571962304921.iam.gserviceaccount.com', 'universe_domain': 'googleapis.com'}
credentials = service_account.Credentials.from_service_account_info(cred)


 # Auth das 2 ferramentas do GCP
def auth_bq():
    client_bq = bigquery.Client(project=project_id, credentials=credentials)

    return client_bq

def auth_gcs():
    client_gcs = storage.Client(project=project_id, credentials=credentials)

    return client_gcs


# Rotinas para receber o Json do webhook do Pipedrive e jogar para o Storage
# as respostas de todas as contas vão chegar no mesmo Bucket

#Funções exclusivas desse endpoint

def put_file_to_gcs(output_file: str, bucket_name: str, content):
    try:
        storage_client = auth_gcs()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_file)
        blob.upload_from_string(content)

        return 'OK'
    except Exception as ex:
        print(ex)
#def parse_json_raw(data):


# Listagem de Endpoints
@app.post("/webhook_pipedrive_all")
async def webhook(request: Request):
    print(request)
    # Recebe o corpo do pedido como bytes
    body = await request.body()

    # Converte os bytes para um objeto JSON
    payload = json.loads(body)

    # Cria um nome de arquivo único
    filename = f"webhook_pipedrive_{payload['meta']['object']}_{payload['meta']['company_id']}_{payload['meta']['timestamp']}.json"
    try:
        put_file_to_gcs(
            bucket_name="ng-raw",
            output_file="/pipedrive/" + filename,
            content=json.dumps(payload)
            )
        return {"Status": "OK", "Bucket_name": "ng_raw"}
    except Exception as ex:
        raise HTTPException(status_code=ex.code, detail=f"{ex}")





class Dataset(BaseModel):
    dataset_id: str
    table_id: str
    data: list[dict]


@app.post("/gas_to_bq")
def gas_to_bq(dataset: Dataset):
    # Configuração do cliente BigQuery
    client = bigquery.Client(project=project_id, credentials=credentials)

    # Preparação dos dados para carregamento no BigQuery

    df = pd.DataFrame(dataset.data)

    # Carregar os dados do DataFrame diretamente no BigQuery
    table_ref = f"{project_id}.{dataset.dataset_id}.{dataset.table_id}"
    load_config = bigquery.LoadJobConfig()
    #load_config.source_format = bigquery.SourceFormat.PARQUET
    load_config.autodetect = True
    load_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job = client.load_table_from_dataframe(df, table_ref, job_config=load_config)
    job.result()  # Wait for the loading job to complete

    return {"message": "Data successfully inserted into BigQuery"}

@app.post("/gas_to_bq_flatten")
def gas_to_bq(dataset: Dataset):
    # Configuração do cliente BigQuery
    client = bigquery.Client(project=project_id, credentials=credentials)

    dados_json = dataset.data

    def flatten_json(data, parent_key='', sep='_'):
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                flattened.update(flatten_json(value, new_key, sep))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{parent_key}{sep}" if parent_key else i
                flattened.update(flatten_json(item, new_key, sep))
        else:
            flattened[parent_key] = data
        return flattened


    # Compor dados do flatten para o carregar no BigQuery
    rows_to_insert = []
    for item in dados_json:
        flattened_item = flatten_json(item)
        rows_to_insert.append(flattened_item)

    #print(rows_to_insert)

    # Preparação dos dados para carregamento no BigQuery

    df = pd.DataFrame(rows_to_insert)

    # Carregar os dados do DataFrame diretamente no BigQuery
    table_ref = f"{project_id}.{dataset.dataset_id}.{dataset.table_id}"
    load_config = bigquery.LoadJobConfig()
    #load_config.source_format = bigquery.SourceFormat.PARQUET
    load_config.autodetect = True
    load_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job = client.load_table_from_dataframe(df, table_ref, job_config=load_config)
    job.result()  # Wait for the loading job to complete

    return {"message": "Data successfully inserted into BigQuery"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
