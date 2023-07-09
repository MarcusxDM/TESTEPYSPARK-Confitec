from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import TimestampType
import boto3
import credentials as cred
import os

def find_csv(folder_path):
    """
    Retorna o path do primeiro arquivo csv encontrado no folder_path,
    caso não encontre, retorna None
    """
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                return os.path.join(root, file)
    return None

def file_to_s3(file_name, bucket, object_name):
    """
    Envia arquivo especificado em file_name, para o bucket e object especificados
    """
    s3_client = boto3.client('s3', 
                      aws_access_key_id=cred.access_key_id, 
                      aws_secret_access_key=cred.secret_access_key, 
                      region_name=cred.region_name
                      )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(e)
        return False
    return True

def main():
    # Iniciar a sessão do Spark
    spark = SparkSession.builder.getOrCreate()

    # Carregar o arquivo Parquet
    df = spark.read.parquet("pyspark_test/OriginaisNetflix - Python.parquet")

    # Quantidade de linhas
    num_rows = df.count()

    # 1. Transformar campos em datetime (timestamp?)
    df = df.withColumn("Premiere", to_timestamp(col("Premiere")))
    df = df.withColumn("dt_inclusao", to_timestamp(col("dt_inclusao")))

    # 2. Ordenar por ativos e genero de forma decrescente
    df = df.orderBy(col("Active").desc(), col("Genre").desc())

    # 3. Remover linhas duplicadas e substituir "TBA" por "a ser anunciado"
    df = df.dropDuplicates()
    df = df.withColumn("Seasons", when(col("Seasons") == "TBA", "a ser anunciado").otherwise(col("Seasons")))

    # 4. Criar coluna "Data de Alteração" com timestamp, é utilizado a data de inclusão como default
    df = df.withColumn("Data de Alteração", to_timestamp(col("dt_inclusao")).cast(TimestampType()))

    # 5. Trocar os nomes das colunas de inglês para português
    df = df.withColumnRenamed("Title", "Título") \
        .withColumnRenamed("Genre", "Gênero") \
        .withColumnRenamed("Seasons", "Temporadas") \
        .withColumnRenamed("Premiere", "Estréia") \
        .withColumnRenamed("Language", "Idioma") \
        .withColumnRenamed("Active", "Ativo") \
        .withColumnRenamed("Status", "Status")

    # 6. Verificar erros de processamento
    num_rows_distinct = df.count()
    if num_rows_distinct == 0:
        print("Algum erro ocorreu no processamento")
    else:
        print(num_rows - num_rows_distinct, 'linhas estavam duplicadas')

        # Selecionar colunas para export
        df = df.select("Título", "Gênero", "Temporadas", "Estréia", "Idioma", "Ativo", "Status", "dt_inclusao", "Data de Alteração")
        
        # 7. Salvar os dados em formato CSV
        csv_output_path = "pyspark_test/output"
        df.write.csv(csv_output_path, header=True, sep=";", mode="overwrite")

        # 8. Enviar o arquivo CSV para um bucket do AWS S3
        csv_file_path = find_csv(csv_output_path)
        if csv_file_path:
            if file_to_s3(csv_file_path, 'bucket', 'object_name'):
                print("Arquivo enviado")
            else:
                print("Não foi possível enviar o arquivo")
        else:
            print("Nenhum arquivo CSV encontrado")


if __name__ == "__main__":
    main()
