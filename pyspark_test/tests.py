import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import TimestampType

# Função para criar SparkSession antes de cada teste
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()

# Usar a fixture spark_session automaticamente em todos os testes
@pytest.mark.usefixtures("spark_session")
class TestMySparkCode:
    def test_load_parquet(self, spark_session):
        df = spark_session.read.parquet("pyspark_test/OriginaisNetflix - Python.parquet")
        assert df is not None
        assert df.count() > 0

    # Teste para transformar campos em datetime e ordenar
    def test_transform_and_sort(self, spark_session):
        df = spark_session.read.parquet("pyspark_test/OriginaisNetflix - Python.parquet")
        df = df.withColumn("Premiere", col("Premiere").cast(TimestampType()))
        df = df.withColumn("dt_inclusao", col("dt_inclusao").cast(TimestampType()))
        df = df.orderBy(col("Active").desc(), col("Genre").desc())

        # Verificar se os campos foram convertidos corretamente para datetime
        assert df.schema["Premiere"].dataType == TimestampType()
        assert df.schema["dt_inclusao"].dataType == TimestampType()

        # Verificar se o DataFrame foi ordenado corretamente
        assert df.select("Active", "Genre").collect() == df.orderBy(col("Active").desc(), col("Genre").desc()).select("Active", "Genre").collect()

    # Teste para remover linhas duplicadas e substituir valores
    def test_remove_duplicates_and_replace(self, spark_session):
        df = spark_session.read.parquet("pyspark_test/OriginaisNetflix - Python.parquet")
        df = df.dropDuplicates()
        df = df.withColumn("Seasons", when(col("Seasons") == "TBA", "a ser anunciado").otherwise(col("Seasons")))

        # Verificar se não existem linhas duplicadas
        assert df.count() == df.dropDuplicates().count()

        # Verificar se a substituição de valores foi feita corretamente
        assert df.filter(col("Seasons") == "TBA").count() == 0
        assert df.filter(col("Seasons") == "a ser anunciado").count() > 0

    # Teste para verificar se a coluna "Data de Alteração" foi criada corretamente
    def test_create_timestamp_column(self, spark_session):
        df = spark_session.read.parquet("pyspark_test/OriginaisNetflix - Python.parquet")
        df = df.withColumn("Data de Alteração", col("dt_inclusao").cast(TimestampType()))

        # Verificar se a coluna "Data de Alteração" foi criada corretamente
        assert "Data de Alteração" in df.columns
        assert df.schema["Data de Alteração"].dataType == TimestampType()

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    pytest.main([__file__])