"""
AWS Glue Job - ETL do Lab de Big Data (Módulo 3 - AIOps Foundation)

Este job implementa as 3 últimas etapas do pipeline de dados descrito no
Módulo 3 do curso (Extração já foi feita pelo upload em S3 + Crawler):

    Enriquecimento e Filtragem -> Limpeza e Integração -> Armazenamento

O que o job faz:
  1. Lê os dados brutos de 'logs' (JSON) e 'metrics' (CSV) via Glue Data Catalog
     (tabelas criadas pelo Crawler sobre s3://<bucket>/raw/).
  2. LIMPEZA: remove registros sem 'service' ou 'message'/timestamp inválido,
     remove duplicatas exatas.
  3. ENRIQUECIMENTO: calcula um 'severity_score' (0-100) combinando o nível do
     log e o tempo de resposta; adiciona a coluna 'ingestion_date'.
  4. ARMAZENAMENTO: grava o resultado em Parquet, particionado por
     'ingestion_date', em s3://<bucket>/processed/logs_enriched/.

Como rodar:
  Este script é feito para ser colado num AWS Glue Job (Glue Studio > Script
  editor) ou enviado via `aws glue create-job` apontando para este arquivo em
  S3. Não roda localmente sem o ambiente do Glue (usa as bibliotecas awsglue).

Parâmetros esperados (Job parameters no console do Glue):
  --RAW_BUCKET       nome do bucket onde estão os dados brutos (raw/)
  --PROCESSED_BUCKET nome do bucket de destino (processed/) - pode ser o mesmo
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --------------------------------------------------------------------------
# 1. Setup padrão de um Glue Job
# --------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "PROCESSED_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]

# --------------------------------------------------------------------------
# 2. EXTRAÇÃO (leitura) - direto do S3, sem depender do Catalog
#    (mais simples para fins didáticos; alternativa: glueContext.create_dynamic_frame.from_catalog)
# --------------------------------------------------------------------------
logs_path = f"s3://{RAW_BUCKET}/raw/logs/"
metrics_path = f"s3://{RAW_BUCKET}/raw/metrics/"

df_logs_raw = spark.read.json(logs_path)
df_metrics_raw = spark.read.option("header", "true").csv(metrics_path)

print(f"[INFO] Logs brutos lidos: {df_logs_raw.count()} registros")
print(f"[INFO] Metrics brutas lidas: {df_metrics_raw.count()} registros")

# --------------------------------------------------------------------------
# 3. LIMPEZA E INTEGRAÇÃO
# --------------------------------------------------------------------------

# 3.1 - Remove registros com campos essenciais ausentes (veracidade)
df_logs_clean = df_logs_raw.dropna(subset=["service", "message", "timestamp"])

# 3.2 - Valida formato de timestamp (ISO 8601). Descarta o que não bate.
df_logs_clean = df_logs_clean.withColumn(
    "timestamp_parsed", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")
).filter(F.col("timestamp_parsed").isNotNull())

# 3.3 - Remove duplicatas exatas (mesmo trace_id)
df_logs_clean = df_logs_clean.dropDuplicates(["trace_id"])

# 3.4 - Garante tipo numérico correto em response_time_ms
df_logs_clean = df_logs_clean.withColumn(
    "response_time_ms", F.col("response_time_ms").cast(DoubleType())
).dropna(subset=["response_time_ms"])

print(f"[INFO] Logs após limpeza: {df_logs_clean.count()} registros")

# --------------------------------------------------------------------------
# 4. ENRIQUECIMENTO E FILTRAGEM
# --------------------------------------------------------------------------

# 4.1 - severity_score: combina nível do log + tempo de resposta em um score 0-100
#       (ilustra "Correlação" do Módulo 1: juntar duas dimensões de dado em um insight)
level_weight = (
    F.when(F.col("level") == "ERROR", 70)
    .when(F.col("level") == "WARN", 35)
    .when(F.col("level") == "INFO", 5)
    .otherwise(0)
)

response_penalty = F.when(F.col("response_time_ms") > 1000, 30).otherwise(
    F.col("response_time_ms") / 1000 * 30
)

df_enriched = df_logs_clean.withColumn(
    "severity_score",
    F.least(F.lit(100), (level_weight + response_penalty)).cast("int"),
)

# 4.2 - coluna de partição para facilitar consultas no Athena
df_enriched = df_enriched.withColumn(
    "ingestion_date", F.date_format("timestamp_parsed", "yyyy-MM-dd")
)

# 4.3 - seleciona e renomeia colunas finais
df_final = df_enriched.select(
    F.col("timestamp_parsed").alias("event_timestamp"),
    "service",
    "level",
    "message",
    "response_time_ms",
    "severity_score",
    "trace_id",
    "ingestion_date",
)

# --------------------------------------------------------------------------
# 5. ARMAZENAMENTO - grava em Parquet particionado (otimizado para Athena)
# --------------------------------------------------------------------------
output_path = f"s3://{PROCESSED_BUCKET}/processed/logs_enriched/"

df_final.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)

print(f"[INFO] Escrita concluída em: {output_path}")
print(f"[INFO] Total de registros no dataset processado: {df_final.count()}")

job.commit()
