# AIOps Foundation — Guia Completo do Instrutor (Módulo 3 + Módulo 4)

Este é o único documento que você precisa durante a aula. Ele combina o
**passo a passo técnico** (todo comando que você vai rodar, no console ou no
terminal) com a **fala sugerida** para cada etapa, na ordem em que você vai
executar.

**Como ler este guia:**
- 🖥️ = comando ou ação técnica (execute isso)
- 🎤 = fala sugerida, ligada diretamente ao comando/ação anterior
- 💬 = **momento sem nenhum comando** — é só discussão/explicação com a
  turma, sem nada para clicar ou digitar. Esses pontos estão marcados
  explicitamente para você não se perguntar "cadê o comando desse passo?"

> Custo estimado: dentro do Free Tier na quase totalidade. Athena cobra
> ~$5/TB escaneado (sem free tier), mas com este dataset (poucos MB) o custo
> fica em frações de centavo.

---

## Arquivos deste diretório

```
generate_data.py               → script que gera os dados sintéticos (rode antes da aula)
logs.jsonl, metrics.csv, notes.txt   → dados já gerados, prontos para upload
etl_job.py                     → script PySpark do Glue Job (Módulo 3)
queries.sql                    → queries de exemplo para o Athena (Módulo 3)
athena_export_query.sql        → query para exportar o CSV usado no Módulo 4
supervised_learning_demo.ipynb → notebook de aprendizado supervisionado (Módulo 4)
requirements.txt               → dependências do notebook (se for rodar localmente)
```

Substitua `SEUNOME` por um identificador seu em todos os comandos abaixo
(ex: `aiops-etl-lab-renan`) — é o sufixo do nome do bucket S3, que precisa
ser globalmente único na AWS.

---

# 🗓️ Preparação (no dia anterior à aula, sem plateia)

Construir tudo do zero ao vivo consome tempo de aula sem ganho pedagógico
proporcional. Faça esta lista uma vez, com calma:

- [ ] Rodar `generate_data.py` (Etapa 0 abaixo)
- [ ] Criar bucket S3 + estrutura de pastas (Etapa 1)
- [ ] Upload dos dados brutos (Etapa 2)
- [ ] Criar o Crawler + corrigir a permissão IAM (Etapa 3)
- [ ] Criar o Glue Job, mas **não rode ainda** — deixe pronto para clicar "Run" ao vivo (Etapa 4)
- [ ] Testar o Job uma vez, fora da aula, para garantir que funciona (pode rodar de novo ao vivo sem problema — `mode("overwrite")` sobrescreve)
- [ ] Catalogar os dados processados como `logs_enriched` (Etapa 5)
- [ ] Configurar o "Query result location" do Athena com antecedência
- [ ] Rodar `athena_export_query.sql` uma vez e testar o notebook inteiro (Módulo 4), garantindo que roda sem erro
- [ ] Deixar as abas do navegador já logadas e organizadas: S3, Glue (Crawlers e Jobs), Athena, e o notebook (Colab)
- [ ] **Fallback:** capture screenshots dos resultados-chave (contagem raw vs. processed, output do notebook) para o caso de instabilidade de rede/AWS ao vivo

---

# PARTE 1 — MÓDULO 3: ETL / Big Data (~30 min de aula)

## 💬 Abertura (0-5 min) — sem comando

Antes de abrir qualquer console, pergunte à turma: **"o que faz um conjunto
de dados ser 'Big Data', e não só 'muitos dados'?"**

Puxe a definição do manual: é sobre técnicas de processamento tradicionais
ficarem ineficazes — não é só volume. Introduza os **5 Vs** (Volume,
Velocidade, Variedade, Veracidade, Valor) e avise que cada um vai aparecer
na demo, na prática.

---

## Etapa 0 — Gerar os dados sintéticos

🖥️ **Comando:**
```bash
python3 generate_data.py --n-logs 800 --n-metrics 500 --n-notes 60 --dirty-rate 0.12
```
Isso gera `logs.jsonl` (JSON, semi-estruturado), `metrics.csv` (CSV,
estruturado) e `notes.txt` (texto livre, não estruturado) — com ~12% de
registros propositalmente "sujos" (campos nulos, timestamps quebrados,
duplicatas).

🎤 **Fala:** *"Eu já gerei esses dados antes da aula — mas o script está aqui
e vocês podem rodar em casa com outros parâmetros, tipo mais volume ou mais
'sujeira'."*

## Etapa 1 — Mostrar os 3 tipos de dado (5-10 min)

🖥️ **Ação:** abra os 3 arquivos lado a lado (`logs.jsonl`, `metrics.csv`,
`notes.txt`) em qualquer editor de texto.

🎤 **Fala:** *"Isso é exatamente a diferenciação Estruturado / Semi-estruturado
/ Não estruturado do manual — e, na vida real, um sistema AIOps recebe os
três tipos ao mesmo tempo. Isso é o que chamamos de **Variedade**."*

🖥️ **Ação:** encontre e mostre ao vivo um registro de `logs.jsonl` com campo
`null` ou timestamp quebrado (procure por `"service": null` ou por uma linha
com data em formato estranho).

🎤 **Fala:** *"Isso é ruído de dado real — sistemas de produção têm isso o
tempo todo. Chamamos isso de baixa **Veracidade**, e é por isso que a etapa
de Limpeza do pipeline existe."*

## Etapa 2 — Criar o bucket S3 e a estrutura de pastas

🖥️ **Comando:**
```bash
aws s3 mb s3://aiops-etl-lab-SEUNOME --region us-east-1
```

⚠️ O S3 não tem pastas de verdade — "pastas" são só prefixos no nome dos
arquivos. Criar o bucket não cria `raw/`, `raw/logs/` etc. automaticamente.

🖥️ **Comando (cria a estrutura de pastas explicitamente):**
```bash
aws s3api put-object --bucket aiops-etl-lab-SEUNOME --key raw/logs/
aws s3api put-object --bucket aiops-etl-lab-SEUNOME --key raw/metrics/
aws s3api put-object --bucket aiops-etl-lab-SEUNOME --key raw/notes/
aws s3api put-object --bucket aiops-etl-lab-SEUNOME --key processed/
```
(Alternativa: pule esses 4 comandos — o upload da Etapa 3 já cria o caminho
sozinho ao apontar para `raw/logs/logs.jsonl`, por exemplo.)

🎤 **Fala:** *"Diferente de pastas no seu computador, no S3 isso é só um nome
de arquivo com barras — não existe uma 'pasta' real por trás."*

## Etapa 3 — Upload dos dados brutos

🖥️ **Comando:**
```bash
aws s3 cp logs.jsonl    s3://aiops-etl-lab-SEUNOME/raw/logs/
aws s3 cp metrics.csv   s3://aiops-etl-lab-SEUNOME/raw/metrics/
aws s3 cp notes.txt     s3://aiops-etl-lab-SEUNOME/raw/notes/
```

🎤 **Fala:** *(nenhuma nova — já foi dita na Etapa 1)*

## Etapa 4 — Criar o Crawler e corrigir a permissão IAM

🖥️ **Console:** AWS Glue → Crawlers → Create crawler
- Nome: `raw-data-crawler`
- Data source: S3, path `s3://aiops-etl-lab-SEUNOME/raw/`
- IAM Role: crie uma nova (ex: `AWSGlueServiceRole-raw-data-crawler`)
- Destino: Database novo, ex: `aiops_lab_db`
- Rode o crawler (~1-2 min)

🎤 **Fala:** *"O Crawler varre o S3 e infere o schema automaticamente — isso é
o Data Catalog: metadados sobre o dado, não o dado em si."*

⚠️ **Correção de permissão obrigatória** — a role que o Glue sugere usa a
política gerenciada `AWSGlueServiceRole`, que só libera S3 para buckets com
`aws-glue` no nome. Como nosso bucket tem outro nome, é preciso liberar
manualmente:

🖥️ **Comando:**
```bash
cat > lab-s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowLabBucketAccess",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::aiops-etl-lab-SEUNOME/*"
    },
    {
      "Sid": "AllowLabBucketListing",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::aiops-etl-lab-SEUNOME"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name AWSGlueServiceRole-raw-data-crawler \
  --policy-name aiops-lab-s3-access \
  --policy-document file://lab-s3-policy.json
```

🎤 **Fala:** *(esse é um passo de infraestrutura — se estiver com o ambiente
pré-preparado do dia anterior, pode pular a explicação detalhada e só
mencionar: "aqui eu já liberei a permissão da role de acesso ao bucket".)*

## Etapa 5 — Rodar o Glue Job (ETL)

🖥️ **Console:** AWS Glue → Jobs → Create job → Spark script editor
1. Cole o conteúdo de `etl_job.py`
2. Job parameters:
   - `--RAW_BUCKET` = `aiops-etl-lab-SEUNOME`
   - `--PROCESSED_BUCKET` = `aiops-etl-lab-SEUNOME`
3. IAM Role: a mesma role da Etapa 4 (já com a policy do bucket)
4. Salve e clique em **Run**

🎤 **Fala (antes de clicar Run):** *"Esse job faz as 3 últimas etapas do
pipeline de dados do Módulo 3: Enriquecimento e Filtragem, Limpeza e
Integração, e Armazenamento. A Extração já aconteceu — foi o upload que
fizemos."*

## 💬 Enquanto o job roda (2-5 min) — sem comando

Abra `etl_job.py` no editor e percorra as 3 seções comentadas no próprio
script, lendo em voz alta os comentários de cada bloco:

- **Limpeza e Integração** (`dropna`, validação de timestamp, `dropDuplicates`)
- **Enriquecimento** (cálculo do `severity_score` — conecte com "Correlação"
  do Módulo 1: juntar duas dimensões do dado em um novo insight)
- **Armazenamento** (por que Parquet em vez de manter JSON/CSV: formato
  colunar, mais eficiente para consulta analítica — e o Athena cobra por
  dado escaneado, então formato importa para o bolso)

## Etapa 6 — Catalogar os dados processados

🖥️ **Comando (SQL, rodar no Athena):**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS aiops_lab_db.logs_enriched (
  event_timestamp timestamp,
  service string,
  level string,
  message string,
  response_time_ms double,
  severity_score int,
  trace_id string
)
PARTITIONED BY (ingestion_date string)
STORED AS PARQUET
LOCATION 's3://aiops-etl-lab-SEUNOME/processed/logs_enriched/';

MSCK REPAIR TABLE logs_enriched;
```

🎤 **Fala:** *"Essa DDL é literalmente o que um Crawler faz por trás dos
panos ao inspecionar o Parquet — estou mostrando na mão pra vocês verem o
que tem 'dentro' do Data Catalog."*

## Etapa 7 — Consultar no Athena

🖥️ **Console:** Athena → selecione o database `aiops_lab_db` → configure o
"Query result location" (se ainda não fez) → rode as queries de
`queries.sql`, uma por vez:

**Query 0 — conferência:**
```sql
SELECT * FROM logs_enriched LIMIT 10;
```
🎤 **Fala:** *"Vamos confirmar que os dados chegaram certinho."*

**Query 1 — erros por serviço:**
```sql
SELECT service, COUNT(*) AS error_count
FROM logs_enriched
WHERE level = 'ERROR'
GROUP BY service
ORDER BY error_count DESC;
```
🎤 **Fala:** *"Isso é 'Descoberta' do Módulo 1 — encontrar padrões nos
dados."*

**Query 2 — severidade média por serviço:**
```sql
SELECT service, ROUND(AVG(severity_score), 1) AS avg_severity,
       ROUND(AVG(response_time_ms), 1) AS avg_response_time_ms,
       COUNT(*) AS total_events
FROM logs_enriched
GROUP BY service
ORDER BY avg_severity DESC;
```
🎤 **Fala:** *"Aqui vemos o valor do `severity_score` que criamos no
Enriquecimento — combinamos duas colunas brutas num indicador novo."*

**Query 3 — distribuição de níveis de log:**
```sql
SELECT level, COUNT(*) AS total,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM logs_enriched
GROUP BY level
ORDER BY total DESC;
```
🎤 **Fala:** *"Isso mostra a mistura de tipos de evento — nossa
**Variedade** dentro de um único dataset já processado."*

**Query 4 — priorização de alertas:**
```sql
SELECT event_timestamp, service, message, severity_score
FROM logs_enriched
WHERE severity_score >= 80
ORDER BY severity_score DESC
LIMIT 20;
```
🎤 **Fala:** *"Imagina que só esses eventos virassem alerta para o time de
operações — é exatamente esse o valor de negócio de 'Redução de Ruído de
Alerta' do Módulo 6."*

**Query 5 — comparação raw vs. processado (o clímax da demo):**
```sql
-- Se você catalogou também os dados brutos (raw_logs), rode:
-- SELECT COUNT(*) AS total_raw FROM raw_logs;
SELECT COUNT(*) AS total_processed FROM logs_enriched;
```
🎤 **Fala:** *"A diferença entre o total bruto e o total processado é
exatamente o que a etapa de Limpeza descartou — a materialização visual do
conceito de **Veracidade**."*

## 💬 Fechamento do Bloco 1 (2 min) — sem comando

🎤 **Fala:** *"Hoje, quem decidiu o que é 'severo' fui eu, escrevendo uma
fórmula fixa no código Spark. E se, em vez disso, um modelo aprendesse
sozinho, a partir do histórico, o que é anômalo? Isso é exatamente o que
vamos ver no Módulo 4."*

---

# PARTE 2 — MÓDULO 4: Aprendizado Supervisionado (~15-20 min de aula)

Pode acontecer em outro dia — só reforce a conexão com o Módulo 3 no início.

## Etapa 8 — Exportar os dados do Athena

🖥️ **Comando (SQL, rodar no Athena):**
```sql
SELECT event_timestamp, service, level, response_time_ms, severity_score
FROM logs_enriched
ORDER BY event_timestamp;
```
(está também em `athena_export_query.sql`)

🖥️ **Ação:** clique no ícone de download no resultado → salve como
`logs_enriched_export.csv`.

🎤 **Fala:** *"Esse `severity_score` que vocês viram no Módulo 3 foi
calculado por uma fórmula que eu escrevi à mão. Hoje vamos ver se um modelo
aprende essa mesma lógica sozinho."*

## Etapa 9 — Abrir o notebook

🖥️ **Ação (Google Colab):**
1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. File → Upload notebook → `supervised_learning_demo.ipynb`
3. Ícone de pasta (barra lateral) → upload → `logs_enriched_export.csv`

🎤 **Fala:** *(nenhuma nova aqui — a explicação já foi dada na Etapa 8)*

## Etapa 10 — Rodar célula por célula (Shift + Enter)

🖥️ **Ação:** rode a célula que cria o rótulo:
```python
df["is_critical"] = (df["severity_score"] >= 80).astype(int)
```
🎤 **Fala:** *"Isso é o dado rotulado — o 'gabarito' que o aprendizado
supervisionado precisa para aprender."*

🖥️ **Ação:** rode o **Experimento A** (todas as features).

🎤 **Fala:** *"A acurácia deve sair bem alta, acima de 95%. Pergunta pra
vocês: isso é o modelo sendo bom, ou fomos nós que demos cola?"* — deixe a
turma perceber que o rótulo veio das próprias features usadas para treinar.

🖥️ **Ação:** rode o **Experimento B** (sem `response_time_ms`).

🎤 **Fala:** *"A acurácia cai. Isso conecta direto com o Módulo 3: a
qualidade dos dados de entrada limita o modelo — e com o Módulo 8: 'não
dedique tempo a ML até ter dados sólidos disponíveis'."*

🖥️ **Ação:** rode a célula que imprime/plota a árvore de decisão.

🎤 **Fala:** *"Por que usar um modelo interpretável em AIOps? Porque o time
de operações precisa confiar na decisão antes de automatizar uma
remediação"* — gancho para o conceito de Remediação do Módulo 1.

## 💬 Discussão final (2-3 min) — sem comando

As perguntas já estão no final do notebook. Deixe a turma discutir em grupo
por 2-3 min antes de você responder:

1. O que aconteceria com desbalanceamento de classes (poucos `ERROR` vs.
   muitos `INFO`)?
2. Quem definiria "crítico" numa empresa real, e como isso afeta viés no
   modelo (Módulo 8 — Viés em Machine Learning)?
3. Como isso mudaria se fosse **Regressão** (prever `severity_score`
   contínuo) em vez de Classificação?
4. Que pergunta sobre esse mesmo dataset só um modelo **não supervisionado**
   (Clustering) responderia?

---

# ✅ Depois da aula

🖥️ **Comando (cleanup, se não for reusar o ambiente tão cedo):**
```bash
aws s3 rb s3://aiops-etl-lab-SEUNOME --force
aws glue delete-job --job-name aiops-etl-job
aws glue delete-crawler --name raw-data-crawler
aws glue delete-database --name aiops_lab_db
```
Se for reaproveitar para outra turma na mesma semana, não precisa desmontar
— só rode o Glue Job de novo (ele sobrescreve os dados).

---

# 🧯 Troubleshooting

| Problema | Causa provável |
|---|---|
| Crawler não encontra nenhuma tabela | Path do S3 errado, ou pasta vazia |
| Glue Job falha com `403 / not authorized to perform s3:PutObject` | Role sem a policy inline da Etapa 4 — reveja o comando `put-role-policy` |
| Athena: `TABLE_NOT_FOUND: ... logs_enriched does not exist` | Tabela ainda não criada — rode `SHOW TABLES IN aiops_lab_db;` e confira, ou repita a Etapa 6 |
| Athena retorna 0 linhas | Esqueceu do `MSCK REPAIR TABLE logs_enriched;` após o job rodar |
| Job demora muito (>10 min) | Normal no cold start dos workers Spark; considere `Python Shell` como Job Type para algo mais leve |
| Notebook: `KeyError: 'is_critical'` | Alguma célula anterior não rodou — use "Executar tudo" (Runtime → Run all) em vez de rodar célula avulsa |
| Notebook: CSV não encontrado | No Colab, o upload manual some ao reiniciar a sessão — suba o CSV de novo |

---

# 🧯 Plano B geral (internet/AWS instável em sala)

1. Use os screenshots pré-capturados na preparação
2. Tenha uma versão do notebook já executada (com outputs salvos) aberta
   localmente como fallback
3. Priorize a Query 5 do Athena (raw vs. processado) e os Experimentos A/B
   do notebook — são os dois momentos de maior valor pedagógico; o resto
   pode ser resumido verbalmente se o tempo apertar
