# AIOps Foundation — Guia Completo do Instrutor (Curso Completo)

Este é o único documento que você precisa durante o curso inteiro. Ele
combina o **passo a passo técnico** (todo comando que você vai rodar, no
console ou no terminal) com a **fala sugerida** para cada etapa, cobrindo as
duas partes da demo:

- **PRIMEIRA_PARTE** — Módulos 3 e 4 (ETL / Big Data + Aprendizado Supervisionado)
- **SEGUNDA_PARTE** — Fechamento do curso (Colaboração + Remediação Automatizada)

**Como ler este guia:**
- 🖥️ = comando ou ação técnica (execute isso)
- 🎤 = fala sugerida, ligada diretamente ao comando/ação anterior
- 💬 = **momento sem nenhum comando** — é só discussão/explicação com a
  turma, sem nada para clicar ou digitar. Esses pontos estão marcados
  explicitamente para você não se perguntar "cadê o comando desse passo?"

> Custo estimado: dentro do Free Tier / "Always Free" na quase totalidade.
> Athena cobra ~$5/TB escaneado (sem free tier, mas frações de centavo com
> este dataset). Lambda, SNS, DynamoDB e CloudWatch ficam no "Always Free"
> para o volume de uma demo de aula. Custo real esperado: R$ 0,00.

---

## Estrutura do repositório

```
aiops-foundation-labs/
├── README.md                        → este arquivo (guia único)
├── PRIMEIRA_PARTE/                  → Módulos 3 e 4
│   ├── generate_data.py               script que gera os dados sintéticos
│   ├── logs.jsonl, metrics.csv, notes.txt   dados já gerados, prontos para upload
│   ├── etl_job.py                     script PySpark do Glue Job (Módulo 3)
│   ├── queries.sql                    queries de exemplo para o Athena (Módulo 3)
│   ├── athena_export_query.sql        query para exportar o CSV usado no Módulo 4
│   ├── supervised_learning_demo.ipynb notebook de aprendizado supervisionado (Módulo 4)
│   └── requirements.txt               dependências do notebook (se for rodar localmente)
└── SEGUNDA_PARTE/                    → Fechamento do curso
    ├── scoring_lambda.py               Lambda de "Insights": pontua o evento
    ├── remediation_lambda.py           Lambda de "Remediação": age e registra o incidente
    ├── test_event_critical.json        evento de teste que DEVE disparar notificação + remediação
    ├── test_event_normal.json          evento de teste que NÃO deve disparar nada além da métrica
    ├── lambda_trust_policy.json        trust policy padrão (permite Lambda assumir a role)
    ├── iam_scoring_policy.json         permissões da role da Lambda de scoring
    └── iam_remediation_policy.json     permissões da role da Lambda de remediação
```

**Convenção de diretório:** toda vez que este guia mostra um comando com um
nome de arquivo simples (ex: `generate_data.py`, `scoring_lambda.py`), ele
assume que você já está dentro da pasta certa (`PRIMEIRA_PARTE/` ou
`SEGUNDA_PARTE/`). Cada Parte abaixo começa com uma ação explícita de `cd`
para deixar isso claro.

Substitua `SEUNOME` por um identificador seu em todos os comandos (ex:
`aiops-etl-lab-renan`) — é o sufixo do nome do bucket S3, que precisa ser
globalmente único na AWS. Substitua `ACCOUNT_ID` pelo ID da sua conta:
```bash
aws sts get-caller-identity --query Account --output text
```

---

# 🗓️ Preparação geral (no dia anterior à aula, sem plateia)

Construir tudo do zero ao vivo consome tempo de aula sem ganho pedagógico
proporcional. Faça esta lista uma vez, com calma — na ordem, já que a
SEGUNDA_PARTE depende do bucket e da tabela criados na PRIMEIRA_PARTE.

**PRIMEIRA_PARTE (Módulos 3 e 4):**
- [ ] `cd PRIMEIRA_PARTE` e rodar `generate_data.py` (Etapa 0)
- [ ] Criar bucket S3 + estrutura de pastas (Etapa 1)
- [ ] Upload dos dados brutos (Etapa 2)
- [ ] Criar o Crawler + corrigir a permissão IAM (Etapa 3)
- [ ] Criar o Glue Job, mas **não rode ainda** — deixe pronto para clicar "Run" ao vivo (Etapa 4)
- [ ] Testar o Job uma vez, fora da aula (pode rodar de novo ao vivo sem problema — `mode("overwrite")` sobrescreve)
- [ ] Catalogar os dados processados como `logs_enriched` (Etapa 5)
- [ ] Configurar o "Query result location" do Athena com antecedência
- [ ] Rodar `athena_export_query.sql` uma vez e testar o notebook inteiro (Módulo 4)
- [ ] Deixar as abas do navegador já logadas e organizadas: S3, Glue, Athena, Colab

**SEGUNDA_PARTE (Colaboração e Remediação):**
- [ ] `cd SEGUNDA_PARTE` (a partir da raiz do repositório) e criar o tópico SNS + confirmar a inscrição do seu e-mail (Etapa 11)
- [ ] Criar a tabela DynamoDB (Etapa 12)
- [ ] Criar as duas roles IAM e publicar as duas Lambdas (Etapas 13, 14, 17, 18)
- [ ] Inscrever a Lambda de remediação no tópico SNS (Etapa 19)
- [ ] Rodar o ciclo completo uma vez, fora da aula, para garantir que tudo dispara certo (Etapa 20)
- [ ] **Fallback:** capture screenshots do e-mail recebido, do item aparecendo no DynamoDB, e das métricas no CloudWatch — para o caso de instabilidade ao vivo

---

# PARTE 1 — MÓDULO 3: ETL / Big Data (~30 min de aula)

🖥️ **Ação inicial:** a partir da raiz do repositório, `cd PRIMEIRA_PARTE`

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
Você ainda está em `PRIMEIRA_PARTE/`, nenhum `cd` novo é necessário.

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
remediação"* — gancho para o conceito de Remediação do Módulo 1, e é
exatamente isso que fechamos na Parte 3.

## 💬 Discussão final do notebook (2-3 min) — sem comando

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

# PARTE 3 — Colaboração e Remediação Automatizada (Fechamento, ~20-25 min)

🖥️ **Ação inicial:** a partir da raiz do repositório, `cd SEGUNDA_PARTE`
(se você ainda estiver dentro de `PRIMEIRA_PARTE/`, use `cd ../SEGUNDA_PARTE`)

> Pré-requisito: o bucket `aiops-etl-lab-SEUNOME` e a tabela `logs_enriched`
> da Parte 1 já precisam existir. Esta parte não recria aquele pipeline, ela
> se conecta a ele.

## 💬 Contexto (0-3 min) — sem comando

🎤 **Fala:** *"No Módulo 3, processamos dados em lote e consultamos com SQL.
No Módulo 4, treinamos um modelo que aprende a classificar eventos. Agora
vamos ver o que falta: pegar essa classificação e transformar em ação — sem
um humano precisar estar olhando a tela o tempo todo. Isso fecha o ciclo que
vocês viram lá no Módulo 1: Ingestão, Insights, Colaboração, Remediação."*

---

## Etapa 11 — Criar o tópico SNS e inscrever seu e-mail

🖥️ **Comando:**
```bash
aws sns create-topic --name aiops-lab-alerts
```
Anote o `TopicArn` retornado — vamos usar em vários passos seguintes.

🖥️ **Comando (inscreva seu e-mail para receber o alerta ao vivo em aula):**
```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:aiops-lab-alerts \
  --protocol email \
  --notification-endpoint seu-email@exemplo.com
```

⚠️ Depois desse comando, **confirme a inscrição no seu e-mail** (a AWS manda
um link de confirmação) — sem isso, o SNS não entrega a notificação.

🎤 **Fala:** *"O SNS é um sistema de publicação/assinatura: alguém publica
uma mensagem, e todo mundo inscrito recebe — e-mail, SMS, ou até outra
Lambda, como vamos ver daqui a pouco. Essa é a peça de Colaboração do
Módulo 1: o sistema avisando um humano, com contexto, em vez de deixar o
problema silencioso."*

## Etapa 12 — Criar a tabela DynamoDB

🖥️ **Comando:**
```bash
aws dynamodb create-table \
  --table-name aiops_lab_incidents \
  --attribute-definitions AttributeName=incident_id,AttributeType=S \
  --key-schema AttributeName=incident_id,KeyType=HASH \
  --billing-mode PROVISIONED \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```
(5/5 de capacidade fica bem dentro do free tier de 25/25 — e é mais do que
suficiente para uma demo de aula)

🎤 **Fala:** *"O DynamoDB vai funcionar como um mini sistema de tickets —
cada remediação automática vira um registro aqui, com o que foi detectado e
o que foi feito a respeito."*

## Etapa 13 — Criar a role IAM da Lambda de scoring

🖥️ **Comando:**
```bash
aws iam create-role \
  --role-name aiops-lab-scoring-role \
  --assume-role-policy-document file://lambda_trust_policy.json

aws iam attach-role-policy \
  --role-name aiops-lab-scoring-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy \
  --role-name aiops-lab-scoring-role \
  --policy-name aiops-lab-scoring-permissions \
  --policy-document file://iam_scoring_policy.json
```

> ⚠️ Antes de rodar, edite `iam_scoring_policy.json` e troque `ACCOUNT_ID`
> pelo ID real da sua conta.

🎤 **Fala:** *"De novo o princípio de privilégio mínimo — essa role só pode
publicar no nosso tópico SNS e mandar métricas pro CloudWatch. Nada além
disso."*

## Etapa 14 — Publicar a Lambda de scoring

🖥️ **Comando:**
```bash
zip scoring_lambda.zip scoring_lambda.py

aws lambda create-function \
  --function-name aiops-lab-scoring \
  --runtime python3.12 \
  --role arn:aws:iam::ACCOUNT_ID:role/aiops-lab-scoring-role \
  --handler scoring_lambda.lambda_handler \
  --zip-file fileb://scoring_lambda.zip \
  --timeout 10 \
  --environment "Variables={SNS_TOPIC_ARN=arn:aws:sns:us-east-1:ACCOUNT_ID:aiops-lab-alerts}"
```

🎤 **Fala:** *"Essa função replica a mesma fórmula que usamos no Glue Job do
Módulo 3 — que é exatamente a lógica que o modelo do Módulo 4 aprendeu a
reproduzir no Experimento A. Em produção, você carregaria o modelo
serializado; aqui replicamos a regra direto em código pra manter a Lambda
simples e sem dependências extras."*

## 💬 Enquanto isso... (opcional, se quiser aprofundar) — sem comando

🎤 **Fala (opcional):** *"Notem que essa Lambda não sabe nada sobre 'Big
Data' — ela processa um evento de cada vez, em milissegundos. É a diferença
entre o processamento em lote que fizemos no Athena (Módulo 3) e
processamento em tempo real. Os dois têm seu lugar: lote para análise
histórica e tendências, tempo real para reagir rápido."*

---

## Etapa 15 — Invocar com um evento normal

🖥️ **Comando:**
```bash
aws lambda invoke \
  --function-name aiops-lab-scoring \
  --payload file://test_event_normal.json \
  --cli-binary-format raw-in-base64-out \
  response_normal.json

cat response_normal.json
```

🎤 **Fala (antes de rodar):** *"Esse evento é um log `INFO` comum, com
tempo de resposta baixo. Esperamos que `is_critical` saia `false` e nada
mais aconteça além de uma métrica de volume."*

## Etapa 16 — Invocar com um evento crítico

🖥️ **Comando:**
```bash
aws lambda invoke \
  --function-name aiops-lab-scoring \
  --payload file://test_event_critical.json \
  --cli-binary-format raw-in-base64-out \
  response_critical.json

cat response_critical.json
```

🎤 **Fala (antes de rodar):** *"Esse é um `ERROR` com tempo de resposta
alto — deve dar `is_critical: true`. Se a inscrição de e-mail já foi
confirmada, o alerta deve chegar na sua caixa de entrada em segundos.
Vamos ver ao vivo."*

💬 **Enquanto o e-mail não chega (sem comando):** *"Enquanto aguardamos,
pensem: hoje esse alerta caiu só no meu e-mail. Daqui a pouco, em vez de um
humano precisar agir manualmente, vamos deixar outra função fazer isso
sozinha."*

---

## Etapa 17 — Criar a role IAM da Lambda de remediação

🖥️ **Comando:**
```bash
aws iam create-role \
  --role-name aiops-lab-remediation-role \
  --assume-role-policy-document file://lambda_trust_policy.json

aws iam attach-role-policy \
  --role-name aiops-lab-remediation-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy \
  --role-name aiops-lab-remediation-role \
  --policy-name aiops-lab-remediation-permissions \
  --policy-document file://iam_remediation_policy.json
```

> ⚠️ Edite `iam_remediation_policy.json` antes: troque `ACCOUNT_ID` e, se for
> usar o bônus de fechar o ciclo no S3 (Etapa 21), confira o nome do bucket.

## Etapa 18 — Publicar a Lambda de remediação

🖥️ **Comando:**
```bash
zip remediation_lambda.zip remediation_lambda.py

aws lambda create-function \
  --function-name aiops-lab-remediation \
  --runtime python3.12 \
  --role arn:aws:iam::ACCOUNT_ID:role/aiops-lab-remediation-role \
  --handler remediation_lambda.lambda_handler \
  --zip-file fileb://remediation_lambda.zip \
  --timeout 10 \
  --environment "Variables={DYNAMODB_TABLE=aiops_lab_incidents}"
```

🎤 **Fala:** *"Essa função tem um 'playbook' simples: um mapa de qual serviço
recebe qual ação de correção. Numa empresa real, isso viria de uma base de
conhecimento documentada — é literalmente o que o Módulo 1 chama de
'Conhecimento, Automação e Colaboração'."*

## Etapa 19 — Inscrever a Lambda de remediação no tópico SNS

🖥️ **Comando:**
```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:aiops-lab-alerts \
  --protocol lambda \
  --notification-endpoint arn:aws:lambda:us-east-1:ACCOUNT_ID:function:aiops-lab-remediation

aws lambda add-permission \
  --function-name aiops-lab-remediation \
  --statement-id sns-invoke-remediation \
  --action lambda:InvokeFunction \
  --principal sns.amazonaws.com \
  --source-arn arn:aws:sns:us-east-1:ACCOUNT_ID:aiops-lab-alerts
```

🎤 **Fala:** *"Isso conecta as duas pontas: agora, toda vez que a Lambda de
scoring publicar um evento crítico no SNS, a Lambda de remediação dispara
sozinha — sem ninguém clicar em nada."*

## Etapa 20 — Rodar o ciclo completo, ao vivo

🖥️ **Comando (o mesmo da Etapa 16 — mas agora o efeito é diferente):**
```bash
aws lambda invoke \
  --function-name aiops-lab-scoring \
  --payload file://test_event_critical.json \
  --cli-binary-format raw-in-base64-out \
  response_critical.json
```

🖥️ **Ação:** enquanto isso roda, abra em paralelo:
- O **DynamoDB Console** → tabela `aiops_lab_incidents` → **Explore table
  items** → deve aparecer um novo item em segundos
- O **CloudWatch Console** → **Metrics** → namespace `AIOpsLab` → métricas
  `EventsProcessed`, `CriticalEventsDetected`, `RemediationsExecuted`,
  `TimeToRemediateSeconds`

🎤 **Fala:** *"Olha o que aconteceu: um evento entrou, foi pontuado, virou
notificação, virou remediação automática, e ficou registrado — tudo em
segundos, sem intervenção humana. Isso é o ciclo Ingestão → Insights →
Colaboração → Remediação do Módulo 1, rodando de verdade."*

---

## 💬 (Opcional/bônus) Etapa 21 — Fechar o ciclo de volta ao S3

Se quiser ir além e mostrar o ciclo se retroalimentando:

🖥️ **Comando (redeploy da Lambda de remediação com a variável extra):**
```bash
aws lambda update-function-configuration \
  --function-name aiops-lab-remediation \
  --environment "Variables={DYNAMODB_TABLE=aiops_lab_incidents,CLOSE_LOOP_BUCKET=aiops-etl-lab-SEUNOME}"
```

Depois de rodar a Etapa 20 de novo, confira:
```bash
aws s3 ls s3://aiops-etl-lab-SEUNOME/raw/remediations/
```

🎤 **Fala:** *"Esse evento de remediação virou um novo dado bruto no nosso
Data Lake — se rodássemos o Crawler e o Glue Job de novo (voltando lá pra
PRIMEIRA_PARTE), ele entraria no próximo ciclo de processamento. Isso é o
`↻` do diagrama: o sistema observando a própria ação."*

---

## 💬 Fechamento do curso (5 min) — sem comando

🎤 **Fala:** *"Vamos conectar tudo: no Módulo 5 vocês aprenderam sobre MTTD e
MTTA — tempo médio para detectar e para reconhecer um incidente. Hoje, esse
tempo foi de segundos, do log chegando até a remediação registrada. No
Módulo 7, vocês viram que medir o impacto do AIOps é uma das maiores
dificuldades citadas pelo Gartner — mas aqui, o próprio CloudWatch já
guardou os números pra gente mostrar num dashboard."*

*"E a última pergunta, ligando com o Módulo 8: será que automatizar 100%
como fizemos aqui é sempre a decisão certa? O que vocês automatizariam, e o
que ainda manteriam com um humano no controle?"*

---

# ✅ Depois do curso — Cleanup completo

🖥️ **Comando (dentro de `PRIMEIRA_PARTE/`, se não for reusar o ambiente tão cedo):**
```bash
aws s3 rb s3://aiops-etl-lab-SEUNOME --force
aws glue delete-job --job-name aiops-etl-job
aws glue delete-crawler --name raw-data-crawler
aws glue delete-database --name aiops_lab_db
```

🖥️ **Comando (dentro de `SEGUNDA_PARTE/`):**
```bash
aws lambda delete-function --function-name aiops-lab-scoring
aws lambda delete-function --function-name aiops-lab-remediation
aws dynamodb delete-table --table-name aiops_lab_incidents
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:aiops-lab-alerts
aws iam delete-role-policy --role-name aiops-lab-scoring-role --policy-name aiops-lab-scoring-permissions
aws iam detach-role-policy --role-name aiops-lab-scoring-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name aiops-lab-scoring-role
aws iam delete-role-policy --role-name aiops-lab-remediation-role --policy-name aiops-lab-remediation-permissions
aws iam detach-role-policy --role-name aiops-lab-remediation-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name aiops-lab-remediation-role
```

Se for reaproveitar para outra turma na mesma semana, não precisa desmontar
nada — só rode o Glue Job e os testes das Lambdas de novo (tudo sobrescreve
sem criar duplicidade).

---

# 🧯 Troubleshooting

### Módulos 3 e 4 (PRIMEIRA_PARTE)

| Problema | Causa provável |
|---|---|
| Crawler não encontra nenhuma tabela | Path do S3 errado, ou pasta vazia |
| Glue Job falha com `403 / not authorized to perform s3:PutObject` | Role sem a policy inline da Etapa 4 — reveja o comando `put-role-policy` |
| Athena: `TABLE_NOT_FOUND: ... logs_enriched does not exist` | Tabela ainda não criada — rode `SHOW TABLES IN aiops_lab_db;` e confira, ou repita a Etapa 6 |
| Athena retorna 0 linhas | Esqueceu do `MSCK REPAIR TABLE logs_enriched;` após o job rodar |
| Job demora muito (>10 min) | Normal no cold start dos workers Spark; considere `Python Shell` como Job Type para algo mais leve |
| Notebook: `KeyError: 'is_critical'` | Alguma célula anterior não rodou — use "Executar tudo" (Runtime → Run all) em vez de rodar célula avulsa |
| Notebook: CSV não encontrado | No Colab, o upload manual some ao reiniciar a sessão — suba o CSV de novo |

### Colaboração e Remediação (SEGUNDA_PARTE)

| Problema | Causa provável |
|---|---|
| E-mail do SNS nunca chega | A inscrição precisa ser confirmada pelo link enviado por e-mail (Etapa 11) antes de qualquer publicação funcionar |
| `aws lambda invoke` retorna erro de permissão | A role não tem a policy anexada corretamente, ou `ACCOUNT_ID`/ARN errado nos arquivos JSON |
| Lambda de remediação não dispara sozinha | Falta a Etapa 19 (subscribe + add-permission) — sem isso, o SNS não tem autorização para invocar a função |
| Item não aparece no DynamoDB | Confira se `DYNAMODB_TABLE` está correta na variável de ambiente da Lambda de remediação, e se a tabela existe (Etapa 12) |
| Métricas não aparecem no CloudWatch | Métricas customizadas podem levar 1-2 min para aparecer no console na primeira vez |
| Erro ao editar os `.json` de política | Lembre de trocar `ACCOUNT_ID` (e `SEUNOME` no bônus da Etapa 21) antes de rodar os comandos que os usam |

---

# 🧯 Plano B geral (internet/AWS instável em sala)

1. Use os screenshots pré-capturados na preparação
2. Tenha uma versão do notebook já executada (com outputs salvos) aberta
   localmente como fallback
3. Na Parte 1/2, priorize a Query 5 do Athena (raw vs. processado) e os
   Experimentos A/B do notebook — os dois momentos de maior valor
   pedagógico ali
4. Na Parte 3, se o e-mail do SNS estiver lento, pule direto para checar o
   **CloudWatch** e o **DynamoDB** — os dois atualizam em segundos e não
   dependem de entrega de e-mail; use a chegada do e-mail como "bônus" caso
   apareça durante a explicação seguinte, sem travar a aula esperando por ele
5. Se o tempo apertar de verdade, o resto pode ser resumido verbalmente
