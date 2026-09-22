# AIOps Foundation — Guia do Instrutor: Colaboração e Remediação (Fechamento)

Este guia fecha o ciclo do Módulo 1 (Ingestão → Insights → Colaboração →
Remediação) usando o que já foi construído nos Módulos 3 e 4. Mesmo padrão
do guia anterior:

- 🖥️ = comando ou ação técnica (execute isso)
- 🎤 = fala sugerida, ligada diretamente ao comando/ação anterior
- 💬 = **momento sem nenhum comando** — só discussão com a turma

> Pré-requisito: o bucket `aiops-etl-lab-SEUNOME` e a tabela `logs_enriched`
> do lab de ETL (Módulos 3/4) já devem existir. Este lab não recria aquele
> pipeline, ele se conecta a ele.

> Custo: dentro do "Always Free" da AWS — Lambda (1M requisições/mês), SNS
> (1M publicações/mês), DynamoDB (25GB, sempre grátis) e CloudWatch (10
> métricas customizadas grátis). Para uma demo de aula, custo real é R$ 0,00.

---

## Arquivos deste diretório

```
scoring_lambda.py          → Lambda de "Insights": pontua o evento (crítico ou não)
remediation_lambda.py      → Lambda de "Remediação": age e registra o incidente
test_event_critical.json   → evento de teste que DEVE disparar notificação + remediação
test_event_normal.json     → evento de teste que NÃO deve disparar nada além da métrica
lambda_trust_policy.json   → trust policy padrão (permite Lambda assumir a role)
iam_scoring_policy.json    → permissões da role da Lambda de scoring
iam_remediation_policy.json → permissões da role da Lambda de remediação
```

Substitua `SEUNOME` pelo mesmo identificador usado no lab anterior, e
`ACCOUNT_ID` pelo ID da sua conta AWS:
```bash
aws sts get-caller-identity --query Account --output text
```

---

## 💬 Contexto (0-3 min) — sem comando

🎤 **Fala:** *"No Módulo 3, processamos dados em lote e consultamos com SQL.
No Módulo 4, treinamos um modelo que aprende a classificar eventos. Hoje
vamos ver o que falta: pegar essa classificação e transformar em ação — sem
um humano precisar estar olhando a tela o tempo todo. Isso fecha o ciclo que
vocês viram lá no Módulo 1: Ingestão, Insights, Colaboração, Remediação."*

---

# PARTE 1 — Preparar a infraestrutura de resposta

## Etapa 1 — Criar o tópico SNS e inscrever seu e-mail

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

## Etapa 2 — Criar a tabela DynamoDB

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

## Etapa 3 — Criar a role IAM da Lambda de scoring

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
> pelo ID real da sua conta (comando `aws sts get-caller-identity` lá em cima).

🎤 **Fala:** *"De novo o princípio de privilégio mínimo — essa role só pode
publicar no nosso tópico SNS e mandar métricas pro CloudWatch. Nada além
disso."*

## Etapa 4 — Publicar a Lambda de scoring

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

# PARTE 2 — Testar a detecção (antes de ligar a remediação)

## Etapa 5 — Invocar com um evento normal

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

## Etapa 6 — Invocar com um evento crítico

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

# PARTE 3 — Ligar a Remediação Automatizada

## Etapa 7 — Criar a role IAM da Lambda de remediação

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
> usar o bônus de fechar o ciclo no S3 (Etapa 11), confira o nome do bucket.

## Etapa 8 — Publicar a Lambda de remediação

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

## Etapa 9 — Inscrever a Lambda de remediação no tópico SNS

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

## Etapa 10 — Rodar o ciclo completo, ao vivo

🖥️ **Comando (o mesmo da Etapa 6 — mas agora o efeito é diferente):**
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

## 💬 (Opcional/bônus) Etapa 11 — Fechar o ciclo de volta ao S3

Se quiser ir além e mostrar o ciclo se retroalimentando:

🖥️ **Comando (redeploy da Lambda de remediação com a variável extra):**
```bash
aws lambda update-function-configuration \
  --function-name aiops-lab-remediation \
  --environment "Variables={DYNAMODB_TABLE=aiops_lab_incidents,CLOSE_LOOP_BUCKET=aiops-etl-lab-SEUNOME}"
```

Depois de rodar a Etapa 10 de novo, confira:
```bash
aws s3 ls s3://aiops-etl-lab-SEUNOME/raw/remediations/
```

🎤 **Fala:** *"Esse evento de remediação virou um novo dado bruto no nosso
Data Lake — se rodássemos o Crawler e o Glue Job de novo, ele entraria no
próximo ciclo de processamento. Isso é o `↻` do diagrama: o sistema
observando a própria ação."*

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

# ✅ Depois da aula — Cleanup

🖥️ **Comando:**
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

---

# 🧯 Troubleshooting

| Problema | Causa provável |
|---|---|
| E-mail do SNS nunca chega | A inscrição precisa ser confirmada pelo link enviado por e-mail (Etapa 1) antes de qualquer publicação funcionar |
| `aws lambda invoke` retorna erro de permissão | A role não tem a policy anexada corretamente, ou `ACCOUNT_ID`/ARN errado nos arquivos JSON |
| Lambda de remediação não dispara sozinha | Falta a Etapa 9 (subscribe + add-permission) — sem isso, o SNS não tem autorização para invocar a função |
| Item não aparece no DynamoDB | Confira se `DYNAMODB_TABLE` está correta na variável de ambiente da Lambda de remediação, e se a tabela existe (Etapa 2) |
| Métricas não aparecem no CloudWatch | Métricas customizadas podem levar 1-2 min para aparecer no console na primeira vez |
| Erro ao editar os `.json` de política | Lembre de trocar `ACCOUNT_ID` (e `SEUNOME` no bônus da Etapa 11) antes de rodar os comandos que os usam |

---

# 🧯 Plano B (se o SNS/e-mail estiver lento na hora da aula)

Pule direto para checar o **CloudWatch** e o **DynamoDB** — os dois
atualizam em segundos e não dependem de entrega de e-mail (que pode levar
alguns minutos dependendo do provedor). Use a chegada do e-mail como
"bônus" caso apareça durante a explicação seguinte, sem travar a aula
esperando por ele.
