"""
remediation_lambda.py — Lambda de "Remediação" (AIOps Foundation, fechamento do curso)

Disparada automaticamente pelo SNS quando a scoring_lambda publica um
evento crítico. Aqui fechamos a etapa de Remediação do Módulo 1: desde
"fornecer links para problemas relacionados" até "executar automação para
remediação totalmente automatizada" — nesta demo, simulamos a segunda
opção de forma segura (sem alterar infraestrutura real).

Fluxo:
  1. Recebe a mensagem do SNS (o JSON publicado pela scoring_lambda).
  2. Decide uma ação de remediação simulada, baseada no serviço afetado
     (mapa simples service -> ação — em produção, isso viria de um
     runbook/playbook real).
  3. Grava o incidente no DynamoDB (histórico de remediações, como um
     mini sistema de tickets).
  4. Publica métricas de tempo de remediação no CloudWatch — dá para medir
     MTTA/MTTR na prática (Módulo 5 e Módulo 7).
  5. (Opcional/bônus) Escreve um evento de volta no S3 (raw/remediations/),
     fechando o ciclo "Observar, Engajar, Agir" do Módulo 1 — controlado
     pela variável de ambiente CLOSE_LOOP_BUCKET (se não definida, esta
     etapa é pulada).

Variáveis de ambiente esperadas:
  DYNAMODB_TABLE      — nome da tabela DynamoDB de incidentes
  CLOSE_LOOP_BUCKET    — (opcional) bucket S3 para fechar o ciclo
"""

import json
import os
import time
import uuid
import boto3

dynamodb = boto3.resource("dynamodb")
cloudwatch = boto3.client("cloudwatch")
s3 = boto3.client("s3")

TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "aiops_lab_incidents")
CLOSE_LOOP_BUCKET = os.environ.get("CLOSE_LOOP_BUCKET")
NAMESPACE = "AIOpsLab"

# Mapa simplificado de "playbook" — em produção viria de uma base de
# conhecimento (ver Módulo 1: "Conhecimento, Automação e Colaboração")
REMEDIATION_PLAYBOOK = {
    "payment-gateway": "restart-payment-gateway-pods",
    "checkout-api": "clear-checkout-cache",
    "auth-service": "rotate-auth-connection-pool",
    "inventory-api": "restart-inventory-pods",
    "notification-worker": "restart-notification-worker",
    "search-api": "scale-up-search-api",
    "recommendation-engine": "restart-recommendation-engine",
}


def decide_remediation(service: str) -> str:
    return REMEDIATION_PLAYBOOK.get(service, "escalate-to-oncall")


def put_metric(name: str, value: float, unit: str = "Count") -> None:
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{"MetricName": name, "Value": value, "Unit": unit}],
    )


def close_the_loop(incident: dict) -> None:
    """Escreve o resultado da remediação de volta no S3 (raw/), simulando
    o sistema 'observando' sua própria ação como um novo evento de entrada."""
    if not CLOSE_LOOP_BUCKET:
        return
    try:
        key = f"raw/remediations/{incident['incident_id']}.json"
        s3.put_object(
            Bucket=CLOSE_LOOP_BUCKET,
            Key=key,
            Body=json.dumps(incident).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[INFO] Ciclo fechado: evento de remediação gravado em s3://{CLOSE_LOOP_BUCKET}/{key}")
    except Exception as e:
        # Não deixamos essa etapa opcional derrubar a remediação principal
        print(f"[WARN] Não foi possível fechar o ciclo no S3: {e}")


def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    processed = []

    for record in event.get("Records", []):
        sns_message = record["Sns"]["Message"]
        original_event = json.loads(sns_message)

        service = original_event.get("service", "unknown-service")
        detected_at = original_event.get("detected_at", time.time())
        remediated_at = time.time()
        time_to_remediate = round(remediated_at - detected_at, 3)

        action = decide_remediation(service)
        incident_id = str(uuid.uuid4())

        incident = {
            "incident_id": incident_id,
            "service": service,
            "level": original_event.get("level"),
            "message": original_event.get("message"),
            "severity_score": original_event.get("severity_score"),
            "trace_id": original_event.get("trace_id"),
            "detected_at": str(detected_at),
            "remediated_at": str(remediated_at),
            "time_to_remediate_seconds": str(time_to_remediate),
            "remediation_action": action,
            "status": "resolved",
        }

        table.put_item(Item=incident)

        put_metric("RemediationsExecuted", 1)
        put_metric("TimeToRemediateSeconds", time_to_remediate, unit="Seconds")

        close_the_loop(incident)

        print(f"[INFO] Incidente remediado: {json.dumps(incident)}")
        processed.append(incident)

    return {"processed": processed}
