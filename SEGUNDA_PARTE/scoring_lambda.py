"""
scoring_lambda.py — Lambda de "Insights" (AIOps Foundation, fechamento do curso)

Aplica, em tempo real, a mesma lógica que o Módulo 4 treinou no notebook
(uma Árvore de Decisão aprendeu a prever `is_critical` a partir de `level`
e `response_time_ms`). Aqui, em vez de carregar o modelo serializado
(possível em produção, mas exige empacotar scikit-learn na Lambda — custo
de complexidade desnecessário para fins didáticos), replicamos a MESMA
regra que o Glue Job usou para gerar o rótulo (`severity_score`) — é
literalmente a fórmula que o modelo aprendeu a reproduzir no Experimento A.

Fluxo:
  1. Recebe um evento (log) via invocação direta (teste manual em aula) ou,
     em produção, via S3 Event Notification / API Gateway / Kinesis.
  2. Calcula severity_score (0-100) e is_critical (severity_score >= 80).
  3. Publica métricas no CloudWatch (namespace "AIOpsLab") — SEMPRE, mesmo
     para eventos não críticos, para termos visibilidade de volume total.
  4. Se crítico, publica uma mensagem no tópico SNS — dispara a etapa de
     Colaboração.

Variáveis de ambiente esperadas:
  SNS_TOPIC_ARN   — ARN do tópico SNS de alertas
"""

import json
import os
import time
import boto3

sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
NAMESPACE = "AIOpsLab"


def compute_severity(level: str, response_time_ms: float) -> int:
    """Mesma fórmula usada no Glue Job (etl_job.py) do Módulo 3 — a lógica
    que o modelo do Módulo 4 aprendeu a reproduzir a partir dos dados."""
    level_weight = {"ERROR": 70, "WARN": 35, "INFO": 5, "DEBUG": 0}.get(level, 0)
    response_penalty = 30 if response_time_ms > 1000 else (response_time_ms / 1000) * 30
    return min(100, int(level_weight + response_penalty))


def put_metric(name: str, value: float, service: str, unit: str = "Count") -> None:
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {
                "MetricName": name,
                "Value": value,
                "Unit": unit,
                "Dimensions": [{"Name": "Service", "Value": service}],
            }
        ],
    )


def lambda_handler(event, context):
    service = event.get("service", "unknown-service")
    level = event.get("level", "INFO")
    response_time_ms = float(event.get("response_time_ms", 0))
    message = event.get("message", "")
    trace_id = event.get("trace_id", "no-trace-id")

    severity_score = compute_severity(level, response_time_ms)
    is_critical = severity_score >= 80

    # Métricas: sempre publica volume processado e a severidade do evento
    put_metric("EventsProcessed", 1, service)
    put_metric("SeverityScore", severity_score, service, unit="None")

    result = {
        "service": service,
        "level": level,
        "response_time_ms": response_time_ms,
        "message": message,
        "trace_id": trace_id,
        "severity_score": severity_score,
        "is_critical": is_critical,
        "detected_at": time.time(),
        "action": "no_action",
    }

    if is_critical:
        put_metric("CriticalEventsDetected", 1, service)

        if not SNS_TOPIC_ARN:
            print("[WARN] SNS_TOPIC_ARN não configurado — evento crítico NÃO notificado.")
        else:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"[AIOps Lab] Evento crítico detectado - {service}",
                Message=json.dumps(result),
            )
            result["action"] = "notified_via_sns"

    print(f"[INFO] Evento processado: {json.dumps(result)}")
    return result
