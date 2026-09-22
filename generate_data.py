"""
Gerador de dados sintéticos para o Lab de ETL / Big Data - Módulo 3 (AIOps Foundation)

Este script gera 3 tipos de dados propositalmente diferentes, simulando os
domínios de dados descritos no Módulo 3 (Telemetria de Aplicações, Telemetria
de Infraestrutura e conceitos de MELT - Métricas, Eventos, Logs, Traces):

1. logs.jsonl   -> Logs de aplicação (semi-estruturado, JSON Lines)
2. metrics.csv  -> Métricas de infraestrutura (estruturado, CSV)
3. notes.txt    -> Notas de operadores em incidentes (não estruturado, texto livre)

Os dados são gerados com "sujeira" proposital (campos ausentes, timestamps
quebrados, duplicatas) para ilustrar o conceito de VERACIDADE (um dos 5 Vs do
Big Data) e justificar a etapa de Limpeza e Integração do pipeline de dados.

Uso:
    python generate_data.py --n-logs 800 --n-metrics 500 --n-notes 60 --dirty-rate 0.12

Saída (por padrão, em ../sample-data/):
    logs.jsonl
    metrics.csv
    notes.txt
"""

import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

SERVICES = [
    "checkout-api",
    "payment-gateway",
    "auth-service",
    "inventory-api",
    "notification-worker",
    "search-api",
    "recommendation-engine",
]

LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

MESSAGES_BY_LEVEL = {
    "INFO": [
        "request processed successfully",
        "cache hit for key",
        "health check passed",
        "user session created",
    ],
    "WARN": [
        "response time above threshold",
        "retrying downstream call",
        "cache miss, falling back to database",
        "connection pool near capacity",
    ],
    "ERROR": [
        "timeout connecting to payment-gateway",
        "unhandled exception in request handler",
        "database connection refused",
        "downstream service returned 500",
        "circuit breaker open",
    ],
    "DEBUG": [
        "entering function process_order",
        "payload validated",
        "feature flag evaluated",
    ],
}

INCIDENT_NOTE_TEMPLATES = [
    "Time notou aumento de latencia no {service} por volta das {time}, "
    "investigando possivel causa relacionada a deploy recente.",
    "Cliente reportou erro ao finalizar compra no {service}. "
    "Reproduzido em ambiente de staging, aguardando fix do time de dev.",
    "Alerta de {service} disparado, mas nao encontramos impacto real ao usuario "
    "- possivel falso positivo, ajustar threshold.",
    "Incidente resolvido apos restart do {service}. "
    "Causa raiz ainda nao identificada, abrir post-mortem.",
    "Pico de erros no {service} coincide com job de batch as {time}, "
    "provavel contencao de recursos.",
]


def random_timestamp(days_back: int = 3) -> datetime:
    start = datetime.utcnow() - timedelta(days=days_back)
    delta_seconds = random.randint(0, days_back * 24 * 3600)
    return start + timedelta(seconds=delta_seconds)


def maybe_break_timestamp(ts: datetime, dirty: bool) -> str:
    """Retorna timestamp ISO 8601 válido, ou uma string quebrada se dirty=True."""
    if not dirty:
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    broken_formats = [
        ts.strftime("%d/%m/%Y %H:%M"),   # formato incorreto
        "",                              # vazio
        "not-a-timestamp",               # texto inválido
        str(int(ts.timestamp())),        # epoch cru sem indicação
    ]
    return random.choice(broken_formats)


def generate_logs(n: int, dirty_rate: float) -> list[dict]:
    records = []
    for _ in range(n):
        is_dirty = random.random() < dirty_rate
        level = random.choices(LEVELS, weights=[55, 20, 15, 10])[0]
        ts = random_timestamp()
        service = random.choice(SERVICES)

        record = {
            "timestamp": maybe_break_timestamp(ts, is_dirty and random.random() < 0.5),
            "service": service,
            "level": level,
            "message": random.choice(MESSAGES_BY_LEVEL[level]),
            "response_time_ms": round(random.uniform(15, 250), 1)
            if level != "ERROR"
            else round(random.uniform(800, 6000), 1),
            "trace_id": str(uuid.uuid4()),
        }

        # Sujeira: campo obrigatório ausente
        if is_dirty and random.random() < 0.5:
            missing_field = random.choice(["message", "response_time_ms", "service"])
            record[missing_field] = None

        records.append(record)

    # Duplicatas propositais (simula reenvio de agente/duplicação em pipeline)
    n_dupes = max(1, int(n * 0.04))
    records.extend(random.sample(records, n_dupes))
    random.shuffle(records)
    return records


def generate_metrics(n: int, dirty_rate: float) -> list[dict]:
    records = []
    for _ in range(n):
        is_dirty = random.random() < dirty_rate
        ts = random_timestamp()
        service = random.choice(SERVICES)
        cpu = round(random.uniform(5, 95), 2)
        mem = round(random.uniform(10, 90), 2)
        error_rate = round(random.uniform(0, 2), 3) if random.random() > 0.1 else round(
            random.uniform(5, 25), 3
        )  # picos ocasionais

        records.append(
            {
                "timestamp": maybe_break_timestamp(ts, is_dirty),
                "service": service,
                "cpu_usage_pct": cpu if not (is_dirty and random.random() < 0.3) else "",
                "memory_usage_pct": mem,
                "error_rate_pct": error_rate,
            }
        )
    return records


def generate_notes(n: int) -> list[str]:
    notes = []
    for _ in range(n):
        template = random.choice(INCIDENT_NOTE_TEMPLATES)
        note = template.format(
            service=random.choice(SERVICES),
            time=f"{random.randint(0,23):02d}:{random.randint(0,59):02d}",
        )
        notes.append(note)
    return notes


def write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_csv(path: Path, records: list[dict]) -> None:
    if not records:
        return
    fieldnames = list(records[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def write_notes(path: Path, notes: list[str]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for i, note in enumerate(notes, start=1):
            f.write(f"[NOTA #{i}] {note}\n")


def main():
    parser = argparse.ArgumentParser(description="Gera dados sintéticos para o lab de ETL/Big Data")
    parser.add_argument("--n-logs", type=int, default=800)
    parser.add_argument("--n-metrics", type=int, default=500)
    parser.add_argument("--n-notes", type=int, default=60)
    parser.add_argument("--dirty-rate", type=float, default=0.12, help="proporção de registros 'sujos' (0-1)")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="pasta de saída (por padrão, o próprio diretório onde este script está)",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)

    out_dir = Path(__file__).parent / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    logs = generate_logs(args.n_logs, args.dirty_rate)
    metrics = generate_metrics(args.n_metrics, args.dirty_rate)
    notes = generate_notes(args.n_notes)

    write_jsonl(out_dir / "logs.jsonl", logs)
    write_csv(out_dir / "metrics.csv", metrics)
    write_notes(out_dir / "notes.txt", notes)

    print(f"OK - gerado em: {out_dir.resolve()}")
    print(f"  logs.jsonl   -> {len(logs)} registros (semi-estruturado)")
    print(f"  metrics.csv  -> {len(metrics)} registros (estruturado)")
    print(f"  notes.txt    -> {len(notes)} registros (não estruturado)")
    print(f"  taxa de sujeira aplicada: {args.dirty_rate:.0%}")


if __name__ == "__main__":
    main()
