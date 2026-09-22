-- ============================================================================
-- Queries de exemplo - Lab de ETL/Big Data (Módulo 3 - AIOps Foundation)
-- Rode no Athena depois de criar a tabela 'logs_enriched' via Crawler
-- apontando para s3://<PROCESSED_BUCKET>/processed/logs_enriched/
-- ============================================================================

-- 0. Conferir se a tabela foi catalogada corretamente
SELECT * FROM logs_enriched LIMIT 10;


-- 1. INSIGHTS -> Descoberta: quais serviços mais geram erro?
-- Conecta com o conceito de "Descoberta" do Módulo 1 (encontrar padrões nos dados)
SELECT
    service,
    COUNT(*) AS error_count
FROM logs_enriched
WHERE level = 'ERROR'
GROUP BY service
ORDER BY error_count DESC;


-- 2. INSIGHTS -> Correlação: severidade média por serviço
-- Mostra o valor do "severity_score" criado na etapa de enriquecimento
SELECT
    service,
    ROUND(AVG(severity_score), 1) AS avg_severity,
    ROUND(AVG(response_time_ms), 1) AS avg_response_time_ms,
    COUNT(*) AS total_events
FROM logs_enriched
GROUP BY service
ORDER BY avg_severity DESC;


-- 3. VARIEDADE -> distribuição de níveis de log (mostra a mistura de tipos de evento)
SELECT
    level,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM logs_enriched
GROUP BY level
ORDER BY total DESC;


-- 4. Casos de uso do Módulo 6 -> "Redução de Ruído de Alerta"
-- Simula priorização: só os eventos mais críticos (severity_score alto) deveriam virar alerta
SELECT
    event_timestamp,
    service,
    message,
    severity_score
FROM logs_enriched
WHERE severity_score >= 80
ORDER BY severity_score DESC
LIMIT 20;


-- 5. VELOCIDADE / VOLUME -> volume de eventos por dia (partição usada no ETL)
SELECT
    ingestion_date,
    COUNT(*) AS total_events
FROM logs_enriched
GROUP BY ingestion_date
ORDER BY ingestion_date;


-- 6. Comparativo "antes x depois" (rode isso numa tabela apontando para raw/logs/
--    criada por um segundo Crawler, para comparar volume bruto x volume limpo)
-- SELECT COUNT(*) AS total_raw FROM raw_logs;
-- SELECT COUNT(*) AS total_processed FROM logs_enriched;
-- A diferença entre as duas contagens = exatamente os registros descartados
-- na etapa de Limpeza e Integração (ótimo gancho visual para a aula).
