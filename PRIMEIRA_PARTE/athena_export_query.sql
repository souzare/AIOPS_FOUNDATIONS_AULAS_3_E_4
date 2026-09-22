-- ============================================================================
-- Query de exportação - roda no Athena, depois clique em "Download results"
-- (ícone de download no canto superior direito do resultado) para gerar o CSV
-- que será usado no notebook do Módulo 4 (supervised_learning_demo.ipynb).
-- ============================================================================

SELECT
    event_timestamp,
    service,
    level,
    response_time_ms,
    severity_score
FROM logs_enriched
ORDER BY event_timestamp;

-- Salve o CSV baixado como: logs_enriched_export.csv (na raiz deste diretório)
-- (esse nome já é o esperado pelo notebook)
