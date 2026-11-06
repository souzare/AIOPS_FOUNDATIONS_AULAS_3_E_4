-- ============================================================================
-- queries_arvore.sql — "Vendo no banco o que a árvore de decisão descobriu"
-- Módulo 4 (Aprendizado Supervisionado) — roda no Athena, tabela logs_enriched
--
-- Ideia: a árvore do notebook faz perguntas e mede o quanto cada pergunta
-- "separa" eventos críticos de não críticos. Aqui fazemos a mesma conta em SQL,
-- para os alunos verem que NÃO há mágica: é contagem e divisão.
--
-- Definições usadas em todas as queries:
--   crítico     = severity_score >= 80   (o rótulo, igual ao notebook)
--   p           = proporção de eventos críticos dentro de um grupo
--   gini        = 1 - (p*p + (1-p)*(1-p))   (0 = grupo puro, 0,5 = metade/metade)
--
-- Observação: o notebook treina com 75% dos dados (543 eventos) e estas queries
-- usam a tabela inteira (725 eventos), então números como o Gini da raiz podem
-- diferir na 3ª casa decimal (0,272 no treino x ~0,270 aqui). A conclusão é a mesma.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Q1. A RAIZ: antes de qualquer pergunta, todos os eventos estão misturados.
--     É o ponto de partida da árvore (a caixa de cima do gráfico).
-- ----------------------------------------------------------------------------
WITH base AS (
    SELECT CAST(CASE WHEN severity_score >= 80 THEN 1 ELSE 0 END AS DOUBLE) AS is_critical
    FROM logs_enriched
)
SELECT
    COUNT(*)                                        AS total_eventos,
    CAST(SUM(is_critical) AS INTEGER)               AS criticos,
    COUNT(*) - CAST(SUM(is_critical) AS INTEGER)    AS nao_criticos,
    ROUND(AVG(is_critical) * 100, 1)                AS pct_criticos,
    ROUND(1 - (AVG(is_critical) * AVG(is_critical)
             + (1 - AVG(is_critical)) * (1 - AVG(is_critical))), 3) AS gini_raiz
FROM base;


-- ----------------------------------------------------------------------------
-- Q2. PERGUNTA DO EXPERIMENTO A: "o nível do log é ERROR?"
--     Cada linha é um ramo da árvore. Veja o gini de cada grupo: ERROR = 100%
--     críticos e os demais = 0% críticos. Os dois grupos ficam PUROS (gini 0),
--     por isso a árvore para depois de uma única pergunta.
-- ----------------------------------------------------------------------------
WITH base AS (
    SELECT
        level,
        severity_score,
        CAST(CASE WHEN severity_score >= 80 THEN 1 ELSE 0 END AS DOUBLE) AS is_critical
    FROM logs_enriched
),
grupos AS (
    SELECT
        level,
        COUNT(*)            AS total,
        SUM(is_critical)    AS criticos,
        AVG(is_critical)    AS p,
        MIN(severity_score) AS score_min,
        MAX(severity_score) AS score_max
    FROM base
    GROUP BY level
)
SELECT
    level,
    total,
    CAST(criticos AS INTEGER)                       AS criticos,
    ROUND(p * 100, 1)                               AS pct_criticos,
    ROUND(1 - (p * p + (1 - p) * (1 - p)), 3)       AS gini,
    score_min,
    score_max,
    CASE WHEN p > 0.5 THEN 'critico' ELSE 'nao_critico' END AS previsao_da_folha
FROM grupos
ORDER BY pct_criticos DESC;


-- ----------------------------------------------------------------------------
-- Q3. OUTRA PERGUNTA QUE TAMBÉM SEPARA: "o tempo de resposta passa de 500 ms?"
--     Mostra por que tirar só `level` NÃO derrubava a acurácia: o tempo de
--     resposta carrega o mesmo sinal (erros: 800-6000 ms; demais: até ~250 ms).
--     Não há nenhum evento entre os dois blocos, então o corte em 500 é perfeito.
-- ----------------------------------------------------------------------------
WITH base AS (
    SELECT
        CASE WHEN response_time_ms > 500 THEN 'sim (> 500 ms)' ELSE 'nao (<= 500 ms)' END AS resposta,
        response_time_ms,
        CAST(CASE WHEN severity_score >= 80 THEN 1 ELSE 0 END AS DOUBLE) AS is_critical
    FROM logs_enriched
),
grupos AS (
    SELECT
        resposta,
        COUNT(*)              AS total,
        SUM(is_critical)      AS criticos,
        AVG(is_critical)      AS p,
        MIN(response_time_ms) AS tempo_min_ms,
        MAX(response_time_ms) AS tempo_max_ms
    FROM base
    GROUP BY resposta
)
SELECT
    resposta,
    total,
    CAST(criticos AS INTEGER)                  AS criticos,
    ROUND(p * 100, 1)                          AS pct_criticos,
    ROUND(1 - (p * p + (1 - p) * (1 - p)), 3) AS gini,
    ROUND(tempo_min_ms, 1)                     AS tempo_min_ms,
    ROUND(tempo_max_ms, 1)                     AS tempo_max_ms
FROM grupos
ORDER BY resposta;


-- ----------------------------------------------------------------------------
-- Q4. EXPERIMENTO B: sem level e sem tempo de resposta, só sobra `service`.
--     Nenhum serviço passa de 50% de críticos, então a "previsão da folha" de
--     TODOS os serviços é "nao_critico". Por isso o modelo B não acha nada.
-- ----------------------------------------------------------------------------
WITH base AS (
    SELECT
        service,
        CAST(CASE WHEN severity_score >= 80 THEN 1 ELSE 0 END AS DOUBLE) AS is_critical
    FROM logs_enriched
),
grupos AS (
    SELECT service, COUNT(*) AS total, SUM(is_critical) AS criticos, AVG(is_critical) AS p
    FROM base
    GROUP BY service
)
SELECT
    service,
    total,
    CAST(criticos AS INTEGER)                  AS criticos,
    ROUND(p * 100, 1)                          AS pct_criticos,
    ROUND(1 - (p * p + (1 - p) * (1 - p)), 3) AS gini,
    CASE WHEN p > 0.5 THEN 'critico' ELSE 'nao_critico' END AS previsao_da_folha
FROM grupos
ORDER BY pct_criticos DESC;


-- ----------------------------------------------------------------------------
-- Q5. O CORAÇÃO DO ALGORITMO: qual pergunta a árvore escolhe?
--     Para cada pergunta candidata calculamos o GINI PONDERADO depois da divisão
--     (média do gini dos dois grupos, pesada pelo tamanho de cada um).
--     MENOR gini ponderado = pergunta que melhor separa = a que a árvore escolhe.
--     Compare com o gini da raiz (Q1): "ganho" = quanto a pergunta reduziu a mistura.
-- ----------------------------------------------------------------------------
WITH base AS (
    SELECT
        level,
        service,
        response_time_ms,
        CAST(CASE WHEN severity_score >= 80 THEN 1 ELSE 0 END AS DOUBLE) AS is_critical
    FROM logs_enriched
),
perguntas AS (
    SELECT 'level = ERROR'          AS pergunta,
           CASE WHEN level = 'ERROR' THEN 'sim' ELSE 'nao' END AS resposta, is_critical FROM base
    UNION ALL
    SELECT 'response_time_ms > 500',
           CASE WHEN response_time_ms > 500 THEN 'sim' ELSE 'nao' END, is_critical FROM base
    UNION ALL
    SELECT 'level = WARN',
           CASE WHEN level = 'WARN' THEN 'sim' ELSE 'nao' END, is_critical FROM base
    UNION ALL
    SELECT 'service = checkout-api',
           CASE WHEN service = 'checkout-api' THEN 'sim' ELSE 'nao' END, is_critical FROM base
    UNION ALL
    SELECT 'service = search-api',
           CASE WHEN service = 'search-api' THEN 'sim' ELSE 'nao' END, is_critical FROM base
),
grupos AS (
    SELECT pergunta, resposta, COUNT(*) AS n, AVG(is_critical) AS p
    FROM perguntas
    GROUP BY pergunta, resposta
),
raiz AS (
    SELECT 1 - (AVG(is_critical) * AVG(is_critical)
              + (1 - AVG(is_critical)) * (1 - AVG(is_critical))) AS gini_raiz
    FROM base
)
SELECT
    g.pergunta,
    ROUND(SUM(g.n * (1 - (g.p * g.p + (1 - g.p) * (1 - g.p)))) / SUM(g.n), 3) AS gini_ponderado,
    ROUND(MAX(r.gini_raiz)
        - SUM(g.n * (1 - (g.p * g.p + (1 - g.p) * (1 - g.p)))) / SUM(g.n), 3) AS ganho
FROM grupos g
CROSS JOIN raiz r
GROUP BY g.pergunta
ORDER BY gini_ponderado, g.pergunta;


-- ----------------------------------------------------------------------------
-- Q6. A "MATRIZ DE CONFUSÃO" EM SQL: a regra aprendida acerta quanto?
--     Regra do Experimento A: "se level = ERROR então crítico, senão não".
--     Compare a previsão da regra com o valor real (severity_score >= 80).
--     Resultado esperado: só linhas em que previsão = real (zero erros).
-- ----------------------------------------------------------------------------
SELECT
    CASE WHEN level = 'ERROR'        THEN 'critico' ELSE 'nao_critico' END AS previsao_da_arvore_A,
    CASE WHEN severity_score >= 80   THEN 'critico' ELSE 'nao_critico' END AS valor_real,
    COUNT(*) AS total
FROM logs_enriched
GROUP BY 1, 2
ORDER BY 1, 2;


-- ----------------------------------------------------------------------------
-- Q7. A MESMA CONTA PARA O EXPERIMENTO B: o modelo que só vê `service`
--     responde "nao_critico" para tudo (como vimos na Q4). Veja o prejuízo:
--     todos os eventos críticos viram "falso negativo" (previsto não crítico,
--     mas era crítico), que é exatamente o recall = 0 do notebook.
-- ----------------------------------------------------------------------------
SELECT
    'nao_critico'                                                         AS previsao_da_arvore_B,
    CASE WHEN severity_score >= 80 THEN 'critico' ELSE 'nao_critico' END  AS valor_real,
    COUNT(*)                                                              AS total
FROM logs_enriched
GROUP BY 2
ORDER BY 2;
