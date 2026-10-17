-- Avaliação do modelo de degradação de pneu (faltava no repo desde a parte 1).


-- 1) Métricas de regressão no conjunto de avaliação reservado no treino
SELECT
  ROUND(mean_absolute_error, 3)      AS mae_s,
  ROUND(SQRT(mean_squared_error), 3) AS rmse_s,
  ROUND(median_absolute_error, 3)    AS mediana_erro_s,
  ROUND(r2_score, 4)                 AS r2,
  ROUND(explained_variance, 4)       AS variancia_explicada
FROM ML.EVALUATE(MODEL `pdm-davi-2026.f1_gold.modelo_degradacao_pneu`);

-- 2) Erro por composto (segmentos, slide 39: "analisar segmentos")
SELECT composto,
       COUNT(*)                                            AS voltas,
       ROUND(AVG(ABS(predicted_alvo_duracao_s - alvo_duracao_s)), 3) AS mae_s
FROM ML.PREDICT(MODEL `pdm-davi-2026.f1_gold.modelo_degradacao_pneu`,
                (SELECT * FROM `pdm-davi-2026.f1_gold.gold_degradacao_pneu`))
GROUP BY composto
ORDER BY voltas DESC;

-- 3) Histórico de treino (épocas/iterações e perda)
SELECT * FROM ML.TRAINING_INFO(MODEL `pdm-davi-2026.f1_gold.modelo_degradacao_pneu`);
