# 🔍 Deteção de Anomalias em Dados Cripto Multi-Exchange

> **Processamento de Big Data — Tema 5**  
> UC: Processamento de Big Data | Docente: Pedro Sobreiro | Ano Letivo 2025/2026  
> **Grupo:** Bruno Proença · Ana Simões · João Resina

---

## 📌 Sobre o Projeto

Este projeto desenvolve um **pipeline distribuído de deteção de anomalias** em dados reais de preços de Bitcoin (BTC/USDT), analisando simultaneamente três exchanges — **Binance**, **Kucoin** e **Coinbase**.

O sistema deteta eventos anómalos como *flash crashes* (colapso de preço de 15–20% numa exchange sem reflexo nas outras), picos de volume anómalos (volume 10× acima do normal) e divergências de preço inter-exchange que sugerem arbitragem ou manipulação de mercado.

Todo o processamento é **100% distribuído com Apache Spark (PySpark)** — sem nunca transferir o dataset completo para a memória de um único nó.

---

## 🏗️ Arquitetura — Medallion

```
Bronze  →  Parquet brutos extraídos via ccxt (dados originais, imutáveis)
Silver  →  Alinhamento temporal + feature engineering com Window Functions
Gold    →  Scores de anomalia + flags de deteção (particionado por anomaly_any)
```

---

## 📊 Resultados Obtidos

| Métrica | Valor |
|---|---|
| Linhas analisadas (após join) | 104 953 |
| Período | Jan 2021 → 2024 (barras de 15 min) |
| Flags Z-score (\|z\| > 3.5) | 4 032 (3,84%) |
| Flags BisectingKMeans (Cluster 7) | 601 (0,57%) |
| Flags combinadas (`anomaly_any`) | 4 040 (3,85%) |
| Silhouette Score | 0,2377 |

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Versão | Função |
|---|---|---|
| Apache Spark (PySpark) | 3.5.0 | Motor de processamento distribuído |
| Python | 3.11 | Linguagem principal |
| ccxt | última | Extração de dados das exchanges via API pública |
| pyspark.ml | 3.5.0 | BisectingKMeans, VectorAssembler, StandardScaler, ClusteringEvaluator |
| pandas | — | Amostragem para visualizações (`.limit(2000)` apenas) |
| matplotlib | — | Visualizações |
| Docker + JupyterLab | — | Ambiente reproduzível |
| Parquet | — | Formato de armazenamento (Bronze, Silver, Gold) |

---

## 🐳 Como Correr — Docker (Recomendado)

O ambiente foi construído a partir do ficheiro `.yml` disponibilizado pelo professor no repositório oficial da UC:  
🔗 [https://github.com/pesobreiro/big_data](https://github.com/pesobreiro/big_data)

Isto garante que as versões de todas as dependências são exactamente as mesmas usadas nas aulas.

### Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e em execução

### Passos

```bash
# 1. Clonar este repositório
git clone https://github.com/brunoproenca03/TP_Anomalias
cd TP_Anomalias

# 2. Arrancar o contentor com JupyterLab
docker compose up

# 3. Abrir o browser em:
#    http://localhost:8888
```

Não é necessário instalar nada manualmente — o contentor já tem o PySpark, Python, e todas as bibliotecas prontas a usar.

---

## 📁 Estrutura do Repositório

```
TP_Anomalias/
│
├── Tema5_DetacaoAnomalias.ipynb   # Notebook principal — pipeline completo
├── Tb.ipynb                        # Extração Bronze via ccxt
├── docker-compose.yml              # Configuração do ambiente Docker
│
├── data/
│   ├── binance_btc.parquet         # Camada Bronze — Binance
│   ├── kucoin_btc.parquet          # Camada Bronze — Kucoin
│   ├── coinbase_btc.parquet        # Camada Bronze — Coinbase
│   ├── silver/
│   │   └── btc_aligned.parquet     # Camada Silver — dados alinhados + features
│   └── gold/
│       └── btc_anomaly_scored.parquet  # Camada Gold — scores + flags
│
└── charts/
    ├── 01_price_anomalies.png      # Preço tri-exchange com anomalias
    └── 02_score_distributions.png  # Distribuição dos scores
```

---

## 🔬 Metodologia — Resumo Técnico

### Extração (Bronze)
Dados OHLCV reais extraídos com `ccxt` via API pública das três exchanges. Mais de 105 000 barras de 15 minutos por exchange, desde Janeiro de 2021. Guardados em formato Parquet.

### Alinhamento (Silver — Fase 1)
Inner join temporal em `timestamp`: só ficam os instantes presentes nas três exchanges em simultâneo. Resultado: **104 978 linhas × 16 colunas**.

### Feature Engineering (Silver — Fase 2)
Window functions distribuídas (`partitionBy → orderBy → rowsBetween`) para calcular por exchange: retornos barra-a-barra, SMA-7, SMA-30, volatilidade (7 barras), rácio de volume, price range. Features inter-exchange: `gap_binance_kucoin`, `gap_binance_coinbase`, `gap_kucoin_coinbase`, `max_cross_gap`. DataFrame final: **104 953 linhas × 41 colunas**.

### Deteção — Método A: Z-score Distribuído
μ e σ calculados numa única passagem de agregação Spark. Escalares embebidos via `F.lit()` — sem `.collect()`. Flag activa quando `zscore_max > 3.5` → **4 032 anomalias**.

### Deteção — Método B: BisectingKMeans (pyspark.ml)
Pipeline `VectorAssembler → StandardScaler → BisectingKMeans (K=8)`, 100% distribuído, sem `.toPandas()`. Clusters com ≤ 2% da população são classificados como anómalos. Cluster 7 (601 linhas, 0,57%) foi o único cluster anómalo identificado.

### Avaliação
`ClusteringEvaluator` (pyspark.ml.evaluation) com Silhouette Score = **0,2377** — clusters razoáveis, com alguma separação entre comportamento normal e anómalo.

### Flag Combinada
`anomaly_any = 1` quando Z-score OR KMeans sinaliza. O KMeans detetou **8 anomalias** que o Z-score isolado teria falhado.

---

## 📚 Referências (APA 7.ª edição)

Apache Software Foundation. (2024). *PySpark API reference (v. 3.5)*. https://spark.apache.org/docs/latest/api/python/

Mendelevitch, O., Stella, C., & Eadline, D. (2017). *Practical data science with Hadoop and Spark*. Addison-Wesley.

Santos, M. Y., & Costa, C. (2019). *Big data: Concepts, warehousing, and analytics*. FCA.

Sobreiro, P. (2025). *big_data* [Repositório GitHub]. https://github.com/pesobreiro/big_data

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65. https://doi.org/10.1145/2934664

Zhang, T., Ramakrishnan, R., & Livny, M. (1996). BIRCH: An efficient data clustering method for very large databases. *ACM SIGMOD Record, 25*(2), 103–114. https://doi.org/10.1145/235968.233324

---

<div align="center">
  <sub>Processamento de Big Data — ISLA Santarém — 2025/2026</sub>
</div>
