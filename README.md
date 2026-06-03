# SELIC Pipeline — beAnalytic

Pipeline de dados orquestrado com Apache Airflow consumindo a API pública do Banco Central do Brasil (BCB). Implementa arquitetura Medallion (Bronze → Silver → Gold) para taxa SELIC diária (2020–2024).

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────┐
│                        Apache Airflow DAG                        │
│                         selic_pipeline                           │
│                                                                  │
│  ┌─────────────────┐      ┌──────────────────┐      ┌─────────┐ │
│  │  bronze_ingest  │ ───► │ silver_transform  │ ───► │  gold   │ │
│  │                 │      │                  │      │aggregate│ │
│  │  BCB API → raw  │      │ tipos + limpeza  │      │métricas │ │
│  │  Parquet        │      │ + colunas deriv. │      │mensais/ │ │
│  └─────────────────┘      └──────────────────┘      │anuais   │ │
│                                                      └─────────┘ │
└──────────────────────────────────────────────────────────────────┘
         │                         │                       │
         ▼                         ▼                       ▼
data/bronze/              data/silver/              data/gold/
selic_raw.parquet         selic_trusted.parquet     selic_mensal.parquet
                                                    selic_anual.parquet
```

### Camadas

| Camada | Arquivo | Descrição |
|--------|---------|-----------|
| **Bronze** | `selic_raw.parquet` | Dados brutos da API — tipos string, sem transformação |
| **Silver** | `selic_trusted.parquet` | `data` como `datetime64`, `valor` como `float64`, colunas derivadas (`ano`, `mes`, `ano_mes`, `dia_semana`) |
| **Gold** | `selic_mensal.parquet` | Média diária, taxa acumulada mensal (composta), variação em pp vs mês anterior |
| **Gold** | `selic_anual.parquet` | Média diária anual, taxa acumulada anual (composta) |

---

## Decisões Técnicas

### Taxa composta vs média aritmética

A taxa acumulada usa **composição**: `(∏(1 + r_i/100) − 1) × 100`

É a fórmula correta para SELIC — cada dia rende sobre o saldo atualizado, não sobre o principal. A média aritmética seria incorreta para períodos com mais de um dia.

### `schedule_interval=None` (trigger manual)

O dataset é histórico e estático (2020–2024). Um schedule diário geraria runs inúteis. A DAG foi projetada para executar sob demanda — em produção, seria parametrizada para buscar janelas de data configuráveis.

### Módulos em `dags/` com `PYTHONPATH`

Os módulos `bronze/`, `silver/`, `gold/` vivem dentro de `dags/` e são importáveis via `PYTHONPATH=/opt/airflow/dags`. Alternativa seria usar `plugins/`, mas isso exigiria restart do Airflow a cada mudança — `dags/` é recarregado automaticamente pelo scheduler.

### Qualidade de dados entre camadas

Cada camada tem um quality gate que falha o task com `ValueError` antes de persistir dados inválidos. Sem dependência de frameworks externos (Great Expectations seria overengineering para o escopo atual).

| Camada | Gate |
|--------|------|
| Bronze | Row count ≥ 1.000 |
| Silver | `valor` ∈ [0, 0,2] · anos ∈ [2020, 2024] |
| Gold | Dias úteis mensais ≥ 10 · taxa anual ∈ [0%, 25%] |

### Retentativas

`retries=3` com `retry_delay=5min` — a API pública do BCB apresenta instabilidade esporádica, especialmente fora do horário comercial.

---

## Testes

Suite de 35 testes unitários cobrindo quality gates, transformações e a lógica de taxa composta. Não dependem do Airflow nem de I/O real — usam `monkeypatch` nos path constants e `tmp_path` do pytest para isolar filesystem.

```bash
# Instalar dependências de desenvolvimento
pip install -r requirements-dev.txt

# Rodar os testes
pytest

# Com cobertura
pytest --cov=. --cov-report=term-missing
```

| Módulo | Testes | O que cobre |
|--------|--------|-------------|
| `test_bronze.py` | 9 | Schema da API, bounds dinâmicos de row count, URL parametrizada |
| `test_silver.py` | 14 | Type parsing, null ratio gate, colunas derivadas, sort, quality gates |
| `test_gold.py` | 12 | `_compound_rate`, sort por data, `is_base_month`, quality gates |

---

## Pré-requisitos

- Docker e Docker Compose v2.x
- Git

---

## Como executar (Docker)

```bash
# 1. Clone o repositório
git clone <repo-url>
cd beanalytic-selic-pipeline

# 2. Configure as variáveis de ambiente
cp .env.example .env
# Para produção: edite .env e gere uma nova Fernet key
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 3. Crie a pasta de dados (necessária pelo volume)
mkdir -p data

# 4. Build e inicialização (aguarda ~60s na primeira vez)
docker compose up --build airflow-init

# 5. Suba os serviços
docker compose up -d airflow-webserver airflow-scheduler

# 6. Acesse o Airflow UI
# http://localhost:8080
# Usuário: admin | Senha: admin

# 7. Ative e dispare a DAG manualmente
# UI → DAGs → selic_pipeline → Enable → Trigger DAG ▶
```

### Verificar os outputs

```bash
# Inspecionar Parquet gerado (requer pandas instalado localmente)
docker exec -it $(docker compose ps -q airflow-scheduler) \
  python -c "
import pandas as pd
df = pd.read_parquet('/opt/airflow/data/gold/selic_anual.parquet')
print(df.to_string())
"
```

### Parar e limpar

```bash
docker compose down -v   # remove containers e volumes
```

---

## Como executar (local, sem Docker)

```bash
# 1. Crie e ative o virtualenv
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows

# 2. Instale as dependências
pip install "apache-airflow==2.9.1" -r requirements.txt

# 3. Configure o Airflow
export AIRFLOW_HOME=$(pwd)/airflow_home
export PYTHONPATH=$(pwd)/dags
airflow db migrate
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@local.com

# 4. Adicione os DAGs
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

# 5. Inicie em dois terminais separados
airflow webserver --port 8080
airflow scheduler
```

---

## Estrutura do repositório

```
beanalytic-selic-pipeline/
├── dags/
│   ├── selic_pipeline.py       # DAG principal
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingest.py           # Task 1: BCB API → Parquet raw
│   ├── silver/
│   │   ├── __init__.py
│   │   └── transform.py        # Task 2: limpeza e padronização
│   └── gold/
│       ├── __init__.py
│       └── aggregate.py        # Task 3: métricas consolidadas
├── data/                       # Gerado em runtime (gitignored)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```
