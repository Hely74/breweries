# 🍺 Pipeline de Dados – Cervejarias (Open Brewery DB)

## 📌 Descrição

Este projeto implementa um pipeline de dados completo baseado na arquitetura **Medallion (Bronze, Silver e Gold)** para ingestão, transformação e análise de dados de cervejarias a partir da API pública Open Brewery DB.

O pipeline foi desenvolvido com foco em boas práticas de engenharia de dados, incluindo tratamento de erros, particionamento, uso de formatos colunares e orquestração com Airflow.

---

## 🧱 Arquitetura

O pipeline segue o padrão Medallion:

### 🥉 Bronze (Dados Brutos)

* Extração dos dados diretamente da API
* Armazenamento em formato JSON
* Dados mantidos no formato original (sem transformação)

### 🥈 Silver (Dados Tratados)

* Remoção de duplicidades
* Tratamento de valores nulos
* Padronização de colunas
* Armazenamento em formato **Parquet**
* Particionamento por estado (localização)

### 🥇 Gold (Dados Analíticos)

* Agregação dos dados
* Cálculo da quantidade de cervejarias por:

  * Tipo (`brewery_type`)
  * Localização (`state`)
* Dados prontos para consumo analítico

---

## 🔄 Orquestração

O pipeline é orquestrado utilizando o Apache Airflow, com:

* Execução diária automática (agendamento via cron)
* Controle de dependência entre tarefas (Bronze → Silver → Gold)
* Retry automático em caso de falhas
* Alertas por e-mail em caso de erro
* Timeout de execução por tarefa

---

## ⚙️ Tecnologias utilizadas

* Python
* Pandas
* Apache Airflow
* Parquet (armazenamento colunar)
* Requests (consumo de API)

---

## ▶️ Como executar o projeto

### 1. Clonar o repositório

```bash
git clone https://github.com/seuusuario/cervejarias.git
cd cervejarias
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Executar manualmente (sem Airflow)

```bash
python extract_breweries_bronze.py
python save_breweries_silver.py
python save_breweries_gold.py
```

### 4. Executar com Airflow

```bash
airflow standalone
```

Depois, acessar a interface web do Airflow e executar a DAG:

```
brewery_medallion_pipeline
```

---

## 📊 Estrutura do projeto

```
cervejarias/
├── extract_breweries_bronze.py
├── save_breweries_silver.py
├── save_breweries_gold.py
├── brewery_pipeline.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── README.md
```

---

## 🧪 Testes (melhoria futura)

Como melhoria futura, podem ser adicionados testes automatizados para validação de:

* Qualidade dos dados (camada Silver)
* Integridade das agregações (camada Gold)
* Consumo da API (camada Bronze)

---

## 📡 Monitoramento e Alertas

O pipeline inclui:

* Retry automático em caso de falha na ingestão
* Alertas por e-mail via Airflow
* Logs de execução das tarefas

Como melhorias futuras:

* Integração com ferramentas de monitoramento (ex: Prometheus, Grafana)
* Alertas via Slack
* Métricas de qualidade de dados

---

## ☁️ Considerações sobre produção

Em ambiente produtivo, o pipeline poderia ser evoluído com:

* Armazenamento em nuvem (ex: AWS S3)
* Execução em containers (Docker/Kubernetes)
* Uso de variáveis de ambiente para configuração
* Particionamento adicional (ex: por país e data)

---

## 📌 Decisões de design

* Uso de arquitetura Medallion para separação de responsabilidades
* Escolha do formato Parquet para otimização de leitura
* Particionamento por estado para melhorar performance
* Uso de Airflow para orquestração e confiabilidade do pipeline

---

## 🚀 Conclusão

Este projeto demonstra a construção de um pipeline de dados completo, aplicando boas práticas de engenharia de dados, desde a ingestão até a disponibilização de dados para análise.

---
