# Stocks Airflow

1.

```
    mkdir -p ./dags ./logs ./plugins ./config  ./output
    echo -e "AIRFLOW_UID=$(id -u)" > .env
```

*Note: Output is extra for local (non-cloud) output.*

2.

```
    docker compose up airflow-init
```

3.

```
    docker compose up
```
