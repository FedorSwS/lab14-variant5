# Лабораторная работа №14: Разработка конвейеров обработки данных

## Информация о студенте
| Поле | Значение |
|------|----------|
| **ФИО** | Евстигнеев Фёдор Алексеевич |
| **Группа** | 220032-11 |
| **Лабораторная работа** | №14 |
| **Вариант** | №5 (Анализ логов веб-сервера) |
| **Уровень сложности** | **ПОВЫШЕННЫЙ** |

## Выполненные задания повышенного уровня

| № | Задание | Статус |
|---|---------|--------|
| 1 | Распределённый сборщик на Go (etcd) | ✅ |
| 2 | Оконная агрегация в Go (tumbling window) | ✅ |
| 3 | Передача данных через Apache Arrow | ✅ |
| 4 | Интеграция Rust-библиотеки (PyO3) | ✅ |
| 5 | Развёртывание в Kubernetes с HPA | ✅ |
| 6 | Сравнение Go vs Python (бенчмарки) | ✅ |
| 7 | Обработка потоковых данных (Kafka) | ✅ |
| 8 | Веб-дашборд с обновлением (Streamlit) | ✅ |

## Архитектура конвейера

Go Collector (etcd sharding) → Tumbling Window (5s) → Apache Arrow Flight →
Rust Validator (PyO3) → Kafka → Python (Polars/DuckDB) → Streamlit Dashboard

## Быстрый старт

```bash
# Сборка Go коллектора
cd collector && go build -o collector . && cd ..

# Запуск
./collector/collector -workers=4 -rate=100

# Анализ данных
python analyzer/analyzer.py data/raw/logs.jsonl

# Kafka consumer
python analyzer/kafka_consumer.py

# Streamlit дашборд
streamlit run analyzer/dashboard.py

# Бенчмарк Go vs Python
python analyzer/benchmark.py

# Docker сборка
docker build -t log-collector .

# Kubernetes
kubectl apply -f k8s/
Результаты бенчмарков
Metric	Go	Python	Improvement
CPU Usage	~45%	~89%	2.0x
Memory	~28 MB	~156 MB	5.6x
Throughput	10k logs/s	1.5k logs/s	6.7x
Ссылки на репозиторий
https://github.com/FedorSwS/lab14-variant5

## Kafka Producer в Go

Коллектор отправляет данные в Kafka:
- **Сырые логи** → топик `logs`
- **Агрегированные статистики** (каждые 5 секунд) → топик `aggregated`

```go
// Пример отправки в Kafka
kafkaProducer.SendWindowStats(ctx, stats)
kafkaProducer.SendLogEntry(ctx, entry)

Переменные окружения
Переменная	Описание
KAFKA_BOOTSTRAP	Адрес Kafka брокера (например, localhost:9092)
KAFKA_TOPIC	Имя топика (по умолчанию logs)

Запуск с Kafka

export KAFKA_BOOTSTRAP=localhost:9092
./collector/collector -kafka-topic=logs
