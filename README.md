# Real-Time Fraud Detection System
Датасеты предоставлены в рамках соревнования https://www.kaggle.com/competitions/teta-ml-1-2025

## 🧩 Архитектура

🔨 Компоненты системы:
1. **`interface`** (Streamlit UI):
   
   Создан для удобной симуляции потоковых данных с транзакциями. Реальный продукт использовал бы прямой поток данных из других систем.
    - Имитирует отправку транзакций в Kafka через CSV-файлы.
    - Генерирует уникальные ID для транзакций.
    - Загружает транзакции отдельными сообщениями формата JSON в топик kafka `transactions`.
    - Вывод 10 последних транзакций, признанных моделью фродовыми из PostgreSQL базы (если в базе менее 10 фродовых транзакций, то для тех, что есть в базе).
    - Гистограмма распределения скоров последних 100 транзакций (если в базе менее 100 транзакций, то для тех, что есть в базе).
    - порт 8501

2. **`fraud_detector`** (ML Service):
   - Загружает предобученную модель XGBoost (`model_xgb.json`) с [диска](https://drive.google.com/uc?id=1fQ7hQUijNBnCQEptuREDvPOTPHXyWRsa).
   - Выполняет препроцессинг данных:
     - Базовая обработка: удаление дубликатов, замена числовых пропусков медианой по признаку
     - Извлечение временных признаков
     - Геопространственные расчеты: расчет расстояния между пользователями(км)
     - Категориальные переменные: перевод 'object'-> 'category'
   - Производит скоринг с порогом 0.98.
   - Выгружает результат скоринга в топик kafka `scoring`

3. **Kafka Infrastructure**:
   - Zookeeper + Kafka брокер (порты: 2181, 9095)
   - `kafka-setup`: автоматически создает топики `transactions` и `scoring`
   - Kafka UI: веб-интерфейс для мониторинга сообщений (порт 8080)
    
4. **`scores_link`**
   - Читает сообщения из Kafka (топик scoring) с результатами скоринга с полями transaction_id, score, fraud_flag
   - Складывает в созданную PostgreSQL базу (порт 5432)
  
## ✏️ Структура проекта

```
.
├── services/
│   ├── fraud_detector/
│   │   ├── app/
│   │   │   └── app.py              # Ядро сервиса с обработчиком файлов, Kafka Consumer/Producer
│   │   ├── models/
│   │   │   └── model_xgb.json      # Модель XGBoost
│   │   ├── src/
│   │   │   ├── preprocessing.py   # Препроцессинг
│   │   │   └── scorer.py          # Применение ML-модели и предсказание
│   │   ├── train_data/
│   │   │   └── train.csv
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── interface/                  # Streamlit UI
│   │   ├── app.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── scores_link/                # Результаты скоринга
│       ├── app.py
│       ├── Dockerfile
│       └── requirements.txt
├── .env
├── docker-compose.yml
└── README.md
```

## 🛠️ Использование

### 1. Загрузка данных:

 - Загрузите CSV через интерфейс Streamlit. Для тестирования работы проекта используется файл формата `test.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Мониторинг:
 - **Kafka UI**: Просматривайте сообщения в топиках transactions и scoring
 - **Логи обработки**: /app/logs/service.log внутри контейнера fraud_detector

### 3. Результаты:

 - Скоринговые оценки пишутся в топик scoring в формате:
    ```json
    {
    "score": 0.995, 
    "fraud_flag": 1, 
    "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
    }
    ```
В Streamlit можно посмотреть:
- 10 последних транзакций, признанных моделью фродовыми из PostgreSQL базы (если в базе менее 10 фродовых транзакций, то для тех, что есть в базе).
- Гистаграмму распределения скоров последних 100 транзакций (если в базе менее 100 транзакций, то для тех, что есть в базе).

## 🚀 Быстрый старт

### Требования
- Docker 20.10+
- Docker Compose 2.0+

### Запуск

Скачайте файл `train.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025 и разместите в директории `.services/fraud_detector/train_data`
```bash
git clone https://github.com/uroplatus666/mts25_mlops_hse_2part.git
cd mts25_mlops_hse_2part

# Сборка и запуск всех сервисов
docker-compose up --build
```
После запуска:
- **Streamlit UI**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **Логи сервисов**: 
```bash
docker-compose logs <service_name>  # Например: fraud_detector, kafka, interface
```

Посмотреть содержание PostgreSQL БД
```bash
docker exec -it <CONTAINER ID>  psql -U app -d frauddb
\dt
```
Остановить контейнеры и удалить все созданные volume
```bash
docker compose down
docker volume rm $(docker volume ls -q)
```

## Настройки Kafka
```yml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)

Репликация: 1 (для разработки)
Партиции: 3
```

*Примечание:* 

Для полной функциональности убедитесь, что:
1. Модель `model_xgb.json` размещена в `services/fraud_detector/models/` (нужна примерно минута на автоматическую загрузку с диска)
2. Тренировочные данные находятся в `services/fraud_detector/train_data/`
3. Порты 8080, 8501, 9095, 2181 и 5432 свободны на хосте
