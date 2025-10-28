# services/fraud_detector/app/app.py
import os
import sys
import json
import uuid
import signal
import logging
import re
import csv
from io import StringIO
from typing import Dict, Any, Iterable, List, Tuple

import pandas as pd
from confluent_kafka import Consumer, Producer

os.makedirs("/app/logs", exist_ok=True)

# доступ к локальным модулям
sys.path.append(os.path.abspath("./src"))
from preprocessing import load_train_data, run_preproc
from scorer import XGBScorer


# --------- Логирование ---------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("/app/logs/service.log"), logging.StreamHandler()],
)
logger = logging.getLogger("fraud_detector")


# --------- Конфиг ---------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC")
GROUP_ID = "ml-fraud-detector"

# ожидаемый порядок колонок, если прилетела CSV-строка
CSV_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merch_lat",
    "merch_lon",
]

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


def _parse_csv_row(row_text: str) -> Dict[str, Any]:
    """
    Разобрать одну CSV-строку в dict нужного формата.
    Учитываем кавычки и запятые внутри полей (csv.reader).
    """
    reader = csv.reader(StringIO(row_text))
    row = next(reader)
    if len(row) < 2:
        raise ValueError("CSV row too short")

    row = (row + [""] * len(CSV_COLUMNS))[: len(CSV_COLUMNS)]
    rec = dict(zip(CSV_COLUMNS, row))

    # приведение типов
    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def _to_int(x):
        try:
            return int(float(x))
        except Exception:
            return None

    rec["amount"] = _to_float(rec.get("amount"))
    for k in ("lat", "lon", "merch_lat", "merch_lon"):
        rec[k] = _to_float(rec.get(k))
    for k in ("post_code", "population_city"):
        rec[k] = _to_int(rec.get(k))

    return rec


def _extract_records(payload: Any) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Унифицированный парсер входящих сообщений из Kafka.

    Поддерживаем 3 формата:
      A) {"transaction_id": "...", "data": {...}}  — "обёртка" с данными
      B) {...}                                    — плоский JSON строки датасета
      C) "csv,строка,...."                        — сырая CSV-строка
    """
    records: List[Tuple[str, Dict[str, Any]]] = []

    def one(item: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        if "data" in item and isinstance(item["data"], dict):
            rec = item["data"]
            tx_id = (
                item.get("transaction_id")
                or rec.get("transaction_id")
                or rec.get("id")
                or str(uuid.uuid4())
            )
            return str(tx_id), rec

        tx_id = item.get("transaction_id") or item.get("id") or str(uuid.uuid4())
        return str(tx_id), item

    if isinstance(payload, str):
        rec = _parse_csv_row(payload)
        tx_id = rec.get("transaction_id") or str(uuid.uuid4())
        return [(tx_id, rec)]

    if isinstance(payload, list):
        for it in payload:
            if isinstance(it, str):
                rec = _parse_csv_row(it)
                tx_id = rec.get("transaction_id") or str(uuid.uuid4())
                records.append((tx_id, rec))
            elif isinstance(it, dict):
                records.append(one(it))
    elif isinstance(payload, dict):
        records.append(one(payload))
    else:
        raise ValueError(f"Unsupported payload type: {type(payload)}")

    return records


def _maybe_repair_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    ts = rec.get("transaction_time")
    if isinstance(ts, str) and "," in ts:
        # Пытаемся распарсить всю строку — если получилось, просто берём оттуда поля
        try:
            parsed = _parse_csv_row(ts)
            # не теряем дополнительные поля, если были
            parsed.update({k: v for k, v in rec.items() if k not in parsed})
            rec = parsed
            return rec
        except Exception:
            # fallback: вытащить только дату-время в начале
            m = re.match(r"^\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?)", ts)
            if m:
                rec["transaction_time"] = m.group(1)
    return rec


class Service:
    def __init__(self):
        logger.info("Loading reference train for preprocessing...")
        self.train = load_train_data()

        logger.info("Loading XGBoost model...")
        self.scorer = XGBScorer()

        logger.info("Setting up Kafka...")
        self.producer = make_producer()
        self.consumer = make_consumer()
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        logger.info(
            "Service is ready. bootstrap=%s, in=%s, out=%s, group=%s",
            KAFKA_BOOTSTRAP_SERVERS,
            TRANSACTIONS_TOPIC,
            SCORING_TOPIC,
            GROUP_ID,
        )

        signal.signal(signal.SIGTERM, self._graceful_shutdown)
        signal.signal(signal.SIGINT, self._graceful_shutdown)
        self._running = True

    def _graceful_shutdown(self, *args):
        logger.info("Shutdown signal received, stopping loop...")
        self._running = False

    def _score_record(self, tx_id: str, rec: Dict[str, Any]) -> Dict[str, Any]:
        rec = _maybe_repair_record(rec)

        # Делаем DataFrame из одной строки
        df = pd.DataFrame([rec])

        # Препроцессинг с выравниванием под train
        X = run_preproc(self.train, df)

        # Скоринг XGB
        pred, probs = self.scorer.make_pred(X)
        score = float(probs[0])
        label = int(pred[0])

        return {"transaction_id": tx_id, "score": score, "fraud_flag": label}

    def handle_message(self, raw: bytes):
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as e:
            txt = raw.decode("utf-8", errors="ignore").strip()
            try:
                items = _extract_records(txt)
            except Exception:
                logger.error("Invalid payload (neither JSON nor CSV): %s", e, exc_info=True)
                return
        else:
            try:
                items = _extract_records(payload)
            except Exception as e:
                logger.error("Failed to extract records: %s", e, exc_info=True)
                return

        for tx_id, rec in items:
            try:
                out = self._score_record(tx_id, rec)
            except Exception as e:
                logger.exception(
                    "Scoring failed for tx_id=%s (keys=%s): %s",
                    tx_id,
                    list(rec.keys())[:10],
                    e,
                )
                continue

            try:
                self.producer.produce(
                    SCORING_TOPIC,
                    json.dumps(out).encode("utf-8"),
                    callback=lambda err, msg: (
                        logger.error("Produce error: %s", err)
                        if err
                        else None
                    ),
                )
                self.producer.poll(0)
                logger.info(
                    "Scored tx_id=%s -> score=%.6f, fraud_flag=%d",
                    out["transaction_id"],
                    out["score"],
                    out["fraud_flag"],
                )
            except Exception as e:
                logger.exception("Failed to publish result for tx_id=%s: %s", tx_id, e)

    def run(self):
        logger.info("Starting Kafka consume loop...")
        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue

                self.handle_message(msg.value())
        finally:
            try:
                logger.info("Flushing producer...")
                self.producer.flush(5)
            finally:
                logger.info("Closing consumer...")
                self.consumer.close()
                logger.info("Stopped.")

if __name__ == "__main__":
    Service().run()
