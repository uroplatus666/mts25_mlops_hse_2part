import os, json, time, logging, sys
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("scores_link")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC     = os.getenv("KAFKA_SCORING_TOPIC")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = int(os.getenv("POSTGRES_PORT"))
PG_DB   = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

DDL = """
CREATE TABLE IF NOT EXISTS scores (
  transaction_id TEXT PRIMARY KEY,
  score          DOUBLE PRECISION NOT NULL,
  fraud_flag     INT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT = """
INSERT INTO scores (transaction_id, score, fraud_flag)
VALUES %s
ON CONFLICT (transaction_id) DO UPDATE
SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag, created_at = NOW();
"""

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def ensure_table(conn):
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
    log.info("Table 'scores' ensured")

def main():
    while True:
        try:
            conn = pg_connect()
            log.info("Successfully connected to PostgreSQL")
            ensure_table(conn)
            break
        except Exception as e:
            log.warning("Waiting for Postgres: %s", e)
            time.sleep(2)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        group_id="scores_link_group",
        consumer_timeout_ms=1000
    )
    log.info("Started sink for topic=%s", TOPIC)

    buf = []
    BATCH = 10
    while True:
        try:
            for msg in consumer:
                log.info("Received message: %s", msg.value)
                payload = msg.value
                try:
                    buf.append((
                        payload["transaction_id"],
                        float(payload["score"]),
                        int(payload["fraud_flag"])
                    ))
                    log.info("Added to buffer: %s", buf[-1])
                except (KeyError, ValueError) as ke:
                    log.error("Invalid message format: %s, error: %s", msg.value, ke)
                    continue
                if len(buf) >= BATCH:
                    log.info("Inserting %d records into PostgreSQL", len(buf))
                    with conn, conn.cursor() as cur:
                        execute_values(cur, UPSERT, buf, page_size=BATCH)
                        log.info("Successfully inserted %d records", len(buf))
                    buf.clear()
            if buf:
                log.info("Inserting %d records into PostgreSQL", len(buf))
                with conn, conn.cursor() as cur:
                    execute_values(cur, UPSERT, buf, page_size=len(buf))
                    log.info("Successfully inserted %d records", len(buf))
                buf.clear()
            log.debug("No messages received, sleeping for 0.2s")
            time.sleep(0.2)
        except Exception as e:
            log.exception("Sink error: %s", e)
            time.sleep(1.0)

if __name__ == "__main__":

    main()
