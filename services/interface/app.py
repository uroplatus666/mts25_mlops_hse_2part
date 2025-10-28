import os, io, uuid
import pandas as pd
import streamlit as st
from kafka import KafkaProducer
import psycopg2
import plotly.express as px

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_IN_TOPIC  = os.getenv("KAFKA_TRANSACTIONS_TOPIC")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = int(os.getenv("POSTGRES_PORT"))
PG_DB   = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")


# Page styling
st.set_page_config(page_title="Fraud RT", page_icon="🛡️", layout="wide")

st.markdown(
    """
    <style>
      .main > div { padding-top: 1rem; }
      /* Title banner */
      .title-card {
          background: radial-gradient(1200px 300px at 10% 0%, rgba(56,189,248,.15), rgba(56,189,248,0));
          border: 1px solid rgba(148,163,184,.18);
          border-radius: 16px;
          padding: 18px 18px 14px 18px;
          margin-bottom: 8px;
      }
      .title-card h1 {
          font-size: 1.9rem;
          line-height: 1.2;
          margin: 0;
      }
      .title-sub {
          color: #94a3b8;
          font-size: 0.95rem;
          margin-top: .35rem;
      }
      /* Buttons */
      .stButton>button {
          border-radius: 10px;
          padding: .6rem 1rem;
          border: 1px solid rgba(148,163,184,.25);
          box-shadow: 0 1px 0 rgba(0,0,0,.04);
      }
      /* Cards */
      .metric-card {
          border: 1px solid rgba(148,163,184,.18);
          border-radius: 14px;
          padding: 14px;
          background: rgba(15,23,42,.45);
      }
      /* Dataframe container */
      .block-container { padding-top: 1.2rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="title-card"><h1>⚡️ Real-Time Fraud</h1>'
    '<div class="title-sub">CSV → Kafka → Scoring → Postgres | live glance at risky transactions</div>'
    '</div>',
    unsafe_allow_html=True,
)

# Tabs
tab_send, tab_view = st.tabs(["📤 Отправить CSV → Kafka", "📊 Посмотреть результаты"])

# TAB 1 — SEND TO KAFKA
with tab_send:
    st.markdown("Загрузите `test.csv` из соревнования — строки будут отправлены в Kafka по одной.")
    with st.expander("Формат файла (проверка перед отправкой)", expanded=False):
        st.caption("Файл может быть любым CSV. Если нет столбца `transaction_id`, он будет создан автоматически (UUID).")

    uploaded = st.file_uploader("CSV файл", type=["csv"], help="Перетащите файл или выберите его на диске")
    preview_cols = st.empty()
    preview_table = st.empty()

    if uploaded is not None:
        try:
            df_preview = pd.read_csv(io.BytesIO(uploaded.getvalue()), nrows=200)
            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("Кол-во колонок", len(df_preview.columns))
            with c2: st.metric("Предпросмотр строк", len(df_preview))
            with c3: st.metric("Есть transaction_id", "Да" if "transaction_id" in df_preview.columns else "Нет")
            with c4: st.metric("Размер файла, KB", f"{len(uploaded.getvalue())/1024:.1f}")
            st.caption("Предпросмотр первых 200 строк:")
            st.dataframe(df_preview, use_container_width=True)
        except Exception as e:
            st.warning(f"Не удалось прочитать CSV: {e}")

    st.divider()
    col_btn, col_hint = st.columns([1, 2])
    with col_btn:
        send_clicked = st.button("🚀 Отправить в Kafka", use_container_width=True, disabled=(uploaded is None))
    with col_hint:
        st.caption("Сообщения будут отправлены последовательно. Состояние — ниже.")

    if send_clicked and uploaded is not None:
        try:
            df = pd.read_csv(io.BytesIO(uploaded.getvalue()))
            # Гарантируем наличие transaction_id
            if "transaction_id" not in df.columns:
                df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

            # Визуальные индикаторы процесса
            progress = st.progress(0, text="Подключение к Kafka…")
            status_box = st.empty()

            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: v.encode("utf-8"),
            )
            progress.progress(1, text="Отправка сообщений…")

            total = len(df)
            sent = 0
            # шагаем прогрессом по 100 шагам
            step = max(1, total // 100)

            for i, (_, row) in enumerate(df.iterrows(), start=1):
                prod.send(KAFKA_IN_TOPIC, row.to_json())
                sent += 1
                if i % step == 0 or i == total:
                    progress.progress(min(i / total, 1.0), text=f"Отправлено: {i}/{total}")

            prod.flush()
            status_box.success(f"✅ Готово: отправлено сообщений — {sent}")
            st.toast("Отправка завершена", icon="✅")
        except Exception as e:
            st.error(f"Ошибка отправки в Kafka: {e}")

# TAB 2 — VIEW RESULTS
with tab_view:
    st.subheader("Последние фрод-транзакции")

    # UI элементы перед запросами
    act_cols = st.columns([1, 1, 6])
    with act_cols[0]:
        refresh = st.button("🔄 Обновить", use_container_width=True)
    with act_cols[1]:
        st.caption("Данные грузятся напрямую из БД.")

    if refresh:
        st.rerun()

    try:
        # Подключение
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
        )

        # ===== Query 1 =====
        q1 = """
        SELECT transaction_id, score, fraud_flag, created_at
        FROM scores
        WHERE fraud_flag = 1
        ORDER BY created_at DESC
	LIMIT 10;
        """
        df1 = pd.read_sql(q1, conn)

        # Верхняя сводка
        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Фрод-событий (в выборке)", len(df1))
            st.markdown("</div>", unsafe_allow_html=True)
        with m2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Макс. скор (fraud=1)", f"{(df1['score'].max() if not df1.empty else float('nan')):.3f}")
            st.markdown("</div>", unsafe_allow_html=True)
        with m3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Мин. скор (fraud=1)", f"{(df1['score'].min() if not df1.empty else float('nan')):.3f}")
            st.markdown("</div>", unsafe_allow_html=True)

        st.dataframe(df1, use_container_width=True)

        st.subheader("Распределение скоров (последние 100)")
        # ===== Query 2 =====
        q2 = """
        SELECT score FROM scores
        ORDER BY created_at DESC
        LIMIT 100;
        """
        df2 = pd.read_sql(q2, conn)

        if len(df2) > 0:
            # гистограмма plotly
            fig = px.histogram(
                df2,
                x="score",
                nbins=20,
                title="Histogram of scores (last 100)",
            )
            fig.update_layout(
                bargap=0.08,
                margin=dict(l=10, r=10, t=48, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Небольшая сводка по последним 100
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("N", int(df2["score"].count()))
            with c2:
                st.metric("Mean", f"{df2['score'].mean():.3f}")
            with c3:
                st.metric("Median", f"{df2['score'].median():.3f}")
            with c4:
                st.metric("Std", f"{df2['score'].std():.3f}")
        else:
            st.info("В базе пока нет данных для графика.")
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {e}")

