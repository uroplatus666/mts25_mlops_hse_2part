from __future__ import annotations
import logging
import os
import numpy as np
import pandas as pd
from geopy.distance import great_circle

# Настройка логгера
logger = logging.getLogger(__name__)

TRAIN_PATH = "/app/train_data/train.csv"

def add_time_features(df):
    logger.info('Adding time features...')
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    df.drop(columns='transaction_time', inplace=True)
    return df


def add_distance_features(df):
    logger.info('Calculating distances...')
    df['distance'] = df.apply(
        lambda x: great_circle(
            (x['lat'], x['lon']), 
            (x['merchant_lat'], x['merchant_lon'])
        ).km,
        axis=1
    )
    return df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])


def _basic_transforms(df):
    # Добавляем временные фичи
    df = add_time_features(df)
    # Считаем расстояние между пользователями
    df = add_distance_features(df)
    # Переводим gender в int
    df['gender'] = df['gender'].apply(lambda x: 1 if x == 'M' else 0).astype(int)
    return df


def _align_categoricals_by_train(train, test):
    cat_cols = ['cat_id', 'one_city', 'us_state', 'jobs']
    for c in cat_cols:
        if c in test.columns:
            test[c] = test[c].astype("category")
            test[c] = pd.Categorical(test[c], categories=train[c].cat.categories)
    test = test[train.columns]
    return test

def load_train_data():
    # Загрузка и базовая предобработка
    df = pd.read_csv(TRAIN_PATH).drop(columns=['merch','name_1','name_2','street','post_code', 'target'])
    df = df.drop_duplicates()
    logger.info("Loaded reference train: shape=%s", df.shape)
    df = _basic_transforms(df)
    num_cols = df.select_dtypes(include=['number']).columns
    if len(num_cols):
        df[num_cols] = df[num_cols].fillna(df[num_cols].median())
    cat_cols = ['cat_id', 'one_city', 'us_state', 'jobs']
    for c in cat_cols:
        df[c] = df[c].astype("category")
    logger.info("Preprocessed train: shape=%s, columns=%s", df.shape, list(df.columns))
    return df

def run_preproc(train_ref, df):
    # Базовая предобработка
    df = df.drop_duplicates()
    df = _basic_transforms(df)
    num_cols = df.select_dtypes(include=['number']).columns
    if len(num_cols):
        df[num_cols] = df[num_cols].fillna(df[num_cols].median())
    # Cогласование категорий
    logger.info('Aligning categories...')
    df = _align_categoricals_by_train(train_ref, df)

    return df