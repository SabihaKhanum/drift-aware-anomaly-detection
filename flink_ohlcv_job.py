from pyflink.table import EnvironmentSettings, TableEnvironment
import os
from dotenv import load_dotenv

load_dotenv()

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql(f"""
CREATE TABLE crude_ticks (
    symbol STRING,
    price DOUBLE,
    volume DOUBLE,
    `timestamp` TIMESTAMP(3),
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'mcx-crude-prices',
    'properties.bootstrap.servers' = '{os.getenv("CONFLUENT_BOOTSTRAP")}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("CONFLUENT_API_KEY")}" password="{os.getenv("CONFLUENT_API_SECRET")}";',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
""")

table_env.execute_sql(f"""
CREATE TABLE crude_ohlcv_1m (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    symbol STRING,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'mcx-crude-ohlcv-1m',
    'properties.bootstrap.servers' = '{os.getenv("CONFLUENT_BOOTSTRAP")}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("CONFLUENT_API_KEY")}" password="{os.getenv("CONFLUENT_API_SECRET")}";',
    'format' = 'json'
)
""")

table_env.execute_sql("""
INSERT INTO crude_ohlcv_1m
SELECT
    window_start,
    window_end,
    symbol,
    FIRST_VALUE(price) AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    LAST_VALUE(price) AS close_price,
    SUM(volume) AS volume
FROM TABLE(
    TUMBLE(TABLE crude_ticks, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol
""").wait()