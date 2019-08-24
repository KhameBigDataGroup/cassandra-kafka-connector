import os

KAFKA_HOST = os.environ.get('KAFKA_HOST', '172.17.0.1')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '')
BTC_BLOCK_TOPIC = 'bitcoin'

CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', '172.17.0.1')
CASSANDRA_KEYSPACE = 'bitcoin'
