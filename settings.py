import os

BOOSTRAP_SERVER="172.17.0.1:9092"
BTC_BLOCK_TOPIC = 'bitcoin'

CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', '172.17.0.1')
CASSANDRA_KEYSPACE = 'bitcoin'
