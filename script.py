import json

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

from settings import BTC_BLOCK_TOPIC, BOOSTRAP_SERVER, CASSANDRA_HOST, CASSANDRA_KEYSPACE

consumer = KafkaConsumer(
    BTC_BLOCK_TOPIC, bootstrap_servers="%s"  % (BOOSTRAP_SERVER))

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect(CASSANDRA_KEYSPACE)

try:
    for message in consumer:
        entry = json.loads(message.value)
#         session.execute(
#             """
# INSERT INTO bitcoin.blocks (height)
# VALUES (%s)
# """,
#             (entry['source'],)
