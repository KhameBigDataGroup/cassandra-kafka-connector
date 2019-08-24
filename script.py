import json

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

from settings import BTC_BLOCK_TOPIC, BOOSTRAP_SERVER, CASSANDRA_HOST, CASSANDRA_KEYSPACE

consumer = KafkaConsumer(
    BTC_BLOCK_TOPIC, bootstrap_servers="%s"  % (BOOSTRAP_SERVER))

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect(CASSANDRA_KEYSPACE)

for message in consumer:
    entry = json.loads(message.value)
    session.execute(
        """
INSERT INTO bitcoin.blocks (hash, confirmations, strippedsize, size, weight, height, version, version_hex, merkleroot, time, mediantime, nonce, bits, difficulty, chainwork, n_tx, previousblockhash, nextblockhash)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""",
        (entry['hash'], entry['confirmations'], entry['strippedsize'], entry['size'], entry['weight'], entry['height'], entry['version'], entry['versionHex'], entry['merkleroot'], entry['time'], entry['mediantime'], entry['nonce'], entry['bits'], entry['difficulty'], entry['chainwork'], entry['nTx'], entry['previousblockhash'], entry['nextblockhash']))
