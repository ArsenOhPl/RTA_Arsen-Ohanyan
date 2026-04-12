from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    if tx["amount"] > 3000:
        print(f"HIGHT {tx['tx_id']} | {tx['amount']} PLN | Sklep: {tx['store']}")
    elif tx["amount"] in range (1000, 3001):
        print(f"MEDIUM {tx['tx_id']} | {tx['amount']} PLN | Sklep: {tx['store']}")
    else tx["amount"] < 1000:
        print(f"LOW {tx['tx_id']} | {tx['amount']} PLN | Sklep: {tx['store']}")
