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
        tx["risk_level"] = "HIGH"
    elif tx["amount"] > 1000:
        tx["risk_level"] = "MEDIUM"
    else:
        tx["risk_level"] = "LOW"

    if tx["risk_level"] == "HIGH":
        print(f"HIGHT {tx['tx_id']} | {tx['amount']} PLN | Sklep: {tx['store']}")
    elif tx["risk_level"] == "MEDIUM":
        print(f"MEDIUM {tx['tx_id']} | {tx['amount']} PLN | Sklep: {tx['store']}")
    else:
        print(f"LOW {tx['tx_id']} | {tx['amount']} PLN | Sklep: {tx['store']}")
