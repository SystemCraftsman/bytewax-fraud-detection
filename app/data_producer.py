#!/usr/bin/env python3

import time
import json

from kafka import KafkaProducer

def main():
    transactions_file = open ('../resources/data/transactions.json')
    transactions = json.load(transactions_file)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    for transaction in transactions:
        transaction = json.dumps(transaction)
        producer.send('transactions', bytes(f'{transaction}','UTF-8'))
        print(f"Transaction data is sent: {transaction}")
        time.sleep(1)


if __name__ == "__main__":
    main()