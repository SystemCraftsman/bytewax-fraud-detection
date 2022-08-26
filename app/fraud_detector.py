import json

from bytewax import Dataflow, spawn_cluster, cluster_main
from bytewax.inputs import ManualInputConfig, AdvanceTo, Emit
from kafka import KafkaConsumer


def load_json(file_name="mydata.json"):
    with open(file_name, "r") as open_file:
        for row in json.load(open_file):
            value = (
                str(row["id"]),
                (
                    row["Transaction_time"],
                    int(row["Amount_spent"]),
                ),
            )

            yield row["id"], value  # Change the form of value to key, value


def current_transaction_amount_abnormally_higher(
    current_amount, previous_amount
):
    return current_amount >= 1.5 * previous_amount

def input_builder(worker_index, worker_count, resume_epoch):
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=["192.168.1.18:9092"],
        auto_offset_reset='earliest',
        group_id = 'trx_group_id'
    )
    for message in consumer:
        data = json.loads(message.value.decode())
        value = (
                str(data["id"]),
                (
                    data["Transaction_time"],
                    int(data["Amount_spent"]),
                ),
            )

        yield AdvanceTo(data["id"])
        yield Emit(value)

def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, (key, payload) = epoch_item
        print(epoch, json.dumps(key), json.dumps(payload))

    return output_handler

class FraudTransaction:
    def __init__(self):
        self.previous_transaction_value = float("inf")
        self.current_transaction_value = None
        self.flagged_items = []
        self.num_of_one_day_transactions = 1
        self.previous_transaction_date = None

    def detect_fraud(self, data):

        self.current_transaction_value = data[1]

        if current_transaction_amount_abnormally_higher(
            self.current_transaction_value, self.previous_transaction_value
        ):
            self.flagged_items.append(data)

        current_transaction_date = data[0]

        if current_transaction_date == self.previous_transaction_date:
            self.num_of_one_day_transactions += 1
        else:
            self.previous_transaction_date = current_transaction_date
            self.num_of_one_day_transactions = 1

        if self.num_of_one_day_transactions >= 3:
            self.flagged_items.append(data)

        self.previous_transaction_value = self.current_transaction_value
        return self, self.flagged_items

def main():
    flow = Dataflow()

    flow.stateful_map(
        "fraud", lambda key: FraudTransaction(), FraudTransaction.detect_fraud
    )
    flow.reduce_epoch(lambda x, y: y)
    flow.capture()

    cluster_main(
        flow,
        ManualInputConfig(input_builder),
        output_builder,
        [], 0,
    )


if __name__ == "__main__":
    main()
