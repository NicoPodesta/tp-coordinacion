import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_publisher = middleware.MessageMiddlewareFanoutRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE
        )
        self.fruits_by_client = {}
        self.lock = threading.Lock()

    def _init_control_thread_resources(self):
        self.control_consumer = middleware.MessageMiddlewareFanoutRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data for client {client_id}")
        with self.lock:
            if client_id not in self.fruits_by_client:
                self.fruits_by_client[client_id] = {}
            client_fruits = self.fruits_by_client[client_id]
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Sending data messages for client {client_id}")
        with self.lock:
            client_fruits = self.fruits_by_client.pop(client_id, {})

        for final_fruit_item in client_fruits.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            client_id = fields[0]
            logging.info(
                f"Received EOF for client {client_id}, broadcasting to control exchange"
            )
            self.control_publisher.send(message)
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        logging.info(f"Control EOF received for client {client_id}")
        self._process_eof(client_id)
        ack()

    def _run_control_thread(self, ready_event):
        self._init_control_thread_resources()
        ready_event.set()
        self.control_consumer.start_consuming(self.process_control_message)

    def start(self):
        ready_event = threading.Event()
        control_thread = threading.Thread(
            target=self._run_control_thread,
            args=(ready_event,),
            daemon=True,
        )
        control_thread.start()
        ready_event.wait()
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
