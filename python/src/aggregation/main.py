import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client = {}
        self.eof_count = {}

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        try:
            self.input_exchange.schedule_on_consumer_thread(
                self.input_exchange.stop_consuming
            )
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")
        if self._prev_sigterm_handler and callable(self._prev_sigterm_handler):
            self._prev_sigterm_handler(signum, frame)

    def _process_data(self, client_id, fruit, amount):
        logging.debug(
            f"Processing data message for client {client_id}: {fruit}={amount}"
        )
        if client_id not in self.fruits_by_client:
            logging.info(f"Started receiving data for client {client_id}")
            self.fruits_by_client[client_id] = {}
        client_fruits = self.fruits_by_client[client_id]
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        logging.info(
            f"Received EOF {self.eof_count[client_id]}/{SUM_AMOUNT} for client {client_id}"
        )
        if self.eof_count[client_id] < SUM_AMOUNT:
            return

        client_fruits = self.fruits_by_client.pop(client_id, {})
        self.eof_count.pop(client_id, None)

        items = sorted(client_fruits.values())
        fruit_chunk = list(items[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, fruit_top])
        )
        logging.info(f"Sent partial top to join for client {client_id}")

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._process_eof(*fields)
            ack()
        except Exception as e:
            logging.error(f"Error processing message in aggregation: {e}")
            nack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        except middleware.MessageMiddlewareDisconnectedError:
            logging.info("Disconnected from RabbitMQ")
        finally:
            self.close()

    def close(self):
        try:
            self.input_exchange.close()
        except Exception as e:
            logging.error(f"Error closing input exchange: {e}")
        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing output queue: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
