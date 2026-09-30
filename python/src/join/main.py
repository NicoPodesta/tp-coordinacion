import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_client = {}

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        try:
            self.input_queue.schedule_on_consumer_thread(
                self.input_queue.stop_consuming
            )
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")
        if self._prev_sigterm_handler and callable(self._prev_sigterm_handler):
            self._prev_sigterm_handler(signum, frame)

    def _send_final_top(self, client_id):
        partial_tops = self.partial_tops_by_client.pop(client_id, [])
        all_fruits = {}

        for top in partial_tops:
            for fruit, amount in top:
                fi = fruit_item.FruitItem(fruit, int(amount))
                if fruit in all_fruits:
                    all_fruits[fruit] = all_fruits[fruit] + fi
                else:
                    all_fruits[fruit] = fi

        sorted_items = sorted(all_fruits.values())
        top_chunk = list(sorted_items[-TOP_SIZE:])
        top_chunk.reverse()
        final_top = list(map(lambda fi: (fi.fruit, fi.amount), top_chunk))

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )
        logging.info(f"Sent final top to gateway for client {client_id}")

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            partial_top = fields[1]

            if client_id not in self.partial_tops_by_client:
                self.partial_tops_by_client[client_id] = []
            self.partial_tops_by_client[client_id].append(partial_top)

            logging.info(
                f"Received partial top {len(self.partial_tops_by_client[client_id])}/{AGGREGATION_AMOUNT} for client {client_id}"
            )

            if len(self.partial_tops_by_client[client_id]) == AGGREGATION_AMOUNT:
                self._send_final_top(client_id)

            ack()
        except Exception as e:
            logging.error(f"Error processing message in join: {e}")
            nack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        except middleware.MessageMiddlewareDisconnectedError:
            logging.info("Disconnected from RabbitMQ")
        finally:
            self.close()

    def close(self):
        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing input queue: {e}")
        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing output queue: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    main()
