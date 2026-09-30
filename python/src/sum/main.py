import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.control_consumer = None
        self.control_thread = None
        self.data_output_exchanges = []
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_publisher = middleware.MessageMiddlewareFanoutRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE
        )
        self.fruits_by_client = {}
        self.lock = threading.Lock()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        try:
            if hasattr(self, "input_queue") and self.input_queue:
                self.input_queue.schedule_on_consumer_thread(
                    self.input_queue.stop_consuming
                )
        except Exception as e:
            logging.error(f"Error stopping input queue consumer: {e}")
        try:
            if hasattr(self, "control_consumer") and self.control_consumer:
                self.control_consumer.schedule_on_consumer_thread(
                    self.control_consumer.stop_consuming
                )
        except Exception as e:
            logging.error(f"Error stopping control consumer: {e}")
        if self._prev_sigterm_handler and callable(self._prev_sigterm_handler):
            self._prev_sigterm_handler(signum, frame)

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
        logging.debug(f"Process data for client {client_id}: {fruit}={amount}")
        with self.lock:
            if client_id not in self.fruits_by_client:
                logging.info(f"Started receiving data for client {client_id}")
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
            agg_index = (
                0
                if AGGREGATION_AMOUNT <= 0
                else zlib.crc32(final_fruit_item.fruit.encode("utf-8"))
                % AGGREGATION_AMOUNT
            )
            self.data_output_exchanges[agg_index].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        try:
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
        except Exception as e:
            logging.error(f"Error processing data message in sum: {e}")
            nack()

    def process_control_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            logging.info(f"Control EOF received for client {client_id}")
            self._process_eof(client_id)
            ack()
        except Exception as e:
            logging.error(f"Error processing control message in sum: {e}")
            nack()

    def _run_control_thread(self, ready_event):
        self._init_control_thread_resources()
        ready_event.set()
        try:
            self.control_consumer.start_consuming(self.process_control_message)
        except middleware.MessageMiddlewareDisconnectedError:
            logging.info("Control consumer disconnected from RabbitMQ")
        finally:
            self._close_control_thread_resources()

    def _close_control_thread_resources(self):
        try:
            if hasattr(self, "control_consumer") and self.control_consumer:
                self.control_consumer.close()
        except Exception as e:
            logging.error(f"Error closing control consumer: {e}")
        for exchange in getattr(self, "data_output_exchanges", []):
            try:
                exchange.close()
            except Exception as e:
                logging.error(f"Error closing data output exchange: {e}")

    def start(self):
        ready_event = threading.Event()
        self.control_thread = threading.Thread(
            target=self._run_control_thread,
            args=(ready_event,),
        )
        self.control_thread.start()
        ready_event.wait()
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        except middleware.MessageMiddlewareDisconnectedError:
            logging.info("Data consumer disconnected from RabbitMQ")
        finally:
            self.close()

    def close(self):
        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing input queue: {e}")
        try:
            self.control_publisher.close()
        except Exception as e:
            logging.error(f"Error closing control publisher: {e}")
        if (
            hasattr(self, "control_thread")
            and self.control_thread
            and self.control_thread.is_alive()
        ):
            self.control_thread.join()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
