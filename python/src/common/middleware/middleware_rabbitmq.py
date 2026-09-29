import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError,
)


class _MessageMiddlewareRabbitMQ:
    def __init__(self, host):
        self.host = host
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )
        self.channel = self.connection.channel()

    def start_consuming(self, queue_name, on_message_callback):
        def _safe_ack(fn, ack_type):
            try:
                fn()
            except (AMQPConnectionError, AMQPChannelError) as e:
                raise MessageMiddlewareDisconnectedError(
                    f"Connection lost on {ack_type}: {e}"
                )
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Error on {ack_type}: {e}")

        def _internal_callback(channel, method, properties, body):
            def ack():
                _safe_ack(
                    lambda: channel.basic_ack(delivery_tag=method.delivery_tag), "ack"
                )

            def nack():
                _safe_ack(
                    lambda: channel.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=True
                    ),
                    "nack",
                )

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=_internal_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while consuming: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"Unexpected error while consuming: {e}"
            )

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while stopping consumption: {e}"
            )

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Unexpected error while closing: {e}")


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._core = None

        try:
            self._core = _MessageMiddlewareRabbitMQ(host)
            self._core.channel.queue_declare(queue=self._queue_name, durable=False)
            self._core.channel.basic_qos(prefetch_count=1)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"connecting or declaring queue: {e}"
            )

    def send(self, message):
        try:
            self._core.channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending message: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"Unexpected error while sending message: {e}"
            )

    def start_consuming(self, on_message_callback):
        self._core.start_consuming(self._queue_name, on_message_callback)

    def stop_consuming(self):
        self._core.stop_consuming()

    def close(self):
        self._core.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._core = None
        self._queue_name = None

        try:
            self._core = _MessageMiddlewareRabbitMQ(host)
            channel = self._core.channel
            channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type="direct",
                durable=False,
            )
            self._queue_name = channel.queue_declare(
                queue="", exclusive=True
            ).method.queue

            for routing_key in self._routing_keys:
                channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key,
                )

            channel.basic_qos(prefetch_count=1)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error connecting or declaring exchange: {e}"
            )

    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._core.channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=1),
                )
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending message to exchange: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"Unexpected error while sending message to exchange: {e}"
            )

    def start_consuming(self, on_message_callback):
        self._core.start_consuming(self._queue_name, on_message_callback)

    def stop_consuming(self):
        self._core.stop_consuming()

    def close(self):
        self._core.close()
