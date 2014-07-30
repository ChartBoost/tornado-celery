from __future__ import absolute_import

import sys
import numbers

from functools import partial
from datetime import timedelta

from kombu import serialization

from kombu.utils import uuid
from celery.app.amqp import TaskProducer
from kombu import Exchange
from kombu.common import Broadcast
from celery import signals
from celery.utils.timeutils import to_utc
from kombu.utils.encoding import safe_repr
from celery.five import string_t
from celery.utils import timeutils
from tornado.concurrent import Future

from .result import AsyncResult

is_py3k = sys.version_info >= (3, 0)


class MissingBrokerException(Exception):
    pass


class NonBlockingTaskProducer(TaskProducer):

    conn_pool = None
    app = None
    TIMEOUT = 5
    result_cls = AsyncResult

    def __init__(self, channel=None, *args, **kwargs):
        super(NonBlockingTaskProducer, self).__init__(channel, *args, **kwargs)

    def on_publish(self, task_id, future, backend, *args):
        future.set_result(self.result_cls(task_id, backend=backend))

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=[], **properties):
        headers = {} if headers is None else headers
        retry_policy = {} if retry_policy is None else retry_policy
        routing_key = self.routing_key if routing_key is None else routing_key
        compression = self.compression if compression is None else compression
        exchange = exchange or self.exchange

        callback = properties.pop('callback', None)
        task_id = body['id']

        if callback and not callable(callback):
            raise ValueError('callback should be callable')

        body, content_type, content_encoding = self._prepare(
            body, serializer, content_type, content_encoding,
            compression, headers)

        self.serializer = self.app.backend.serializer

        serialization.registry.enable(serializer)

        (self.content_type, self.content_encoding, self.encoder) = serialization.registry._encoders[self.serializer]

        conn = self.conn_pool.connection()
        publish = conn.publish

        if conn.connection.is_closed:
            raise MissingBrokerException('Broker Connection is Closed')
        publish_future = Future()

        conn.channel.confirm_delivery(partial(self.on_publish, task_id, publish_future, self.app.backend))

        result = publish(body, priority=priority, content_type=content_type,
                         content_encoding=content_encoding, headers=headers,
                         properties=properties, routing_key=routing_key,
                         mandatory=mandatory, immediate=immediate,
                         exchange=exchange, declare=declare)


        return publish_future

    def decode(self, payload):
        payload = is_py3k and payload or str(payload)
        return serialization.decode(payload,
                                    content_type=self.content_type,
                                    content_encoding=self.content_encoding)

    def on_result(self, callback, method, channel, deliver, reply):
        reply = self.decode(reply)
        callback(self.result_cls(**reply))

    def prepare_expires(self, value=None, type=None):
        if value is None:
            value = self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(value, timedelta):
            value = timeutils.timedelta_seconds(value)
        if value is not None and type:
            return type(value * 1000)
        return value

    def publish_task(self, task_name, task_args=None, task_kwargs=None,
                     countdown=None, eta=None, task_id=None, group_id=None,
                     taskset_id=None,  # compat alias to group_id
                     expires=None, exchange=None, exchange_type=None,
                     event_dispatcher=None, retry=None, retry_policy=None,
                     queue=None, now=None, retries=0, chord=None,
                     callbacks=None, errbacks=None, routing_key=None,
                     serializer=None, delivery_mode=None, compression=None,
                     reply_to=None, time_limit=None, soft_time_limit=None,
                     declare=None, headers=None,
                     send_before_publish=signals.before_task_publish.send,
                     before_receivers=signals.before_task_publish.receivers,
                     send_after_publish=signals.after_task_publish.send,
                     after_receivers=signals.after_task_publish.receivers,
                     send_task_sent=signals.task_sent.send,  # XXX deprecated
                     sent_receivers=signals.task_sent.receivers,
                     **kwargs):
        """Send task message."""
        retry = self.retry if retry is None else retry
        headers = {} if headers is None else headers

        qname = queue
        if queue is None and exchange is None:
            queue = self.default_queue
        if queue is not None:
            if isinstance(queue, string_t):
                qname, queue = queue, self.queues[queue]
            else:
                qname = queue.name
            exchange = exchange or queue.exchange.name
            routing_key = routing_key or queue.routing_key
        if declare is None and queue and not isinstance(queue, Broadcast):
            declare = [queue]
        if delivery_mode is None:
            delivery_mode = self._default_mode

        # merge default and custom policy
        retry = self.retry if retry is None else retry
        _rp = (dict(self.retry_policy, **retry_policy) if retry_policy
               else self.retry_policy)
        task_id = task_id or uuid()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        if not isinstance(task_args, (list, tuple)):
            raise ValueError('task args must be a list or tuple')
        if not isinstance(task_kwargs, dict):
            raise ValueError('task kwargs must be a dictionary')
        if countdown:  # Convert countdown to ETA.
            now = now or self.app.now()
            eta = now + timedelta(seconds=countdown)
            if self.utc:
                eta = to_utc(eta).astimezone(self.app.timezone)
        if isinstance(expires, numbers.Real):
            now = now or self.app.now()
            expires = now + timedelta(seconds=expires)
            if self.utc:
                expires = to_utc(expires).astimezone(self.app.timezone)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        body = {
            'task': task_name,
            'id': task_id,
            'args': task_args,
            'kwargs': task_kwargs,
            'retries': retries or 0,
            'eta': eta,
            'expires': expires,
            'utc': self.utc,
            'callbacks': callbacks,
            'errbacks': errbacks,
            'timelimit': (time_limit, soft_time_limit),
            'taskset': group_id or taskset_id,
            'chord': chord,
        }

        if before_receivers:
            send_before_publish(
                sender=task_name, body=body,
                exchange=exchange,
                routing_key=routing_key,
                declare=declare,
                headers=headers,
                properties=kwargs,
                retry_policy=retry_policy,
            )

        publish_future = self.publish(
            body,
            exchange=exchange, routing_key=routing_key,
            serializer=serializer or self.serializer,
            compression=compression or self.compression,
            headers=headers,
            retry=retry, retry_policy=_rp,
            reply_to=reply_to,
            correlation_id=task_id,
            delivery_mode=delivery_mode, declare=declare,
            **kwargs
        )

        if after_receivers:
            send_after_publish(sender=task_name, body=body,
                               exchange=exchange, routing_key=routing_key)

        if sent_receivers:  # XXX deprecated
            send_task_sent(sender=task_name, task_id=task_id,
                           task=task_name, args=task_args,
                           kwargs=task_kwargs, eta=eta,
                           taskset=group_id or taskset_id)
        if self.send_sent_event:
            evd = event_dispatcher or self.event_dispatcher
            exname = exchange or self.exchange
            if isinstance(exname, Exchange):
                exname = exname.name
            evd.publish(
                'task-sent',
                {
                    'uuid': task_id,
                    'name': task_name,
                    'args': safe_repr(task_args),
                    'kwargs': safe_repr(task_kwargs),
                    'retries': retries,
                    'eta': eta,
                    'expires': expires,
                    'queue': qname,
                    'exchange': exname,
                    'routing_key': routing_key,
                },
                self, retry=retry, retry_policy=retry_policy,
            )
        return publish_future

    def __repr__(self):
        return '<NonBlockingTaskProducer: {0.channel}>'.format(self)
