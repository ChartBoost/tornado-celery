from __future__ import absolute_import

import celery
from celery.app import amqp

from tornado import ioloop

from .connection import ConnectionPool
from .producer import NonBlockingTaskProducer, MissingBrokerException
from .result import AsyncResult

VERSION = (0, 3, 5)
__version__ = '.'.join(map(str, VERSION)) + '-dev'


def setup_nonblocking_producer(celery_app=None, io_loop=None,
                               on_ready=None, result_cls=AsyncResult,
                               limit=1):
    celery_app = celery_app or celery.current_app
    io_loop = io_loop or ioloop.IOLoop.instance()

    NonBlockingTaskProducer.app = celery_app
    NonBlockingTaskProducer.conn_pool = ConnectionPool(limit, io_loop)
    NonBlockingTaskProducer.result_cls = result_cls
    if celery_app.conf['BROKER_URL'] and celery_app.conf['BROKER_URL'].startswith('amqp'):
        amqp.AMQP.producer_cls = NonBlockingTaskProducer

    def connect():
        broker_url = celery_app.connection().as_uri(include_password=True)
        options = celery_app.conf.get('CELERYT_PIKA_OPTIONS', {})
        NonBlockingTaskProducer.conn_pool.connect(broker_url,
                                                  options=options,
                                                  callback=on_ready)

    io_loop.add_callback(connect)
