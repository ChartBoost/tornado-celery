from __future__ import absolute_import

import celery

from tornado import web
from tornado.options import define, options

from . import handlers as _ # noqa
from celery import uuid
from celery._state import get_current_worker_task
from celery.utils.functional import maybe_evaluate, maybe_list
from .utils import route
from tornado.concurrent import Future


define("debug", type=bool, default=False, help="run in debug mode")

class AsyncCelery(celery.Celery):

    def send_task(self, name, args=None, kwargs=None, countdown=None,
                  eta=None, task_id=None, producer=None, connection=None,
                  router=None, result_cls=None, expires=None,
                  publisher=None, link=None, link_error=None,
                  add_to_parent=True, reply_to=None, **options):
        task_id = task_id or uuid()
        producer = producer or publisher  # XXX compat
        router = router or self.amqp.router
        conf = self.conf

        options = router.route(options, name, args, kwargs)
        if connection:
            producer = self.amqp.TaskProducer(connection)
        with self.producer_or_acquire(producer) as P:
            self.backend.on_task_call(P, task_id)
            publish_future = P.publish_task(
                name, args, kwargs, countdown=countdown, eta=eta,
                task_id=task_id, expires=expires,
                callbacks=maybe_list(link), errbacks=maybe_list(link_error),
                reply_to=reply_to or self.oid, **options
            )
        if add_to_parent:
            parent = get_current_worker_task()
            if parent:
                parent.add_trail(result)
        return result


class Application(web.Application):
    def __init__(self, celery_app=None):
        handlers = route.get_routes()
        settings = dict(debug=options.debug)
        super(Application, self).__init__(handlers, **settings)
        self.celery_app = celery_app or celery.Celery()