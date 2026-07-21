"""Fork-safe FluentHandler and FluentSender for use with Celery prefork.

This module is intentionally separate from cloudwatch_handlers.py so that
mocking in tests survives module reloads (same pattern as fluent_handler.FluentHandler).
"""

import os
import threading
import weakref
from queue import Queue

from fluent import asynchandler as fluent_handler
from fluent import asyncsender


class ForkSafeFluentSender(asyncsender.FluentSender):
    """
    A FluentSender subclass that is safe to inherit across fork().

    The base FluentSender uses threading.Lock and Queue (which also uses
    threading.Lock internally). These locks are not fork-safe: if a thread
    holds one at fork time, the child inherits it locked with no thread to
    release it, causing a permanent deadlock on futex.

    This subclass registers an os.register_at_fork() callback that
    reinitializes these locks in the child process after fork. This follows
    the same pattern Python's logging.Handler uses to make its RLock
    fork-safe (see CPython logging/__init__.py _register_at_fork_reinit_lock).
    """

    def __init__(self, *args, **kwargs):
        """Initialize the ForkSafeFluentSender with fork-safe callbacks."""
        super().__init__(*args, **kwargs)
        # Use a weakref so this callback doesn't prevent the sender from being
        # garbage collected. os.register_at_fork callbacks are never unregistered,
        # so a strong reference would keep the sender alive forever.
        weak_self = weakref.ref(self)
        # Register a callback that Python will invoke in the child process
        # immediately after fork(), before any user code runs. This gives us
        # a chance to reset inherited thread-unsafe state.
        os.register_at_fork(
            after_in_child=lambda: ForkSafeFluentSender._reinit_after_fork(weak_self)
        )

    @staticmethod
    def _reinit_after_fork(weak_self):
        self = weak_self()
        if self is None:
            return
        # 1. Replace self.lock (threading.Lock): The log-emitting thread may
        #    have held this lock at fork time. The child inherits it locked,
        #    but the owning thread doesn't exist in the child.
        self.lock = threading.Lock()
        # 2. Replace self._queue (Queue): Python's Queue uses threading.Lock
        #    internally (Queue.mutex). The _send_loop thread holds this during
        #    get(). Same deadlock risk as self.lock.
        self._queue = Queue(maxsize=self._queue_maxsize)
        # 3. Mark closed: The _send_loop thread doesn't survive fork, so this
        #    sender can never deliver anything. This makes _send() return False
        #    immediately instead of accumulating data in a queue nothing drains.
        self._closed = True
        # 4. Close inherited socket: Prevents the child from sharing a TCP
        #    connection with the parent, which would corrupt data on the wire.
        self._close()


class ForkSafeFluentHandler(fluent_handler.FluentHandler):
    """A FluentHandler that uses ForkSafeFluentSender for fork-safety.

    Also enforces strictly increasing millisecond timestamps per handler:
    CloudWatch does not guarantee display order for same-millisecond events,
    so tied timestamps are nudged forward to make ordering deterministic.
    """

    def getSenderClass(self):
        """Return the ForkSafeFluentSender class for fork-safe logging."""
        return ForkSafeFluentSender

    def emit(self, record):
        """Emit a record, enforcing a strictly increasing ms timestamp."""
        # Compute ms with the same arithmetic as the wire encoding: fluent's
        # EventTime packs seconds plus the fractional second as nanoseconds
        # (int((created % 1) * 1e9)), which Fluent Bit floors to milliseconds
        # for CloudWatch. A plain int(record.created * 1000) can disagree with
        # that by 1ms near boundaries, letting same-millisecond ties through
        # the check below.
        ms = int(record.created) * 1000 + int((record.created % 1) * 1e9) // 1_000_000
        last_ms = getattr(self, "_last_emitted_ms", 0)
        if ms <= last_ms:
            ms = last_ms + 1
            # Mid-ms bias: a bare ms/1000.0 can floor back into the previous
            # millisecond after EventTime float reconstruction.
            record.created = (ms + 0.5) / 1000.0
            record.msecs = (record.created - int(record.created)) * 1000
        self._last_emitted_ms = ms
        return super().emit(record)
