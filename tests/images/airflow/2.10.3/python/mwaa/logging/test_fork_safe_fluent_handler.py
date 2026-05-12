"""Tests for ForkSafeFluentSender and ForkSafeFluentHandler.

These tests verify that the fork-safety mechanism works correctly:
- After os.register_at_fork fires _reinit_after_fork in a child process,
  all thread-unsafe state (locks, queue, socket) is reset so the child
  doesn't deadlock on inherited locked mutexes.
"""

import gc
import os
import signal
import threading
import time
import weakref
from unittest.mock import Mock

import pytest

# Note: ForkSafeFluentHandler and ForkSafeFluentSender are imported inside
# fixtures and test methods rather than at module level. This is because the
# test suite uses an autouse fixture that reloads the cloudwatch_handlers module,
# which creates new class objects. Top-level imports would hold references to
# the original (pre-reload) classes, causing `is` and `isinstance` checks to fail.


@pytest.fixture
def sender():
    """Create a ForkSafeFluentSender and ensure cleanup (stops background thread)."""
    from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
    s = ForkSafeFluentSender('test', host='localhost', port=24224)
    yield s
    s.close()


@pytest.fixture
def handler():
    """Create a ForkSafeFluentHandler and ensure cleanup."""
    from mwaa.logging.fork_safe_handler import ForkSafeFluentHandler
    h = ForkSafeFluentHandler('test', host='localhost', port=24224)
    yield h
    h.close()


class TestForkSafeFluentSender:
    """Tests for ForkSafeFluentSender fork-safety behavior.

    Each test simulates what happens in a child process after fork by
    calling _reinit_after_fork() directly, then verifying the sender
    is in a safe, non-deadlocking state.
    """

    def test_reinit_after_fork_resets_lock(self, sender):
        """Verify that a locked lock becomes acquirable after reinit.

        Simulates the scenario where fork() happened while a thread held
        sender.lock. In the child, _reinit_after_fork replaces it with a
        fresh unlocked Lock so subsequent code won't deadlock.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        sender.lock.acquire()

        ForkSafeFluentSender._reinit_after_fork(weakref.ref(sender))

        assert sender.lock.acquire(blocking=False), "Lock should be acquirable after reinit"
        sender.lock.release()

    def test_reinit_after_fork_marks_closed(self, sender):
        """Verify sender is marked closed so _send() becomes a no-op.

        The _send_loop thread doesn't survive fork, so the sender can never
        deliver anything. _closed=True makes _send() return False immediately.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        ForkSafeFluentSender._reinit_after_fork(weakref.ref(sender))

        assert sender._closed is True

    def test_reinit_after_fork_replaces_queue(self, sender):
        """Verify the queue is replaced with a fresh empty one.

        Python's Queue uses threading.Lock internally (Queue.mutex).
        If the _send_loop thread held Queue.mutex at fork time, the child
        inherits it locked. Replacing the Queue avoids this.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        sender._queue.put(b'test data', block=False)

        ForkSafeFluentSender._reinit_after_fork(weakref.ref(sender))

        assert sender._queue.empty(), "Queue should be empty after reinit"

    def test_reinit_after_fork_closes_socket(self, sender):
        """Verify the inherited TCP socket is closed.

        Prevents the child from sharing a TCP connection with the parent,
        which would corrupt data on the wire (interleaved writes).
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        sender.socket = Mock()

        ForkSafeFluentSender._reinit_after_fork(weakref.ref(sender))

        assert sender.socket is None

    def test_send_returns_false_after_reinit(self, sender):
        """Verify _send() is a no-op after reinit (returns False).

        Confirms that if any code in the child accidentally calls emit()
        on the inherited handler before set_context() replaces it, the
        message is silently dropped instead of accumulating in a dead queue.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        ForkSafeFluentSender._reinit_after_fork(weakref.ref(sender))

        assert sender._send(b'test') is False

    def test_reinit_with_dead_weakref(self):
        """Verify reinit is a no-op when the sender has been garbage collected.

        os.register_at_fork callbacks are never unregistered, so the callback
        may fire after the sender is GC'd. Using weakref ensures this case
        is handled gracefully (no crash, no action).
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        sender = ForkSafeFluentSender('test', host='localhost', port=24224)
        ref = weakref.ref(sender)
        sender.close()
        del sender
        gc.collect()

        # Should not raise
        ForkSafeFluentSender._reinit_after_fork(ref)

    def test_close_after_reinit_does_not_deadlock(self, sender):
        """Verify close() completes after reinit (no deadlock on lock).

        asyncsender.close() does 'with self.lock:' - if the lock were still
        in its inherited locked state, this would deadlock. After reinit,
        the lock is fresh and unlocked, so close() completes normally.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        ForkSafeFluentSender._reinit_after_fork(weakref.ref(sender))

        completed = []

        def try_close():
            sender.close()
            completed.append(True)

        t = threading.Thread(target=try_close)
        t.start()
        t.join(timeout=2)

        assert completed, "close() deadlocked after fork reinit"


class TestForkSafeFluentHandler:
    """Tests for ForkSafeFluentHandler wiring."""

    def test_uses_fork_safe_sender_class(self, handler):
        """Verify getSenderClass() returns ForkSafeFluentSender.

        This is the extension point that makes the handler create our
        fork-safe sender instead of the default asyncsender.FluentSender.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        assert handler.getSenderClass() is ForkSafeFluentSender

    def test_sender_is_fork_safe_instance(self, handler):
        """Verify the lazily-created sender is a ForkSafeFluentSender."""
        from mwaa.logging.fork_safe_handler import ForkSafeFluentSender
        assert isinstance(handler.sender, ForkSafeFluentSender)


class TestForkSafetyIntegration:
    """Integration test using actual fork()."""

    def test_child_can_close_inherited_handler(self):
        """Verify a forked child can close an inherited handler without deadlocking.

        This is a smoke test. The real deadlock only occurs when a thread holds
        the sender lock at the exact moment of fork (a race condition that is not
        deterministically reproducible). This test verifies the basic machinery:
        os.register_at_fork fires, _reinit_after_fork runs, and close() completes.
        """
        from mwaa.logging.fork_safe_handler import ForkSafeFluentHandler
        handler = ForkSafeFluentHandler('test', host='localhost', port=24224)
        # Access sender to ensure it is fully initialized (thread + lock + queue)
        _ = handler.sender

        pid = os.fork()
        if pid == 0:
            # Child: close the inherited handler. Without the fix, this could
            # deadlock if the sender lock was held at fork time.
            try:
                handler.close()
                os._exit(0)
            except Exception:
                os._exit(1)
        else:
            # Parent: wait for child with timeout to detect deadlock
            start = time.time()
            while time.time() - start < 5:
                result = os.waitpid(pid, os.WNOHANG)
                if result[0] != 0:
                    break
                time.sleep(0.1)
            else:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                pytest.fail("Child process deadlocked on close() after fork")

            assert os.WIFEXITED(result[1]) and os.WEXITSTATUS(result[1]) == 0, \
                "Child process exited with error"

        handler.close()

