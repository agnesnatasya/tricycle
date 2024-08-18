import contextvars
from threading import Thread
import trio
from tricycle._tree_var import TreeVar
import time

from threading import Thread, local
# CASE 1: Multiple sync threads that run trio inside
cv_1 = TreeVar("cv_1")

def thread_task(user):
    data = local()

    cv_1.set(user)
    async def async_fn():
        print(cv_1.get())
        # Sleep to force context switch
        time.sleep(0.1)
        print(cv_1.get(), user)
        assert cv_1.get() == user

    trio.run(async_fn)

Thread(target=thread_task, args=("koby",)).start()
Thread(target=thread_task, args=("world",)).start()

# CASE 2: Multiple threads in single trio run 
cv_2 = TreeVar("cv_2")
import random
value_set_initially = 20
async def main():
    def thread_task(value_for_nursery):
        # print(trio.lowlevel.current_trio_token())
        print(trio.from_thread.run_sync(cv_2.get), value_for_nursery, cv_2.get())
        assert cv_2.get() == value_for_nursery
        value_for_thread = random.randint(1, 10)
        cv_2.set(value_for_thread)
        time.sleep(0.1)
        cv_2.get() == value_for_thread 

    async def nursery_task(value_for_nursery):
        assert cv_2.get() == value_set_initially
        cv_2.set(value_for_nursery)
        await trio.to_thread.run_sync(thread_task, value_for_nursery)
        await trio.sleep(0)
        cv_2.get() == value_for_nursery

    assert cv_2.get() == value_set_initially

    async with trio.open_nursery() as nursery:
        nursery.start_soon(nursery_task, 1)
        nursery.start_soon(nursery_task, 2)

cv_2.set(value_set_initially)
trio.run(main)


# CASE 3: Multiple threads with multiple threads from trio run
cv_3 = TreeVar("cv_3", default =20)
import random
# value_set_initially = 20
async def main():
    def trio_thread_task(value_for_nursery):
        assert cv_3.get() == value_for_nursery
        value_for_thread = random.randint(1, 10)
        cv_3.set(value_for_thread)
        time.sleep(0.1)
        cv_3.get() == value_for_thread 

    async def nursery_task(value_for_nursery):
        assert cv_3.get() == value_set_initially
        cv_3.set(value_for_nursery)
        await trio.to_thread.run_sync(trio_thread_task, value_for_nursery)
        await trio.sleep(0)
        cv_3.get() == value_for_nursery

    assert cv_3.get() == value_set_initially

    async with trio.open_nursery() as nursery:
        nursery.start_soon(nursery_task, 1)
        nursery.start_soon(nursery_task, 2)

# cv_3.set(value_set_initially)
def thread_task(arg):
    try:
        cv_2.get() 
    except Exception:
        pass
    else:
        raise RuntimeError("get doesnt raise")
    
    def trio_thread_task(value_for_thread):
        print(cv_3.get())
        assert cv_3.get() == value_for_thread
    #     value_for_thread = random.randint(1, 10)
    #     cv_3.set(value_for_thread)
    #     time.sleep(0.1)
    #     cv_3.get() == value_for_thread 

    value_for_thread = random.randint(1, 10)
    cv_3.set(value_for_thread)
    async def main():
        # assert cv_3.get() == value_for_thread
        await trio.to_thread.run_sync(trio_thread_task, value_for_thread)
    trio.run(main)
Thread(target=thread_task, args=(1,)).start()
Thread(target=thread_task, args=(2,)).start()

# CASE 4: Multiple threads spawning multiple thread
cv_4 = TreeVar("cv_4")

def thread_task(user):
    cv_4.set(user)
    async def async_fn():
        print(cv_4.get())
        # Sleep to force context switch
        time.sleep(0.1)
        assert cv_4.get() == user
        print(cv_4.get())
        def thread_task_inner(user):
            try:
                cv_4.get()
            except LookupError:
                pass
            else:
                raise
        Thread(target=thread_task_inner, args=("koby",)).start()
        Thread(target=thread_task_inner, args=("world",)).start()

    trio.run(async_fn)

Thread(target=thread_task, args=("koby",)).start()
Thread(target=thread_task, args=("world",)).start()


# CASE 5: thread -> trio thread -> thread
cv_5 = TreeVar("cv_5")
def thread_task(arg):
    def trio_thread_task(value_for_thread):
        assert cv_3.get() == value_for_thread

        def thread_task_inside_trio():
            try:
                cv_5.get()
            except LookupError:
                pass
            else:
                raise

        Thread(target=thread_task_inside_trio).start()
        Thread(target=thread_task_inside_trio).start()

    value_for_thread = random.randint(1, 10)
    cv_3.set(value_for_thread)
    async def main():
        # assert cv_3.get() == value_for_thread
        await trio.to_thread.run_sync(trio_thread_task, value_for_thread)
    trio.run(main)

Thread(target=thread_task, args=(1,)).start()
Thread(target=thread_task, args=(2,)).start()
