import random
from threading import Thread, local
import contextvars
import trio
import time
cv_1 = contextvars.ContextVar("cv_1")
data = local()

def thread_task(user):
    data.x = random.randint(1, 10)
    cv_1.set(user)
    async def async_fn():
        print(data.x)
        def trio_thread_task(my_parent_data):
            data.x = random.randint(1, 10)
            trio.from_thread.run_sync(lambda: data.x)
            print("At trio thread", data.x, "At normal thread", my_parent_data)
        await trio.to_thread.run_sync(trio_thread_task, data.x)
        # Sleep to force context switch
        time.sleep(0.1)
        assert cv_1.get() == user
        print(cv_1.get())

    trio.run(async_fn)

Thread(target=thread_task, args=("koby",)).start()
Thread(target=thread_task, args=("world",)).start()
