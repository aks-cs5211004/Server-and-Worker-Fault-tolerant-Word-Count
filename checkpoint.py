import multiprocessing
import time
import redis
from mrds import MyRedis, stop_event

def checkpoint_thread(rds: MyRedis, interval: int):
    while not stop_event.is_set():
        try:
            # Check if Redis is available before creating a checkpoint
            if rds.rds.ping():
                rds.rds.bgsave()
                print("Checkpoint created")
            else:
                print("Redis is not available. Skipping checkpoint.")
        except redis.exceptions.RedisError as e:
            print(f"Error creating checkpoint: {e}")
        except redis.exceptions.ConnectionError:
            print("Redis connection error. Skipping checkpoint.")
        time.sleep(interval)

def create_checkpoints(rds: MyRedis, interval: int):
    # Start the checkpoint process
    process = multiprocessing.Process(target=checkpoint_thread, args=(rds, interval))
    process.start()
    print("Checkpoint process started")

