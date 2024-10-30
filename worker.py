import logging
import sys
import time
from typing import Any
from base import Worker
from config import config
from mrds import MyRedis, stop_event
from multiprocessing import Process, Lock
import uuid
import redis.exceptions

# Global variables for worker management
active_workers = 0
workers_lock = Lock()

class WcWorker(Worker):
    def run(self, **kwargs):
        rds: MyRedis = kwargs.get('rds')
        consumer_name = f"consumer-{uuid.uuid4()}" 
        
        with workers_lock:
            global active_workers
            active_workers += 1
            
        while True:
            try:
                result = rds.rds.xreadgroup(
                    streams={config["IN"]: ">"},
                    consumername=consumer_name,
                    groupname=config["GROUP"],
                    count=1,
                )
                if not result or len(result[0][1]) == 0:
                    break
                if self.crash:
                    logging.critical("CRASHING!")
                    sys.exit()
                if self.slow:
                    logging.critical("Sleeping!")
                    time.sleep(1)
                for stream, messages in result:
                    for message_id, message in messages:
                        fname = message.get(config["FNAME"])
                        word_counts = rds.extract_words_from_file(fname)
                        rds.process_file_atomic(consumer_name, message_id, word_counts)
                        
                # rds.reclaim_unacknowledged_messages(config["IN"], config["GROUP"], consumer_name, min_idle_time=3000)
            except redis.exceptions.BusyLoadingError:
                logging.error("Redis is loading the dataset in memory. Skipping current processing.")
                continue
            except redis.exceptions.ConnectionError:
                logging.error("Redis connection error. Skipping current processing.")
                continue
            except redis.exceptions.RedisError as e:
                logging.error(f"Unexpected Redis error: {e}")
                continue
            except Exception as e:
                logging.error(f"Unexpected error in worker: {e}")
                continue

        while True:
            try:
                pending_info = rds.rds.xpending(config["IN"], config["GROUP"])
                pending_count = pending_info['pending']
                if pending_count == 0:
                    logging.info("No more pending messages.")
                    break
                rds.reclaim_unacknowledged_messages(consumer_name, min_idle_time=3000)
            except redis.exceptions.BusyLoadingError:
                logging.error("Redis is loading the dataset in memory. Cannot check pending messages.")
            except redis.exceptions.ConnectionError:
                logging.error("Redis connection error. Cannot check pending messages.")
            except redis.exceptions.RedisError as e:
                logging.error(f"Unexpected Redis error: {e}")
            except Exception as e:
                logging.error(f"Unexpected error checking pending messages: {e}")
                
        with workers_lock:
            active_workers -= 1
            if active_workers == 0:
                stop_event.set()

    def create_and_run(self, **kwargs):
        process = Process(target=self.run, kwargs=kwargs)
        process.start()
            
            