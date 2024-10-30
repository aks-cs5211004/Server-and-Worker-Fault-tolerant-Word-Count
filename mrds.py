# import logging
# import subprocess
# from redis.client import Redis
# from typing import List, Tuple
# import json
# import time
# from config import config
# import pandas as pd
# from typing import Dict
# from collections import Counter
# from typing import Dict
# import multiprocessing
# import redis.exceptions

# stop_event = multiprocessing.Event()

# class MyRedis:
#     def __init__(self):
#         self.rds = Redis(host='localhost', port=6379, password=None, db=0, decode_responses=True)
#         self.rds.flushall()
#         self.rds.xgroup_create(config["IN"], config["GROUP"], id="$", mkstream=True)

#         self.lua_script = """
#         local stream_key = KEYS[1]
#         local group_name = ARGV[1]
#         local consumer_name = ARGV[2]
#         local message_id = ARGV[3]
#         local word_counts = cjson.decode(ARGV[4])

#         local ack_result = redis.call('XACK', stream_key, group_name, message_id)

#         if ack_result == 1 then
#             for word, count in pairs(word_counts) do
#                 redis.call('ZINCRBY', '""" + config["COUNT"] + """', count, word)
#             end
#             return "Success"
#         else
#             return "Failed to acknowledge message"
#         end
#         """
#         self.lua_sha = self.rds.script_load(self.lua_script)

#     def add_file(self, fname: str):
#         self.rds.xadd(config["IN"], {config["FNAME"]: fname})

#     def top(self, n: int, max_retries: int = 3, retry_delay: int = 5) -> List[Tuple[str, float]]:
#         attempt = 0
#         while attempt < max_retries:
#             try:
#                 return self.rds.zrevrangebyscore(config["COUNT"], '+inf', '-inf', 0, n, withscores=True)
#             except Exception:
#                 attempt += 1
#                 if attempt >= max_retries:
#                     # Return an empty list if maximum retries are reached
#                     return []
#                 # Wait before retrying
#                 sleep(retry_delay)
        
#     def is_pending(self) -> bool:
#         try:
#             pending = self.rds.xpending_range(config["IN"], config["GROUP"], min='-', max='+', count=10)
#             if pending:
#                 return True
#             return False
#         except Exception as e:
#             print(f"Error checking pending messages: {e}")
#             return False
        
#     def restart(self, downtime: int, retry_attempts: int = 3, retry_delay: int = 0.5):
#         try:
#             subprocess.run(['docker', 'stop', 'redis'], check=True)
#             logging.info("Redis container stopped.")
#             time.sleep(downtime)
#             subprocess.run(['docker', 'restart', 'redis'], check=True)
#             logging.info("Redis container started.")
            
#             for attempt in range(retry_attempts):
#                 try:
#                     self.lua_sha = self.rds.script_load(self.lua_script)
#                     logging.info("Lua script loaded successfully.")
#                     break
#                 except Exception as e:
#                     logging.error(f"Attempt {attempt + 1} of {retry_attempts} - Error loading Lua script: {e}")
#                     if attempt < retry_attempts - 1:
#                         time.sleep(retry_delay) 
#                     else:
#                         self.lua_sha = None 
#                         logging.error("All retry attempts failed. Lua script not loaded.")
            
#         except subprocess.CalledProcessError as e:
#             logging.error(f"Error stopping or starting Redis container: {e}")
            
#     def process_file_atomic(self, consumer_name: str, message_id: str, word_counts: Dict[str, int]):
#         try:
#             word_counts_json = json.dumps(word_counts)
#             result = self.rds.evalsha(self.lua_sha, 1, config["IN"], config["GROUP"], consumer_name, message_id, word_counts_json)
#             logging.info(f"Lua script execution result: {result}")
#             return result
#         except redis.exceptions.ResponseError as e:
#             logging.error(f"Redis error during atomic processing: {e}")
#             return None
#         except Exception as e:
#             logging.error(f"Error during atomic processing: {e}")
#             return None

#     def reclaim_unacknowledged_messages(self, consumer_name, min_idle_time):
#         try:
#             result = self.rds.xautoclaim(config["IN"], config["GROUP"], consumer_name, min_idle_time, start_id='0-0', count=1)
#             if result:
#                 reclaimed_messages = result[1]
#                 for message_id, message in reclaimed_messages:
#                     logging.info(f"AUTO Reclaimed message: {message_id} -> {message}")
#                     word_counts = self.extract_words_from_file(message[config['FNAME']])
#                     self.process_file_atomic(consumer_name, message_id, word_counts)
#             return result
#         except Exception as e:
#             logging.error(f"Error during XAUTOCLAIM: {e}")
#             return None

#     def extract_words_from_file(self, fname: str) -> Dict[str, int]:
#         wc = {}
#         df = pd.read_csv(fname, lineterminator='\n')
#         df["text"] = df["text"].astype(str)
#         for text in df["text"]:
#             if text == '\n':
#                 continue
#             for word in text.split(" "):
#                 if word not in wc:
#                     wc[word] = 0
#                 wc[word] += 1
#         return wc
    
    
    


import logging
import subprocess
from redis.client import Redis
from typing import List, Tuple, Dict
import time
from config import config
import pandas as pd
import multiprocessing
import redis.exceptions

stop_event = multiprocessing.Event()

class MyRedis:
    def __init__(self):
        self.rds = Redis(host='localhost', port=6379, password=None, db=0, decode_responses=True)
        self.rds.flushall()
        self.rds.xgroup_create(config["IN"], config["GROUP"], id="$", mkstream=True)

        # Updated Lua script to accept word counts as a table
        self.lua_script = """
        local stream_key = KEYS[1]
        local group_name = ARGV[1]
        local consumer_name = ARGV[2]
        local message_id = ARGV[3]

        local word_counts = {}

        for i = 4, #ARGV, 2 do
            local word = ARGV[i]
            local count = tonumber(ARGV[i+1])
            word_counts[word] = count
        end

        local ack_result = redis.call('XACK', stream_key, group_name, message_id)

        if ack_result == 1 then
            for word, count in pairs(word_counts) do
                redis.call('ZINCRBY', '""" + config["COUNT"] + """', count, word)
            end
            return "Success"
        else
            return "Failed to acknowledge message"
        end
        """
        self.lua_sha = self.rds.script_load(self.lua_script)
        
    def add_file(self, fname: str):
        self.rds.xadd(config["IN"], {config["FNAME"]: fname})

    def top(self, n: int, max_retries: int = 3, retry_delay: int = 5) -> List[Tuple[str, float]]:
        attempt = 0
        while attempt < max_retries:
            try:
                return self.rds.zrevrangebyscore(config["COUNT"], '+inf', '-inf', 0, n, withscores=True)
            except Exception:
                attempt += 1
                if attempt >= max_retries:
                    # Return an empty list if maximum retries are reached
                    return []
                # Wait before retrying
                sleep(retry_delay)
        
    def is_pending(self) -> bool:
        try:
            pending = self.rds.xpending_range(config["IN"], config["GROUP"], min='-', max='+', count=10)
            if pending:
                return True
            return False
        except Exception as e:
            print(f"Error checking pending messages: {e}")
            return False
        
    def restart(self, downtime: int, retry_attempts: int = 3, retry_delay: int = 0.5):
        try:
            subprocess.run(['docker', 'stop', 'redis'], check=True)
            logging.info("Redis container stopped.")
            time.sleep(downtime)
            subprocess.run(['docker', 'restart', 'redis'], check=True)
            logging.info("Redis container started.")
            
            for attempt in range(retry_attempts):
                try:
                    self.lua_sha = self.rds.script_load(self.lua_script)
                    logging.info("Lua script loaded successfully.")
                    break
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} of {retry_attempts} - Error loading Lua script: {e}")
                    if attempt < retry_attempts - 1:
                        time.sleep(retry_delay) 
                    else:
                        self.lua_sha = None 
                        logging.error("All retry attempts failed. Lua script not loaded.")
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error stopping or starting Redis container: {e}")

            
    def process_file_atomic(self, consumer_name: str, message_id: str, word_counts: Dict[str, int]):
        try:
            # Flatten the word_counts dictionary into a list of arguments
            lua_args = [config["IN"], config["GROUP"], consumer_name, message_id]
            for word, count in word_counts.items():
                lua_args.append(word)
                lua_args.append(str(count))

            result = self.rds.evalsha(self.lua_sha, 1, *lua_args)
            logging.info(f"Lua script execution result: {result}")
            return result
        except redis.exceptions.ResponseError as e:
            logging.error(f"Redis error during atomic processing: {e}")
            return None
        except Exception as e:
            logging.error(f"Error during atomic processing: {e}")
            return None

    def reclaim_unacknowledged_messages(self, consumer_name, min_idle_time):
        try:
            result = self.rds.xautoclaim(config["IN"], config["GROUP"], consumer_name, min_idle_time, start_id='0-0', count=1)
            if result:
                reclaimed_messages = result[1]
                for message_id, message in reclaimed_messages:
                    logging.info(f"AUTO Reclaimed message: {message_id} -> {message}")
                    word_counts = self.extract_words_from_file(message[config['FNAME']])
                    self.process_file_atomic(consumer_name, message_id, word_counts)
            return result
        except Exception as e:
            logging.error(f"Error during XAUTOCLAIM: {e}")
            return None

    def extract_words_from_file(self, fname: str) -> Dict[str, int]:
        wc = {}
        df = pd.read_csv(fname, lineterminator='\n')
        df["text"] = df["text"].astype(str)
        for text in df["text"]:
            if text == '\n':
                continue
            for word in text.split(" "):
                if word not in wc:
                    wc[word] = 0
                wc[word] += 1
        return wc
