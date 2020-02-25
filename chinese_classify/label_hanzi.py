import concurrent.futures
import string
import logging

import redis

from ydm import YDMHttp

redis_conn = redis.StrictRedis()
logger = logging.getLogger("label")


def login(username, password, appid, appkey):
    client = YDMHttp(username, password, appid, appkey)
    uid = client.login()
    logger.info('uid: %s' % uid)
    balance = client.balance()
    logger.info('balance: %s' % balance)
    return client


def worker(file_path, client):
    with open(file_path, 'r') as f:
        for line in f:
            path = line.strip()
            if redis_conn.sismember('path', path):
                continue
            else:
                redis_conn.sadd('path', path)
            try:
                cid, result = client.decode(codetype=2001, filename=path, timeout=20)

                logger.info('cid: %s, result: %s' % (cid, result))
            except Exception as e:
                logger.warning(e)
            else:
                if (result == '看不清' or len(result) != 1 or result in string.ascii_lowercase) and cid != -3003:
                    try:
                        yundama.report(cid)
                    except Exception as e:
                        print(e)
                else:
                    redis_conn.hset("result", path, result)


def batch(files):
    with concurrent.futures.ThreadPoolExecutor(max_workers=files) as executor:
        future_to_file = {executor.submit(worker, file_path): file_path for file_path in files}
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                future.result()
            except Exception as exc:
                logger.warning(exc, exc_info=True)
            else:
                logger.info("complete task: %s" % file_path)