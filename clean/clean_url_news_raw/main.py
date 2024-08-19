'''
Kiểm tra Database để lọc bản ghi có thời gian crawl lớn hơn 48h để xoá bỏ.
Mục đích loại bỏ các bản ghi quá cũ, giải phóng dung lượng Database.
Chạy lúc 23h45 chủ nhật mỗi tuần.
'''

import logging
import pymongo
import datetime

import _env


def config_log():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # dinh dang log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # set file luu log
    file_handler = logging.FileHandler(_env.PATH_LOG_FILE)
    file_handler.setFormatter(formatter)

    # Loại bỏ tất cả các handler ghi ra console
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            logger.removeHandler(handler)

    # them file handler vao logger
    logger.addHandler(file_handler)

def main():
    # config log
    config_log()
    logger = logging.getLogger("Main")
    logger.info("__________________________________Run main")

    # connect database
    client = pymongo.MongoClient(_env.DB_CONNECTION_STRING)
    logger.info(f"Connect database {_env.DB_NAME}")
    db = client[_env.DB_NAME]
    logger.info(f"Connect collection {_env.COL_NAME}")
    col = db[_env.COL_NAME]

    # query delete data
    time_now = datetime.datetime.now()
    time_threshold = time_now - datetime.timedelta(hours=48)
    try:
        result = col.delete_many({
                "time_crawl": {
                    "$lt": time_threshold
                }
            }
        )
        logger.info(f"Delete {result.deleted_count} docs before {time_threshold}")
    except Exception as e:
        logger.error(f"Exception when delete docs before {time_threshold}: \n{e}")
