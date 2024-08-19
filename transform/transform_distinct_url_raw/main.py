
import json
import logging
import pymongo

import _env, distinct, transform


def config_log(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    my_handler = logging.FileHandler(_env.PATH_LOG_FILE)
    my_handler.setFormatter(format)
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            logger.removeHandler(handler)
    logger.addHandler(my_handler)
    return logger

def read_config(logger):
    try:
        with open(_env.PATH_CONFIG, "r", encoding="utf-8") as open_file:
            logger.info(f"Doc config")
            return json.load(open_file)
    except Exception as e:
        logger.error(f"Doc config khong thanh cong:\n{e}")
    
def connect_db(logger, connection_string, db_name):
    try:
        client = pymongo.MongoClient(connection_string)
        db = client[db_name]
        logger.info(f"Ket noi toi database {db_name}")
        return db
    except Exception as e:
        logger.error(f"Ket noi toi database {db_name} khong thanh cong:\n{e}")

def connect_col(logger, db, col_name):
    try:
        col = db[col_name]
        logger.info(f"Ket noi toi collection {col_name}")
        return col
    except Exception as e:
        logger.info(f"Ket noi toi collection {col_name} khong thanh cong:\n{e}")     

def main():
    logger = config_log("Main")
    logger.info("__________________________________Run main")

    # ket noi database
    db = connect_db(logger, _env.DB_CONNECTION_STRING, _env.DB_NAME)
    col_raw = connect_col(logger, db, _env.COL_RAW_NAME)
    col_clean = connect_col(logger, db, _env.COL_CLEAN_NAME)

    # doc config
    config = read_config(logger)

    # lap qua tung config
    for obj in config:
        logger.info(f"kiem tra url crawl {obj['url_crawl']}")
        try:
            # select 100 ban ghi trong urls raw
            list_doc_raw = col_raw.find(
                {
                    "url_crawl": obj["url_crawl"],
                }
            ).sort("time_crawl", -1).limit(100)
            
            # select 100 ban ghi trong url clean
            list_doc_clean = col_clean.find(
                {
                    "url_crawl": obj["url_crawl"]
                }
            ).sort("time_crawl", -1).limit(100)
            
            list_data = distinct.cach_1(list_doc_raw, list_doc_clean)
            list_data = transform.make_url(list_data, obj)
            if not list_data:
                logger.info(f"Url crawl {obj['url_crawl']} khong co url nao moi")
            else:
                result = col_clean.insert_many(list_data)
                logger.info(f"Them {len(result.inserted_ids)} ban ghi tu url crawl {obj['url_crawl']} vao collection {_env.COL_CLEAN_NAME}")

        except Exception as e:
            logger.error(f"Query fail:\n{e}")
