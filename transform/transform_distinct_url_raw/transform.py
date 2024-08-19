

def make_url(list_docs, config):
    '''
    hàm tạo url đầy đủ nếu cần thiết
    format bản ghi đúng định dạng
    '''
    list_data = []
    if config["type"] == 1: # url đã đầy đủ 
        for doc in list_docs:
            list_data.append(
                {
                    "website": doc["website"],
                    "url_crawl": doc["url_crawl"],
                    "data": doc["data"],
                    "url": doc["data"],
                    "time_crawl": doc["time_crawl"],
                    "time_crawl_detail": None,
                    "numb_crawl_detail": 0
                }
            )
        return list_data
    
    elif config["type"] == 2: # url khuyết tên miền, cộng thêm tên miền
        for doc in list_docs:
            list_data.append(
                {
                    "website": doc["website"],
                    "url_crawl": doc["url_crawl"],
                    "data": doc["data"],
                    "url": doc["website"] + doc["data"],
                    "time_crawl": doc["time_crawl"],
                    "time_crawl_detail": None,
                    "numb_crawl_detail": 0 
                }
            )
        return list_data

    elif config["type"] == 3: # url khuyết thông tin đặc biệt, điền theo config
        pass