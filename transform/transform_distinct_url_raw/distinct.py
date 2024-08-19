

def cach_1(list_doc_raw, list_doc_clean):
    '''
    lấy 100 bản ghi gần nhất trong url raw
    lấy 100 bản ghi gần nhất trong url clean
    tạo 1 list url temp
    for để so sánh lấy url chưa tồn tại trong 100 bản ghi url clean
    '''
    list_data = []
    list_url_clean = []
    for doc in list_doc_clean:
        list_url_clean.append(doc["data"])
    for doc in list_doc_raw:
        if doc["data"] in list_url_clean:
            continue
        else:
            list_data.append(doc)
            list_url_clean.append(doc["data"])
    return list_data

def cach_2():
    '''
    trong url clean, đánh index cho trường website, url, time_crawl
    tìm kiếm theo 3 trường này, nếu chưa tồn tại thì insert
    '''
    pass