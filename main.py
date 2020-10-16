from pymongo import MongoClient
from fastapi import FastAPI

from settings import *

app = FastAPI()
client = MongoClient('mongodb://%s:%d' % (MONGODB_HOST, MONGODB_PORT),
                     username=MONGODB_USERNAME,
                     password=MONGODB_PASSWORD)


@app.get("/")
async def get_records(skip: int = 0, limit: int = 50, query: str = ""):
    with client:
        db = client.common_crawl
        articles = db.wet_articles.find({'WARC-Target-URI': {
            "$exists": True, "$not": {
                "$size": 0
            }
        }}).skip(skip).limit(limit)
        # return map(lambda x: (x['_id'], x['WARC-Target-URI']), articles)
        articles = list(map(lambda x: {"id:": str(x['_id']), "url": x['WARC-Target-URI']}, articles))
        return {"articles": articles}


@app.get("/random")
async def get_random_record(size: int = 1):
    if size < 1:
        size = 1
    elif size > 20:
        size = 20

    with client:
        db = client.common_crawl
        articles = db.wet_articles.aggregate([{"$sample": {"size": size}}])
        articles = list(map(lambda x: {"id:": str(x['_id']), "url": x['WARC-Target-URI']}, articles))
        return {"articles": articles}
