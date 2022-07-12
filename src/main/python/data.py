# encoding=utf-8

import json
import random
import time

from pykafka import KafkaClient

all = []

# 日访问量
a1 = {
    "type_id": 0,
    "user_id": 1001,
    "timestamp": int(time.time() * 1000)
}
row = 10
for i in range(1000, 1000 + row):
    a1.update({'user_id': random.randint(1000, 1005), "timestamp": int(time.time() * 1000 - 3 * 86400000)})
    all.append(a1.copy())
time.sleep(random.random())

# 当日注册
b1 = {
    "type_id": 1,
    "timestamp": int(time.time() * 1000)
}
row = 10
for i in range(0, 0 + row):
    b1.update({"timestamp": int(time.time() * 1000 - 2 * 86400000)})
    all.append(b1.copy())
time.sleep(random.random())
# 活跃度(文章显示次数)
c1 = {
    "type_id": 2,
    "timestamp": int(time.time()),
    "article_id": 4532
}
row = 10
for i in range(0, 0 + row):
    c1.update({'article_id': random.randint(1000, 1005), "timestamp": int(time.time() * 1000 - 2 * 86400000)})
    all.append(c1.copy())
time.sleep(random.random())
# 搜索
d1 = {
    "type_id": 3,
    "timestamp": int(time.time()),
    "search_content": "花二乔"
}
keywords = ["脂红", "虞姬艳装", "璎珞宝珠", "银红巧对", "十八号", "肉芙蓉", "胡红"]
row = 10
for i in range(0, 0 + row):
    d1.update({'search_content': random.choice(keywords), "timestamp": int(time.time() * 1000 - 2 * 86400000)})
    all.append(d1.copy())

if __name__ == '__main__':

    client = KafkaClient(hosts="lylg102:9092", zookeeper_hosts="lylg102:2181")
    topic = client.topics[b"first"]
    with topic.get_sync_producer() as producer:
        for data in all:
            _random_data = json.dumps(data)
            producer.produce(bytes(_random_data, encoding="utf-8"))
            print(_random_data)
            time.sleep(0.1)
