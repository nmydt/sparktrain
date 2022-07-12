import random

dic = [{'key': '001', 'addres': 'guangzhou', 'age': 20, 'username': 'alex'},
       {'key': '002', 'addres': 'shenzhen', 'age': 34, 'username': 'jack'},
       {'key': '003', 'addres': 'beijing', 'age': 23, 'username': 'lili'},
       ]


def getdata(name):
    return random.choice(dic)
