# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
import numpy as np
import json
import time
from bs4 import BeautifulSoup
from code.demo import Simhash, SimhashIndex


def get_article():
    """
    获取待测试文章
    :return:
    """
    with open('/Users/cmy/Desktop/work/secondweek/data/article.txt') as f:
        line = f.readline()
    soup = BeautifulSoup(line, "html5lib")
    j_text = json.loads(soup.text)
    desc = j_text['param']['article']['description']
    return desc


def articles_library():
    """
    从数据库中获取文章
    :return:
    """
    conn = pymysql.connect(host='192.168.0.233', port=3306, user='reader', passwd='', db='gene')
    cur = conn.cursor()
    sql = 'select id,description from t_article limit 500'
    cur.execute(sql)
    articles = cur.fetchall()
    articles = dict(articles)
    return articles


def simhash_per_article(articles):
    """
    计算数据中每篇文章的simhash，并和相应的id一起放入列表
    :param articles: 数据库中的文章
    :return: 返回ID，simhash列表
    """
    L = []
    for id, content in articles.items():
        soup = BeautifulSoup(content,"html5lib")
        simhash = Simhash(soup.text)
        L.append([id, simhash.value])
    return L


if __name__ == '__main__':
    # 获取数据库中的文章
    articles = articles_library()
    # 计算每篇文章的simhash
    articles_simhash_list = simhash_per_article(articles)
    print(articles_simhash_list)
    # 获取当前待入库文章
    content = get_article()
    moment_simhash = Simhash(content)
    print('当前待入库文章的simhash值：%s' % moment_simhash.value)
    # 当前待入库文章与数据库中每篇文章的距离
    distance_list = []
    for id, desc in articles.items():
        soup = BeautifulSoup(desc, "html5lib")
        simhash = Simhash(soup.text)
        distance = simhash.distance(moment_simhash)
        distance_list.append([id, simhash.value, distance])
    print(distance_list)
    # 满足距离小于K的文章ID列表
    objs = [(str(id), Simhash(BeautifulSoup(desc, "html5lib").text)) for id, desc in articles.items()]
    simhashindex = SimhashIndex(objs, k=20)
    L = simhashindex.get_near_dups(moment_simhash)
    print(L)
