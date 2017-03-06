# coding=utf-8
import requests, re, Queue, threading
from requests.exceptions import SSLError, ConnectionError
from bs4 import BeautifulSoup
from fake_useragent import FakeUserAgent
from mongoengine import *
import hashlib
from spider_proxy import Proxy, get_proxy_random
from lxml import etree

connect('newmovie')

def user_agent():
    ua = FakeUserAgent()
    return ua.random

IMG_DIR = 'E:\\newimg'
HEADERS = {
'User-Agent':user_agent(),
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding': 'gzip, deflate',
}


# 保存电影粗略信息
class Movie(Document):
    title = StringField()
    _id = StringField(unique=True)  # 电影的后几位做id
    score = StringField()
    vote_count = IntField()
    rank = StringField
    cover_url = URLField()
    types = ListField()
    url = URLField()
    release_date = StringField()
    actors = ListField()


#保存每个电影的简介
class MovieContent(Document):
    url = StringField()
    content = StringField()
    id_ = StringField(unique=True)


class Task(Document):
    task = ListField()  # 保存要抓取的大类,task=['30','50:60']


class URL(Document):
    url = StringField()  # 保存要抓取详情的电影url,url_q


def request_task(url_q, task_q):
    while True:
        type_interval = task_q.get()
        print 'task {} is processing'.format(type_interval)
        handle_task(url_q, type_interval[0], type_interval[1])
        task_q.task_done()


def request_url(url_q):
    while True:
        try:
            url = url_q.get()
            handle_url(url)
            url_q.task_done()
        except Exception as e:
            print e
            print '还有{}个url没爬取'.format(url_q.qsize())
            while url_q.qsize():
                u = URL(url=url_q.get())
                u.save()



def handle_task(url_q, type, interval_id, start=0, retry=0):
    proxy = get_proxy_random()
    try:
        r = requests.get('http://www.douban.com', proxies=proxy)
        url = 'https://movie.douban.com/j/chart/top_list'
        params = {'type': type, 'interval_id': interval_id, 'action': '', 'start': start, 'limit': 20}
        cookies = {'bid': r.cookies['bid']}
        response = requests.get(url=url, headers=HEADERS, params=params, cookies=cookies, proxies=proxy)
        if response.status_code == 200:
            info = response.json()  # ValueError: No JSON object could be decoded
            if isinstance(info, list):
                for object_ in info:
                    save_info(object_, url_q, headers=HEADERS, params=params, cookies=cookies, proxies=proxy)
                start += 20
                return handle_task(url_q, type, interval_id, start=start)
            task = Task(task=[type, interval_id])
            task.delete()
            return
    except (SSLError, ConnectionError):
        retry += 1
        p = Proxy(proxy=proxy)
        p.delete()
        print 'delete p {}'.format(p)
        if retry > 3:
            print 'too much retries:type{}intervalid{}start{} '.format(type,interval_id,start)
            return
        return handle_task(url_q, type, interval_id, retry=retry)
    except Exception as e:
        raise


# 保存电影的粗略信息，保存成功后将电影的url放入url_q，进行下一步爬取
def save_info(object_, url_q, **kwargs):
    m = Movie(title=object_.get('title'),
            id_=object_.get('id'),
            score=object_.get('score'),
            vote_count=object_.get('vote_count'),
            rank=object_.get('rank'),
            cover_url = object_.get('cover_url'),
            types = object_.get('types'),
            url = object_.get('url'),
            release_date = object_.get('release_date'),
            actors = object_.get('actors'))
    try:
        m.save()
        save_img(img_url=object_.get('cover_url'), **kwargs)
        url_q.put(object_.get('id_'))
    except NotUniqueError:
        pass


def handle_url(url, retry=0):
    proxy = get_proxy_random()
    try:
        r = requests.get(url, proxies=proxy, headers=HEADERS)
        if r.status_code == 200:
            parse_page(url, r.content)
    except (SSLError, ConnectionError):
        if retry > 3:
            u = URL(url=url)
            u.delete()
            return
        p = Proxy(proxy=proxy)
        p.delete()
        retry += 1
        return handle_url(url, retry=retry)


def parse_page(url, content):
    selector = etree.HTML(content)
    summary = selector.xpath('.//span[@property="v:summary"]/text()')[0]
    m = MovieContent(content=summary, url=url.split('/')[-2])
    try:
        m.save()
    except NotUniqueError:
        pass


def save_movie_content(content, url):
    m = MovieContent(content=content, url=url, id_=url.split('/')[-2])
    try:
        m.save()
    except NotUniqueError:
        pass


def save_img(img_url, **kwargs):
    img = requests.get(url=img_url, headers=HEADERS, proxies=kwargs['proxies'], cookies=kwargs['cookies']).content
    foo = hashlib.md5()
    foo.update(img_url)
    img_name = foo.hexdigest()
    with open(IMG_DIR + '\\' + img_name + '.jpg', 'wb') as f:
        f.write(img)


# interval：50，results=[(type,interval_id)]
def put_task_to_db(interval):
    intervallist = [(str(item[0]) + ':' + str(item[1])) for item in zip(range(interval,91, 10), range(interval+10, 101, 10))]
    for type in range(1,31):
        for interval_ in intervallist:
            p = Task(task=[str(type), interval_])
            p.save()
            print [str(type), interval_]


# 传入线程数，从数据库里取出任务放入task_q,用request_task处理，没处理完一条，删除这条任务
# 保存请求的json数据（粗略信息），再将json里面的电影url放入url_q，用request_url处理
# 如果程序中断，将url_q里没处理完的url，保存起来，下次再爬
def run(thread_num):
    task_q = Queue.Queue()
    url_q = Queue.Queue()
    task_list =iter(Task.objects.all())
    for task in task_list:
        task_q.put(task.task)
    for i in range(thread_num):
        t = threading.Thread(target=request_task, args=(url_q, task_q))
        t.start()
    for i in range(thread_num):
        t = threading.Thread(target=request_url, args=(url_q,))
        t.start()
    task_q.join()
    url_q.join()

if __name__ == '__main__':
    run(8)


