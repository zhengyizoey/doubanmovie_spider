# coding=utf-8
from mongoengine import *
import requests
import re, chardet
from selenium import webdriver
from lxml import etree

connect('newmovie')

headers = {
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding': 'gzip, deflate',
}


class Proxy(Document):
    proxy = StringField(unique=True)

    @classmethod
    def get_random(cls):
        p = cls.objects.aggregate({'$sample': {'size':1}}).next()
        return {'http': p.get('proxy')}


def delete_all_proxy():
    Proxy.drop_collection()


def xici_fetch():
    wb = webdriver.Firefox()
    wb.get('http://www.xicidaili.com/')
    r = wb.page_source
    selector = etree.HTML(r)
    trs = [selector.xpath('//tr[@class="odd"]'), selector.xpath('//tr[@class=""]')]
    for tr in trs:
        tr = tr[0]
        http = tr.xpath('//td[2]/text()')
        port = tr.xpath('//td[3]/text()')
        for i in range(len(http)):
            s = http[i] + ':' + port[i]
            print s
            try:
                p = Proxy(proxy='http://'+s)
                p.save()
            except NotUniqueError:
                pass
    # tr2 = selector.xpath('//tr[@class=""]')
    # tr = tr2[0]
    # http = tr.xpath('//td[2]/text()')
    # port = tr.xpath('//td[3]/text()')
    # for i in range(len(http)):
    #     s = 'http://' + http[i] + ':' + port[i]
    #     results.append(s)


def kuai_fetch():
    for page in range(1, 6):
        url = 'http://www.kuaidaili.com/proxylist/{}/'.format(page)
        web = webdriver.Firefox()
        web.get(url)
        r = web.page_source
        selector = etree.HTML(r)
        # trs = [selector.xpath('//tr[@class="odd"]'), selector.xpath('//tr[@class=""]')]
        trs = selector.xpath('.//tr')
        for tr in trs:
            https = tr[0].xpath('//td[1]/text()')
            ports = tr[0].xpath('//td[2]/text()')
            for i in range(len(https)):
                http = 'http://' + https[i] + ':' + ports[i]
                print http
                try:
                    p = Proxy(proxy=http)
                    p.save()
                except NotUniqueError:
                    pass


def guoban_fetch():
    for page in range(1, 6):  # 爬取前面5页，多了不新鲜
        url = 'http://www.goubanjia.com/free/gngn/index{}.shtml'.format(page)
        r = requests.get(url, headers=headers).content
        selector = etree.HTML(r)
        for i in xrange(15):
            d = selector.xpath('.//table[@class="table"]/tbody/tr[{}]/td'.format(i + 1))[0]
            o = d.xpath('.//span/text() | .//div/text()')
            proxy =  'http://'+''.join(o[:-1]) + ':' + o[-1]
            print proxy
            p = Proxy(proxy=proxy)
            try:
                p.save()
            except NotUniqueError:
                pass
    # tds = selector.xpath(u'.//div[@id="list"]/table/tbody/tr/td[@class="ip"]')[0]
    # print len(tds)
    # for td in tds:
    #     td_txt = td.xpath(u'//*[text()]')[0].text
    #     print td_txt


def check_proxy():
    proxies = Proxy.objects.all()
    for proxy in proxies:
        try:
            r = requests.get('https://www.baidu.com/', proxies={'http': proxy}, headers=headers)
        except Exception as e:
            p = Proxy(proxy=proxy)
            p.delete()


def get_proxy_random():
    return Proxy.get_random()


if __name__ == '__main__':
    #delete_all_proxy()
    #xici_fetch()
    #kuai_fetch()
    # guoban_fetch()
    # check_proxy()
    print len(Proxy.objects.all())
    print get_proxy_random()


