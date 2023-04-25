#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：BK_Knowledge_Graph 
@File    ：crawler.py
@IDE     ：PyCharm 
@Author  ：HongYuan Guo
@Date    ：2023/3/14 19:52 
'''
import requests
from bs4 import BeautifulSoup
import multiprocessing

import re
import time
try:
    from .neo4j.CRUD import neo4j_CRUD
    from .neo4j.CRUD import redis_crud

except:
    from neo4j.CRUD import neo4j_CRUD
    from neo4j.CRUD import redis_crud


crud = neo4j_CRUD()
redis_c =  redis_crud()

def baike_crawler(url:str) -> requests.Response:
    '''
    爬取百度百科内容的代码
    :param url:
    :return:
    '''
    headers = {
    'Connection': 'keep-alive',
    'Host': 'baike.baidu.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.54',
    'cookies':'zhishiTopicRequestTime=1681813315491;'
    }
    res = requests.get(headers=headers,url=url)

    return res

def clean(c:str) -> str:
    return c.replace('\xa0','').replace('\n','')

def replace_zongkuohao(c:str) -> str:
    num = re.sub(r'\[[0-9\-]*\]', "", c)
    return num

def analyzing_baike_html(htmlstr:str) -> dict:
    '''
    解析百科html
    :param htmlstr:
    :return:
    '''

    soup = BeautifulSoup(htmlstr,'lxml')

    entity_name =  soup.find('h1').text
    entity_profile = soup.find('div',attrs = {'class':'lemma-desc'}).text

    attr_list = []
    attr_div = soup.find('div',attrs = {'class':'basic-info J-basic-info cmn-clearfix'})

    attr_left = attr_div.find('dl',attrs = {'class':'basicInfo-block basicInfo-left'})
    for dt in attr_left.find_all('dt'):
        attr_name = dt.text
        dd = dt.find_next('dd')

        dc =  dd.children
        for item in dc:
            if item.name == 'a':
                try:
                    href = item.attrs['href']
                    attr_value = item.text
                    attr_list.append([clean(attr_name), clean(attr_value), href])
                except:
                    attr_value = item.text
                    attr_list.append([clean(attr_name), clean(attr_value), False])
            else:
                try:
                    attr_value = item
                    attr_list.append([clean(attr_name), clean(attr_value), False])
                except:
                    attr_value = item.text
                    attr_list.append([clean(attr_name), clean(attr_value), False])


    attr_right = attr_div.find('dl',attrs = {'class':'basicInfo-block basicInfo-right'})
    for dt in attr_right.find_all('dt'):
        attr_name = dt.text
        dd = dt.find_next('dd')

        dc =  dd.children
        for item in dc:
            if item.name == 'a':
                try:
                    href = item.attrs['href']
                    attr_value = item.text
                    attr_list.append([clean(attr_name), clean(attr_value), href])
                except:
                    attr_value = item.text
                    attr_list.append([clean(attr_name), clean(attr_value), False])
            else:
                try:
                    attr_value = item
                    attr_list.append([clean(attr_name), clean(attr_value), False])
                except:
                    attr_value = item.text
                    attr_list.append([clean(attr_name), clean(attr_value), False])

    attr_list = clean_attr_value(attr_list)


    event_list = []
    if len(soup.find_all('h3')) > 1:
        for h2 in soup.find_all('h2'):
            if h2.text == '目录':
                continue
            h3 = h2.find_next('h3')
            while h3:
                h3_p_h2 = h3.find_previous('h2').text
                if  h3_p_h2 == h2.text:
                    # print(h2.text,h3.text)
                    content = ''
                    content_div = h3.find_next('div',attrs = {'class':'para'})
                    while content_div:
                        content_div_h3 = content_div.find_previous('h3').text
                        if content_div_h3 == h3.text:
                            content = content + clean(replace_zongkuohao(content_div.text)) + '\n'
                            content_div = content_div.find_next('div',attrs = {'class':'para'})
                        else:
                            break
                    if '扫码' not in h3.text:
                        event_list.append([h3.text,content])
                    h3 = h3.find_next('h3')
                else:
                    break

    entity_href_list = []
    entity_href = soup.find_all('a')
    for eh in entity_href:
        if  'href' in eh.attrs and 'item' in eh.attrs['href']:
            if '秒懂' in eh.attrs['href'] :
                continue
            else:
                entity_href_list.append('https://baike.baidu.com'+eh.attrs['href'])


    return {
        'entity_name':entity_name,
        'entity_profile':entity_profile,
        'attr_list':attr_list,
        'event_list':event_list,
        'entity_href_list':entity_href_list
    }

def analyzing_baike_html_get_title(htmlstr:str) -> str:
    soup = BeautifulSoup(htmlstr,'lxml')
    try:
        entity_profile = soup.find('div', attrs = {'class' : 'lemma-desc'}).text
        return entity_profile
    except:
        print('need choise')
        div = soup.find('div', attrs = {'class' : 'para'})
        entity_profile_href = div.find('a').attrs['href']
        r = baike_crawler('https://baike.baidu.com' + entity_profile_href)
        new_soup = BeautifulSoup(r.text,'lxml')
        entity_profile = new_soup.find('div', attrs = {'class' : 'lemma-desc'}).text
        return entity_profile

def get_href_titile(url:str) -> str:
    r = baike_crawler(url)
    res = analyzing_baike_html_get_title(r.text)
    return res

def clean_attr_value(attrlist:list) -> list:
    return_list = []
    for item in attrlist:
        if '[' in item[1]:
            continue
        elif '' == item[1] or ' ' == item[1] or '、' == item[1]:
            continue
        elif item[1][0] == '、':
            return_list.append([item[0],item[1][1:],item[2]])
        else:
            return_list.append([item[0],item[1],item[2]])
    return return_list


def analyzing_baike_url(url:str) -> dict:
    r = baike_crawler(url)
    res = analyzing_baike_html(r.text)
    return res

def build_graph_from_url(url:str) -> None:
    graph_data = analyzing_baike_url(url)
    #------------------ 页面主实体构建-----------------------------------------------------
    entity =  crud.creat_node(clabels = graph_data['entity_profile'],**{
        'entity_name':graph_data['entity_name'],'entity_profile':graph_data['entity_profile']
    })
    #------------------ 属性实体构建 --------------------------------------------------------
    for item in graph_data['attr_list']:
        if item[1] == '等':    # 去掉多个实体后面的 等 字
            continue
        elif not item[2] and item[1][-1] == '等':
            item[1] = item[1][:-1]

        if item[2]:
            entity_profile = get_href_titile('https://baike.baidu.com'+item[2])  # 有链接的实体，就用百度给的标签
            attr_ent = crud.creat_node(clabels = entity_profile,**{
                'entity_name' : item[1],
                'entity_profile' : entity_profile
            })
            crud.creat_resp(entity,attr_ent,item[0])
        else:
            attr_ent = crud.creat_node(clabels = item[0],**{    # 没有链接的实体，就用他的类别作为标签
                'entity_name' : item[1],
                'entity_profile' : graph_data['entity_name'] + '的' + item[0]
            })
            crud.creat_resp(entity, attr_ent, item[0])
    #-------------------- 普通实体构建 -----------------------------------------------------------
    for item in graph_data['event_list']:   #最后是普通的事件实体，统统使用event_list

            attr_ent = crud.creat_node(clabels = 'normal event',**{
                'entity_name' : item[0],
                'entity_profile' : item[0],
                'entity_content' : item[1],
            })
            crud.creat_resp(entity,attr_ent,item[0])

    for item in graph_data['entity_href_list']:
        if item and not redis_c.check_i_need_crawl(item) :  # href存在且不在已经爬取过得集合中
            redis_c.insert_list(item) #将解析的实体链接加入到待爬取队列
        else:
            print('-url list exist')
    return None

def crawler():
    while True:
        href = redis_c.pop_list()  # 弹出一个链接
        if href and not redis_c.check_i_need_crawl(href): # href存在且不在已经爬取过得集合中
            try:
                build_graph_from_url(href)
                print('Finish url', href)
                redis_c.insert_set(href)  # 加入待爬取队列，失败了不需要重新爬取
            except Exception as e:
                print('Bad url','because:',str(e),href)
        else:
            print('had Finish url', href)
            time.sleep(0.1)


if __name__ == '__main__':
    crawler()
    # build_graph_from_url('https://baike.baidu.com/item/%E9%B2%81%E8%BF%85/36231')

