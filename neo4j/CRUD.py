# coding:utf-8
from py2neo import Graph, Node, Relationship,NodeMatcher
import glob
import redis

class redis_crud:

    def __init__(self):
        # 连接池
        self.redis_conn = redis.Redis(host='127.0.0.1', port=6379,db=1)
        self.redis_list_need_crawl = "redis_list_need_crawl"
        self.redis_set_had_crawl = "redis_set_had_crawl"  # 爬取过的集合

    def insert_list(self,href) -> None:
        '''
        插入爬取队列,尾部添加
        :return:
        '''
        self.redis_conn.rpush(self.redis_list_need_crawl, href)
        pass

    def pop_list(self) -> str:
        '''
        从头部获取一个待爬取的url
        :return:
        '''
        return self.redis_conn.lpop(self.redis_list_need_crawl)

    def insert_set(self,href) -> None:
        '''
        插入已经爬取过得集合
        :return:
        '''
        self.redis_conn.sadd(self.redis_set_had_crawl, href)


    def check_i_need_crawl(self,href:str) -> bool:
        '''
        检查已经爬取过得集合
        :return:
        '''
        return self.redis_conn.sismember(self.redis_set_had_crawl,href)




class neo4j_CRUD:

    def __init__(self):
        self.graph = self.get_connection()
        self.label = 'bkkg'

    #neo4j链接
    def get_connection(self):
        graph = Graph(
            "bolt://localhost:7687",
            auth = ('neo4j','exhibit-join-donor-orion-cable-4724')
        )
        print('connection success')
        return graph

    #创建节点
    def creat_node(self,clabels,**kwargs) -> Node:
        node_e = self.select_node_or_resp(clabels,kwargs['entity_name'],kwargs['entity_profile'])
        if node_e in (None, '') :  # 如果节点不存在
            node = Node(clabels,**kwargs,type=self.label)
            self.graph.create(node)
            print('creat node ',kwargs['entity_name'],kwargs['entity_profile'])
            return node
        else:
            print('node exist',kwargs['entity_name'],kwargs['entity_profile'])
            return node_e

    #建立关系
    def creat_resp(self,node1,node2,resp_name:str):
        resp = Relationship(node1, resp_name, node2)
        print('creat rela ',resp_name)
        self.graph.create(resp)

    #查询节点
    def select_node_or_resp(self,slabels,entity_name:str,entity_profile:str):
        matcher = NodeMatcher(self.graph)
        node_resp = matcher.match(slabels,entity_name=entity_name,entity_profile=entity_profile,type = self.label).first()
        return node_resp

    #切割字符串
    def cut_str(self,Str:str):
        if ':' in Str:
            return Str.split(':') #英文分隔
        else:
            return Str.split('：') #中文分隔


if __name__ == '__main__':
    pass
    rc = redis_crud()
    a = 'https://baike.baidu.com/item/夏朝/22101'
    # rc.insert_list(a)
    # rc.insert_set(a)
    # print(rc.pop_list())
    print(rc.check_i_need_crawl(a))