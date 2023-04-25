#!/usr/bin/env python
# coding=utf-8
# Creative time 2020/3/20
# Creator HongYuan Guo

from gensim.models import word2vec
from gensim.models import Doc2Vec
import jieba.posseg as psg
import jieba
import time

class WordVector:

    def __init__(self):
        self.corpus_path = 'Corpus\corpus_jingdong.txt'
        self.static_model_path = 'baike_26g_news_13g_novel_229g.model'

    def Train_model(self,path,Size):
        sentences = word2vec.Text8Corpus(path)  # 加载语料
        model = word2vec.Word2Vec(sentences, size=Size)  # 默认window=5
        model.save('static_models\WordVector_JD60W_'+str(Size)+'.model')
        return model

    def get_model(self,path,size):
        try:
            model = Doc2Vec.load('static_models\WordVector_JD60W_'+str(size)+'.model')
            print('模型已存在')
        except Exception as e:
            print('正在训练模型')
            self.Train_model(path,size)
            print('训练结束')
        finally:
            model = Doc2Vec.load('static_models\WordVector_JD60W_'+str(size)+'.model')
            return model

    def Similarity_New( self ,model,word1,word2):
        word_list1 = []
        word_list2 = []
        similarity_TOP1 = 0
        similarity_list = model.wv.most_similar ( word2 , topn = 1 )  # 获取排名第一的相似之=值
        for item in similarity_list :
            similarity_TOP1 = item [ 1 ]
        similarity_between = model.wv.similarity ( word1 , word2 )  # 计算相似值

        # print ( '{}和{}的相似程度为{}'.format ( word1 , word2 , str ( format ( similarity_between / similarity_TOP1 * 100 , '.2f' ) ) + '%' ) )
        for k , v in psg.cut ( word1 ) :
            word_list1.append ( [ k , v ] )
        for k , v in psg.cut ( word2 ) :
            word_list2.append ( [ k , v ] )
        print ( '{} 和 {} 的相似系数为{},相似程度为:{}'.format ( word1 , word2 , str ( similarity_between ),str ( format ( similarity_between / similarity_TOP1 * 100 , '.2f' ) ) + '%' )  )
        return [word_list1,word_list2,similarity_between / similarity_TOP1]

if __name__ == '__main__':
    wv = WordVector()
    # model = wv.get_model(wv.corpus_path, 200)
    import time
    load_start = time.time()
    model = Doc2Vec.load(r'static_models\baike_26g_news_13g_novel_229g.model')
    load_finish = time.time()
    print('加载模型时长：',load_finish-load_start)
    word1 = u'语言'
    word2 = u'连横所著小说'
    similarity_TOP1 = 0

    # for cut_word, v in psg.cut(word2):
    for cut_word in jieba.lcut(word2 ,cut_all=True) :
        sim_start = time.time()
        similarity_list = model.wv.most_similar(cut_word, topn=1) #获取排名第一的相似之=值
        sim_stop = time.time()
        print('相似度计算时长：',sim_stop-sim_start)

        for item in similarity_list:
            similarity_TOP1 = [item[0],item[1]]
        similarity_between = model.wv.similarity(word1, cut_word) #计算相似值
        print('{}和{}的相似系数为{}'.format(word1, cut_word, str(similarity_between)))
        # print('与{}最相似的词为{}，相似系数为{}'.format(word1,similarity_TOP1[0],similarity_TOP1[1]))
        print ( '{}和{}的最终相似程度为{}'.format (word1, cut_word, str (format(similarity_between / similarity_TOP1[1] * 100, '.2f')) + '%'))
    # for k,v in psg.cut(word1):
    #     print(k,v)
    # for k,v in psg.cut(word2):
    #     print(k,v)