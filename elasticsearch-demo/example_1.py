#!/usr/bin/env python
# *_* encoding=utf-8 *_*

from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections
from datetime import datetime, timedelta
from elasticsearch.exceptions import ConflictError

# Define a default Elasticsearch client
es_cluster = "master"
connections.create_connection(hosts=['localhost:9200'], alias=es_cluster, timeout=20)


class Article(DocType):
    class Meta:
        doc_type = 'article_data'
        index = '{env}-article-data'.format(env="test")

    class Index:
        doc_type = 'article_data'
        index = '{env}-article-data'.format(env="test")

    id=Keyword()
    title = Text(analyzer="ik_max_word", search_analyzer='ik_smart',fields={'keyword':Keyword()})
    body = Text(analyzer="ik_max_word", search_analyzer='ik_smart',fields={'keyword':Keyword()})
    tags = Keyword()
    published_from = Date()
    lines = Integer()

    @classmethod
    def save_with_version(cls, data, external_version=None):
        print data
        if not external_version:
            external_version = int((datetime.now() - datetime(1970, 1, 1)).total_seconds() * 1000) - 8*3600*1000
        id = data.get('id')
        if id:
            try:
                document = cls(**data)
                document.meta.id = id
                document.meta.version = external_version
                document.meta.version_type = 'external_gte'
                print document
                document.save(using=es_cluster, index='test-article-data')
            except ConflictError as ex:
                print ex

    def is_published(self):
        return datetime.now() >= self.published_from


    @classmethod
    def get_article(cls, id=id):
        article = cls.get(id=id, ignore=404, using=es_cluster, index='test-article-data')

        print(article.to_dict())

    @classmethod
    def get_all_titles(cls):
        # scroll
        title_list = []
        s = Article.search(using=es_cluster).sort({"lines": {"order": "desc"}}).params(preserve_order=True)
        s = s.source(['title'])
        for hit in s.scan():
            print 'hit:',hit
            title_list.append(hit.id)
        print "all article count is:", len(title_list)
        return title_list

    @classmethod
    def delete_article(cls,id):
        article = cls.get(id=id)
        article.delete()



if __name__ == '__main__':
    # create the mappings in elasticsearch
    Article.init(index='test-article-data',  using=es_cluster)
    data = {'id':1, 'title':'elasticsearch good', 'body':'I like elasticsearch.', 'tags':['elasticsearch', 'kibana']}
    for i in range(1,140):
        Article.save_with_version(data)
    Article.get_article(id=1)
    print Article.get_all_titles()
    Article.delete_article(id=1)
    #pass