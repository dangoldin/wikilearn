import datetime, urllib2, time, random

from pymongo import Connection
from bson import BSON
import base64
#from bson.son import SON

from BeautifulSoup import BeautifulSoup
from collections import defaultdict

class wikilearn:
    def __init__(self,domain,port,db):
        self.domain = domain
        self.port = port
        self.db_name = db
        self.connection = Connection(domain, port)
        self.db = self.connection[db]

    def get_data(self,url,depth,replace):
        print 'Getting data for',url

        if depth == 0:
            return

        self.download(url,replace)
        self.process_article(url)
        
        articles = self.db['articles']
        article = articles.find_one({'url':url})
        if article is not None:
            links = article.get('links')
            if links is not None:
                for link in links.items():
                    link = base64.b64decode(link[0])
                    self.get_data(link,depth-1,replace)
        else:
            print 'Error',url,'not found'

    def download(self,url,replace):
        insert = True

        articles = self.db['articles']
        if articles.find({'url': url}).count() > 0:
            if replace:
                articles.remove({ 'url' : None })
                articles.remove({ 'url' : url })
            else:
                article = articles.find_one({'url': url})
                if article.get('text') is not None:
                    insert = False

        if insert:
            time.sleep(4 + random.randint(0,4))
            print 'Downloading',url
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            f = opener.open(url)
            articles.insert({ 'date': datetime.datetime.utcnow(),
                              'url' : url,
                              'text': f.read()
                              })

    def process_article(self, url):
        articles = self.db['articles']
        article = articles.find_one({'url':url})
        if article is not None:
            text = article.get('text')
            if article.get('links') is None:
                links = self.extract_links(text)
                print 'Inserting links', links
                articles.update( {'url': url}, {'$set': { 'links': links }} )
            else:
                links = article.get('links')

            for link,cnt in links.items():
                link = base64.b64decode(link)
                to_article = articles.find_one( {'url':link} )
                if to_article is not None:
                    to_links = to_article.get('to_links')
                    if to_links is not None:
                        to_links.append(url)
                        to_links = list(set(to_links))
                    else:
                        to_links = [ url ]
                    articles.update({'url': link},{'$set':{'to_links':to_links}})
                else:
                    articles.insert({ 'date': datetime.datetime.utcnow(),
                                      'url' : link,
                                      'to_links' : [ url ]
                                      })
        else:
            print 'No such article'

    def extract_links(self,text):
        base_url = 'http://en.wikipedia.org'
        links = {}
        soup = BeautifulSoup(text)
        for link in soup.findAll('a'):
            if '/wiki/' in link.get('href','') and ':' not in link.get('href') and 'http' not in link.get('href') and '//' not in link.get('href') and 'Main_Page' not in link.get('href'):
                link = link.get('href')
                print link
                hash_loc = link.find('#')
                if hash_loc >= 0:
                    link = link[:hash_loc]
                if 'http' not in link:
                    link = base_url + link
                
                link = base64.b64encode(link)
                try:
                    links[link] += 1
                except:
                    links[link] = 1
        print 'Retrieved',len(links),'links'
        return links

    def pagerank(self,iterations=10):
        for i in range(iterations):
            print 'Iteration',i
            articles = self.db['articles']
            for article in articles.find():
                pr = 0.15
                links = article.get('to_links')
                if links is not None:
                    for link in links:
                        la = articles.find_one({'url':link})
                        if la is not None:
                            score = la.get('score',1.0)
                            num_links = len(la.get('links'))
                            pr += 0.85 * (score/num_links)
                    articles.update({'url': article.get('url')},{'$set':{'score':pr}})
                    if random.randint(0,100) == 0:
                        print article.get('url'),':',pr

    def print_db(self):
        print 'DB Contents:'
        for a in self.db['articles'].find():
            print a.get('url'),len(a.get('links',[]))
            for link in a.get('links',[]):
                print link
        print 'Done printing'

if __name__ == '__main__':
    #url = 'http://en.wikipedia.org/wiki/American_Revolution'
    url = 'http://en.wikipedia.org/wiki/William_H._Lamport'
    depth = 3
    replace = True
    w = wikilearn('localhost', 27017, 'wikilearn')

    w.print_db()

    #w.pagerank()
    #exit()

    w.get_data(url,depth,replace)
    #w.download(url, False)
    w.process_article(url)
    w.print_db()