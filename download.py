import datetime, urllib2
from pymongo import Connection
from BeautifulSoup import BeautifulSoup
from collections import defaultdict

class wikilearn:
    def __init__(self,domain,port,db):
        self.domain = domain
        self.port = port
        self.db_name = db
        self.connection = Connection(domain, port)
        self.db = self.connection[db]

    def download(self,url,depth):
        print 'Downloading',url
        if depth == 0:
            return
        opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        f = opener.open(url)

        articles = self.db['articles']
        if articles.find({'url': url}).count() > 0:
            articles.remove({ 'url' : None })
            articles.remove({ 'url' : url })

        articles.insert({ 'date': datetime.datetime.utcnow(),
                          'url' : url,
                          'text': f.read() }
                        )

    def process_article(self, url):
        articles = self.db['articles']

        article = articles.find_one({'url':url})
        if article is not None:
            text = article.get('text')
            links = self.extract_links(text)
            articles.update({'url': url},{'$set':{'links':links}})
        else:
            print 'No such article'

    def extract_links(self,text):
        links = defaultdict(int)
        soup = BeautifulSoup(text)
        for link in soup.findAll('a'):
            if '/wiki/' in link.get('href','') and ':' not in link.get('href') and 'http' not in link.get('href'):
                link = link.get('href')
                hash_loc = link.find('#')
                if hash_loc >= 0:
                    link = link[:hash_loc]
                links[link] += 1
#        for link,cnt in links.items():
#            print link, cnt
#        print len(links)
        return links

    def print_db(self):
        print 'DB Contents:'
        for a in self.db['articles'].find():
            print a.get('url')
            print len(a.get('links'))

if __name__ == '__main__':
    url = 'http://en.wikipedia.org/wiki/American_Revolution'
    w = wikilearn('localhost', 27017, 'wikilearn')
    w.download(url, 6)
    w.process_article(url)
    w.print_db()
