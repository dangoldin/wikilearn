import datetime, urllib2, time, random
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

    def get_data(self,url,depth,replace):
        if depth == 0:
            return

        self.download(url,replace)
        self.process_article(url)
        
        articles = self.db['articles']
        article = articles.find_one({'url':url})
        if article is not None:
            links = article.get('links')
            for link in links:
                self.get_data(link,depth-1,replace)
        else:
            print 'Error',url,'not found'

    def download(self,url,replace):
        time.sleep(4 + random.randint(0,4))
        insert = True

        articles = self.db['articles']
        if articles.find({'url': url}).count() > 0:
            if replace:
                articles.remove({ 'url' : None })
                articles.remove({ 'url' : url })
            else:
                insert = False

        if insert:
            print 'Downloading',url
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            f = opener.open(url)
            articles.insert({ 'date': datetime.datetime.utcnow(),
                              'url' : url,
                              'text': f.read() }
                            )

    def process_article(self, url):
        articles = self.db['articles']

        article = articles.find_one({'url':url})
        if article is not None:
            text = article.get('text')
            if article.get('links') is None:
                links = self.extract_links(text)
                articles.update({'url': url},{'$set':{'links':links}})
        else:
            print 'No such article'

    def extract_links(self,text):
        base_url = 'http://en.wikipedia.org'
        links = defaultdict(int)
        soup = BeautifulSoup(text)
        for link in soup.findAll('a'):
            if '/wiki/' in link.get('href','') and ':' not in link.get('href') and 'http' not in link.get('href'):
                link = link.get('href')
                hash_loc = link.find('#')
                if hash_loc >= 0:
                    link = link[:hash_loc]
                if 'http' not in link:
                    link = base_url + link
                links[link] += 1
        print 'Retrieved',len(links),'links'
        return links

    def print_db(self):
        print 'DB Contents:'
        for a in self.db['articles'].find():
            print a.get('url'),len(a.get('links'))
            for link in a.get('links'):
                print link
                exit()

if __name__ == '__main__':
    url = 'http://en.wikipedia.org/wiki/American_Revolution'
    depth = 6
    replace = False
    w = wikilearn('localhost', 27017, 'wikilearn')
    w.get_data(url,depth,replace)
    #w.download(url, False)
    w.process_article(url)
    w.print_db()
