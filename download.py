import datetime, urllib
from pymongo import Connection

connection = Connection('localhost', 27017)

db = connection['wikilearn']

articles = db['articles']

f = urllib.urlopen('http://en.wikipedia.org/wiki/American_Revolution')

article = { 'date': datetime.datetime.utcnow(),
            'text': f.read() }

articles.insert(article)
