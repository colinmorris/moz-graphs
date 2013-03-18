import urllib2
import urllib
import time
from chrome_cookies import ChromiumCookieJar
from bs4 import BeautifulSoup as Soup
import sqlite3
from optparse import OptionParser
import sys
from database import Debugger

OPENER = urllib2.build_opener(urllib2.HTTPCookieProcessor(ChromiumCookieJar('mozillians.org')))

HEADERS =  {'User-agent' : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/11.10 Chromium/18.0.1025.142 Chrome/18.0.1025.142 Safari/535.19",
    }

MAX_TRIES = 4

NAP_TIME = 2

def persistent_open(url, postdata=None):
    """Try to open and return the page at the given url. On failure, wait for 
    exponentially increasing amounts of time.
    """
    opener = OPENER
    try_no = 1
    while 1:
        try:
            if postdata is not None:
                postdata = urllib.urlencode(postdata)
            request = urllib2.Request(url, postdata, HEADERS)
            page = opener.open(request)#.read()
            time.sleep(NAP_TIME) # Throttle requests a bit
            return page
        except (urllib2.HTTPError, urllib2.URLError):
            if try_no >= MAX_TRIES:
                raise
            sleep_time = 20 + 2**try_no
            sys.stderr.write("[W01] Couldn't access location at %s on %dth try. Sleeping for %d seconds.\n" % (url, try_no, sleep_time))
            time.sleep(sleep_time)
            try_no += 1
            
def child_tags(elem):
    for child in elem.children:
        if not isinstance(child, Soup.NavigableString):
            yield child
            
class MozillianDB(sqlite3.Connection):

    table_names = ['moz']
    table_schemata = ['(dbid int, alias text, noccs int default 1)',
        '(email text, name text, irc text, username text, website text)',
        ]

    def __init__(self,fname):
        super(MozillianDB, self).__init__(fname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.text_factory = str 
        
    def get(self, mozname):
        raise NotImplementedError()
        
    def add(self, debugger):
        """Find the mozillian corresponding to the given Debugger (see class in
        database.py), if one exists, add it to the db and return it.
        """
        raise NotImplementedError()
        
        for alias in aliases_sorted_by_popularity:
            #try to add mozillian by alias and return
            pass
            
    def _add_mozillian(self, mozillian):
        pass
        
    def mozillian_candidates(self, debugger):
        pass
        
            
class Mozillian(object):

    def __init__(self, mozpage, uname):
        self.uname = uname
        self.name = mozpage.h2.string.strip()
        
        dt_to_attrname = {'Email':'email', 'IRC Nickname':'irc', 'Website':'website'}
        next_val = None
        for ele in child_tags(mozpage.dl):
            if ele.name == "dt":
                field = ele.string.strip()
                next_val = dt_to_attrname.get(field, None)
            elif ele.name == "dd" and next_val is not None:
                val = ele.a.string.strip()
                setattr(self, next_val, val)
                
    #def match_goodness(self, 

def mozillian_from_nick(nick):
    search_url = 'https://mozillians.org/en-US/search?q=' + nick
    res = persistent_open(search_url)
    final_url = res.geturl()
    soup = Soup(res)
    if '?' not in final_url:
        uname = final_url.split('/')[-1]
        return Mozillian(soup, uname)
        
    # Look through results and choose best cand
    
def chatty_chatters(db):
    """Return debuggers who talk on IRC and address or are addressed
    by other chatters.
    """
    for (dbid,) in db.execute("SELECT * FROM (SELECT p1 FROM chats UNION SELECT p2 FROM chats)"):
        yield Debugger(dbid, db)
    
def main():
    parser = OptionParser()
    parser.add_option("-o",
        "--output-fname",
        dest="out",
        default="mozillians.db",
        help="Filename to save the resulting db to")
        
    parser.add_option("-i",
        "--interactive",
        dest="interactive",
        action="store_true",
        help="Interactive mode: exit immediately to give access to all fns, globals,etc,")
    
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print "Usage: mozillian_scrape.py debugger_database_file"
        sys.exit(1)
        
    if options.interactive:
        sys.exit(1)
        
    mozillians = MozillianDB(options.out)
    main_db = sqlite3.connect(args[0])
    try:
        for chatter in chatty_chatters(main_db):
            mozillian = mozillians.add(chatter)
            main_db.merge_mozillian(chatter, mozillian)
            
    # We want to gracefully exit from anything, including a KeyboardInterrupt etc.
    except BaseException:
        print "WARNING: aborting in the middle of adding " + str(chatter)
        mozillians.commit()
        mozillians.close()
        raise
    
if __name__ == '__main__':
    main()
