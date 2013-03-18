__author__ = 'colin'

import urllib2
import time
import sys
import socket
from bs4 import BeautifulSoup as Soup
import bs4
from chrome_cookies import ChromiumCookieJar
import urllib
import ssl
import httplib

socket.setdefaulttimeout(35)

# This is bad and not portable. Should be specified elsewhere.
OPENER = urllib2.build_opener(urllib2.HTTPCookieProcessor(ChromiumCookieJar('mozillians.org')))

HEADERS =  {'User-agent' : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/11.10 Chromium/18.0.1025.142 Chrome/18.0.1025.142 Safari/535.19",
            }

MAX_TRIES = 8

NAP_TIME = 2

DEPR_WARNED = False

class SmartOpener(object):
    """An opener that incorporates some of the smarts I need for scraping.
    Notably: fault-tolerance and automagic cookie handling integrated with
    Chromium.
    """

    def __init__(self, cookie_domain=''):
        self.max_tries = MAX_TRIES
        self.nap_time = NAP_TIME
        if cookie_domain:
            self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(ChromiumCookieJar(cookie_domain)))
        else:
            self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
        self.headers = HEADERS

    def open(self, url, postdata=None, astext=False):
        try_no = 1
        while 1:
            try:
                if postdata is not None:
                    postdata = urllib.urlencode(postdata)
                request = urllib2.Request(url, postdata, self.headers)
                page = self.opener.open(request)
                time.sleep(self.nap_time) # Throttle requests a bit
                if astext:
                    return page.read()
                else:
                    return page
            # NB: Some of these errors are kind of idiosyncratic, like BadStatusLine, which I think is
            #     a Bugzilla-specific weirdness.
            except (urllib2.HTTPError, urllib2.URLError, ssl.SSLError, httplib.BadStatusLine):
                if try_no >= self.max_tries:
                    raise
                sleep_time = 20 + 2**try_no
                sys.stderr.write("[W01] Couldn't access location at %s on %dth try. Sleeping for %d seconds.\n" % (url, try_no, sleep_time))
                time.sleep(sleep_time)
                try_no += 1

def persistent_open(url, postdata=None):
    """Try to open and return the page at the given url. On failure, wait for
    exponentially increasing amounts of time.
    """
    global DEPR_WARNED
    if not DEPR_WARNED:
        print "WARNING: Use of the persistent_open is deprecated. Use a SmartOpener instead."
        DEPR_WARNED = True
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

def next_tag(elem):
    for child in elem.next_siblings:
        if not isinstance(child, Soup.NavigableString):
            return child
    raise ValueError("No subsequent tag siblings")

def next_tags(elem):
    for child in elem.next_siblings:
        if not isinstance(child, Soup.NavigableString):
            yield child

def child_tag(elem):
    for child in elem.children:
        if not isinstance(child, Soup.NavigableString):
            return child
    raise ValueError("No tag children")

def child_tags(elem):
    for child in elem.children:
        if not isinstance(child, bs4.NavigableString):
            yield child