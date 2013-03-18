import sqlite3
import os
import cookielib
import urllib2

"""REMINDER:
CREATE TABLE cookies (
    creation_utc INTEGER NOT NULL UNIQUE PRIMARY KEY,
    host_key TEXT NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    path TEXT NOT NULL,
    expires_utc INTEGER NOT NULL,
    secure INTEGER NOT NULL,
    httponly INTEGER NOT NULL,
    last_access_utc INTEGER NOT NULL, 
    has_expires INTEGER DEFAULT 1, 
    persistent INTEGER DEFAULT 1);


cookielib.Cookie:
__init__(self, version, name, value, port, port_specified, domain, domain_specified, domain_initial_dot, path, path_specified, secure, expires, discard, comment, comment_url, rest, rfc2109=False)

"""

class ChromiumCookieJar(cookielib.CookieJar):

    RELEVANT_CJ_FIELDS = "name, value, host_key, path, secure, expires_utc"

    def __init__(self, domains, cookiepath=None):
        """
        A CookieJar that comes prepopulated with Chromium's cookies on the given
        domains. If no path to the cookie file is given, try the default location
        in the user's home dir.
        """
        cookielib.CookieJar.__init__(self)
        if cookiepath is None:
            # Load default location of chromium cookies
            cookiepath = os.path.join(os.getenv("HOME"), ".config", "chromium", "Default", "Cookies")
        
        self.db = sqlite3.connect(cookiepath)
        
        # Let's put some training wheels on this so we can pass in a single domain as well without putting it in a list
        if isinstance(domains, basestring):
            domains = [domains]
            
        for domain in domains:
            self.load_domain(domain)
        
    def load_domain(self, domain, fuzzy=True):
        """Add all cookies from the given domain to the jar.
        """
        if fuzzy:
            condition = 'WHERE host_key LIKE "%%%s"' % (domain)
        else:
            condition = "WHERE host_key=%s" % (domain)
        cookies = self.db.execute("SELECT %s FROM cookies %s" % (self.RELEVANT_CJ_FIELDS, condition) )
        for cookie_record in cookies:
            cookie = self.cookify(cookie_record)
            self.set_cookie(cookie)
        
    @staticmethod
    def cookify(record):
        """Return a freshly baked cookielib.Cookie object corresponding to
        the given sqlite3 record
        """
        (name, value, host_key, path, secure, expires) = record
        return cookielib.Cookie(
            version=0,
            name=name,
            value=value,
            port=None,
            port_specified=False,
            domain=host_key,
            domain_specified=bool(host_key),
            domain_initial_dot=host_key and host_key.startswith('.'), # XXX: Is this right?
            path=path,
            path_specified=bool(path),
            secure=secure,
            expires=expires, # TODO: might have to cast this?
            discard=False,
            comment=None,
            comment_url=None,
            rest={}
            )
            
