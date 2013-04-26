import time
import datetime
import os
import cStringIO
import codecs
import csv
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from alchemical_base import Base
import config
import warnings

_t0 = time.time()
_t1 = time.time()
_t2 = time.time()

_DATE_FORMAT = "%Y-%m-%d"
_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Certain IRC nicks cause a host of problems because they're shared by prolific debuggers transiently
_IRC_BLACKLIST = ['nobody', 'david', '``', 'mc', 'foo', 'mw', 'help', 'register', 'rheet']

# Map database locations to engines for those databases, as a sort of caching thing.
DBLOC_TO_ENGINE = {}
#engine = create_engine(dbname)
#Session = sessionmaker(bind=engine)

def museumpiece(func):
    '''Decorator to mark functions that were only intended to be called once, and
    have served their purpose (e.g. to populate a table from scratch), but which we
    keep around for illustrative purposes, and in case the table gets dropped or
    something.'''
    def new_func(*args, **kwargs):
        warnings.warn("Call to museum piece function {}.".format(func.__name__),
                       category=DeprecationWarning)
        conf = raw_input("Are you sure you want to continue? (y/*) ")
        if conf in 'yY':
            return func(*args, **kwargs)
        else:
            print "Okay then."
            return
    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def open_data_file(fname, mode='r'):
        return open(os.path.join(config.DATA_DIR, fname), mode)

def open_log_file(fname, mode='w'):
    return open(os.path.join(config.LOG_DIR, fname), mode)

def clock_it(msg):
    """Print the amount of time that has passed since the last call, and report a message.
    Precondition: t1 has been initialized to a time.
    """
    global _t0, _t1, _t2
    _t2 = time.time()
    print msg + ' took %d seconds (total t=%d)' % (_t2 - _t1, _t2-_t0)
    _t1 = _t2
    
def datify(date_str):
    """Return a datetime object corresponding to a date string of the format used
    in bug_summary.csv etc.

    Jan 23 rev: Now that I think of it, this should return a date rather than a datetime.
    We don't deal with times anywhere here.

    Jan 27 rev: HAHA DISREGARD THAT. I need times for ordering, hence the next fn
    """
    date_str = date_str.split()[0]
    return datetime.datetime.strptime(date_str, _DATE_FORMAT).date()

def datetimeify(date_str):
    date, tz = date_str.rsplit(' ', 1)
    assert tz.isalpha(), "Expected last token in date string to be a time zone: " + date_str
    return datetime.datetime.strptime(date, _DATETIME_FORMAT)
    
def valid_nick(nick):
    return not nick.startswith('ircmonkey') and len(nick) > 1 and nick not in _IRC_BLACKLIST
    
def canonize(nick):
    """Return a canonical representation of this nickname.
    """
    # Remove trailing underscores
    nick = nick.rstrip('_')
    
    # Remove anything including and after a |
    bar_index = nick.find('|')
    if bar_index != -1:
        nick = nick[:bar_index]
        
    # Lowercase
    nick = nick.lower()
    
    return nick

def canon_name(name):
    """Right now we just strip anything in parens. Might want to do more later.
    """
    left = name.find("(")
    if left != -1:
        canon = name[:left].strip()
        return canon or name # Don't return an empty string
    return name

# Decorator for lazy property, stolen from stackoverflow
def lazyprop(fn):
    attr_name = '_lazy_' + fn.__name__
    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazyprop

def god_base():
    """Return a declarative base which is aware of all the classes that
    inherit from it (i.e. our full db structure).
    """
    import models, months, mozillians, bugstate, bugs, bugmonth_variables, debuggermonth, undirected_chats
    return Base

def create_tables(dbname=config.DB_URI, alembic_reset=False):
    global DBLOC_TO_ENGINE
    if dbname in DBLOC_TO_ENGINE:
        engine = DBLOC_TO_ENGINE[dbname]
    else:
        engine = create_engine(dbname)

    # We need to import all modules that contain mapped classes using Base so
    # we create all the relevant tables and don't get confused by any foreignkeys
    import models, months, mozillians, bugstate, bugs, bugmonth_variables, debuggermonth, undirected_chats
    Base.metadata.create_all(engine)


    # Code taken from Alembic documentation: This is supposed to clobber existing revs and start fresh (I don't know
    # whether this is what I actually want, but I'm running into issues and it seems like this might help.)

    if alembic_reset:
        # then, load the Alembic configuration and generate the
        # version table, "stamping" it with the most recent rev:
        from alembic.config import Config
        from alembic import command
        config_fname = os.path.join(os.getenv("HOME"), "Dropbox/Joel/debuggers/alembic.ini")
        alembic_cfg = Config(config_fname)
        command.stamp(alembic_cfg, "head")

def get_session(dbname=config.DB_URI):
    #import models, months, mozillians
    global DBLOC_TO_ENGINE
    if dbname in DBLOC_TO_ENGINE:
        engine = DBLOC_TO_ENGINE[dbname]
    else:
        engine = create_engine(dbname)
    # TODO: Blurgh. I think this is bad design, and the sessionmaker should only be made once, but I can't easily refactor this right now so blurgh
    Session = sessionmaker(bind=engine)
    return Session()

def gate(message):
    if not soft_gate(message):
        sys.exit(1)

def soft_gate(message):
    response = raw_input("About to %s. Continue? (Y/*) " % (message))
    return response in 'yY'

# I don't think this fn should really live here, but I don't know where else to put it,
# and I've deprecated its home base of sqla.py
def mozillian_merge(debugger, moz):
    """We've found that the given debugger and the given mozillian are a match.
    We need to set up the foreign keys accordingly, and also link some of their shared data.
    """
    debugger.mozillian = moz # No way it's this easy - right?

    # This feels a bit bad. Should probably distinguish names and nicks based on provenance...
    if debugger.name and moz.name:
        if debugger.name != moz.name:
            sys.stderr.write(u"""WARNING: Debugger with id %d was matched with mozillian
            with id %d, but debugger has name %s and mozillian has name %s\n""" %
                             (debugger.id or -1, moz.id or -1, debugger.name, moz.name))
    elif moz.name:
        debugger.name = moz.name

    if debugger.irc and moz.nick:
        if debugger.irc != moz.nick:
            sys.stderr.write(u"""Debugger with id %d was matched with mozillian
            with id %d, but debugger has nick %s and mozillian has nick %s
            """ % (debugger.id or -1, moz.id or -1, debugger.irc, moz.nick))
    elif moz.nick:
        debugger.irc = moz.nick

def bug_summary_dict():
    return _bug_summary("dict")

def bug_summary_list():
    return _bug_summary("list")

def _bug_summary(format):
    f = open_data_file("bug_summary.csv")
    headers = f.readline().split(',')
    reader = csv.reader(f)
    for row in reader:
        if format == 'dict':
            yield dict(zip(headers, row))
        elif format == 'list':
            yield row
        else:
            raise Exception("invalid format")

    f.close()

def rmparens(s):
    """Remove any parenthesized stuff from s, involving any kind of brackets or parens: < [ ( {...
    Then squeeze any repeated or trailing or leading spaces.

    >>> rmparens("Colin (the best) Morris")
    'Colin Morris'
    """
    res = ''
    PARENS = dict([('(', ')'), ('[', ']'), ('<', '>'), ('{', '}')])
    state = 0 # 0 -> spitting, 1 -> eating
    opener = None
    for char in s:
        if opener:
            if char == PARENS[opener]:
                opener = None
        else:
            if char in PARENS:
                opener = char
            else:
                res += char

    if opener:
        raise Exception("Mismatched parentheses in:\n" + s)
    return _squeeze_spaces(res)

def _squeeze_spaces(s):
    res = ''
    last = None
    for char in s:
        if not last:
            last = char
            res += char
            continue
        if not (last == ' ' and char == ' '):
            res += char
        last = char

    return res.strip()