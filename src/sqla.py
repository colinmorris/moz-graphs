"""
This is try 2 (3?) at solving the mozillians problem, this time using sqlalchemy.

Overview:

We have information about Mozilla developers from two primary sources of interest:
    1) Mozillian IRC chat logs. We know what IRC nicks different actors are using,
    and when they addressed one another.
    2) Bugzilla data for a specific set of bugs (those discussed in the IRC chats).
    We know which devs (by EMAIL only) acted on which bugs on which dates.

We're ultimately interested in synthesizing this information, producing adjacency
matrices for various timespans showing for a given developer how many times they
talked to each other developer, and whether they were active on each bug.

This requires that we unify the data from our two sources above. To do this, we
use the mozillians phonebook, which should ideally have e-mails and IRC nicks.

This module is concerned with building up a database of devs, bugs, and their
interactions (dev-dev interactions and dev-bug interactions).

JAN 18 POSTSCRIPT:

This module looks a little weird now. I started off thinking I could put everything
in a God DB class, and then slowly came to realize that this was untenable. If I
were principled, I would break this up into modules using the same design as I've
used with the new tables. But I won't.
"""

from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine, or_, func

import sys
import csv
import re
from optparse import OptionParser
from src.bug_events import BugEvent
from src.bugs import Bug
from src.debuggers import Debugger

from utils import *
from models import *
from mozillians import Mozillian
import chatters
import adjacency

from alchemical_base import Base

class RedundantLoadException(Exception):
    pass

class BugRow(object):
    """Information encoded in a row of bug_summary.csv
    """

    fields = """assigned_to,blocks,bug,cc_list,component,depends_on,duplicates,history,
    importance,keywds,n_cc_list,n_depends,n_duplicates,n_history,n_keywds,
    nblocks,nvoters,platform,product,reported,resolved,status,verified,version,voters
    """.replace('\n', '').replace(' ', '').split(',')
    
    int_fields = ['n_cc_list', 'n_depends', 'n_duplicates', 'n_history', 'n_keywds',
        'nblocks', 'nvoters', 'bug']
    date_fields = ['reported', 'resolved', 'verified']
    
    def __init__(self, row):
        assert len(row) == len(self.fields)
        for (field, value) in zip(self.fields, row):
            if field in self.int_fields:
                value = int(value)
            elif field in self.date_fields:
                try:
                    value = datify(value)
                except ValueError:
                    value = None
            setattr(self, field, value)
            
    def to_bug(self):
        return Bug(bzid=self.bug,
            importance = self.importance,
            n_cc_list = self.n_cc_list,
            n_depends = self.n_depends,
            n_duplicates = self.n_duplicates,
            n_history = self.n_history,
            n_keywds = self.n_keywds,
            nblocks = self.nblocks,
            nvoters = self.nvoters,
            product = self.product,
            reported = self.reported, # NTS: somehow I forgot to put this in the first time...
            resolved = self.resolved,
            status = self.status,
            verified = self.verified,
            )
            
    def assigned_to_debugger(self):
        """Return a debugger object corresponding to the person this bug is assigned to.
        """
        email = None
        name = None
        nick = None
        
        assign_str = self.assigned_to
        ass_re = re.compile("(?P<name>.*?)( [\[(]?:(?P<nick>[^ \])]*)[\])]?(.*?))? <(?P<email>.*)>$")
        if assign_str == "Nobody; OK to take it and work on it <nobody@mozilla.org>":
            return None
        if len(assign_str.split()) == 1:
            email = assign_str
            
        else:            
            match = ass_re.match(assign_str)
            if match.group("nick"):
                nick = canonize(match.group("nick"))
            else:
                nick = None
            email = match.group("email")
            name = match.group("name")
            name = unicode(name, 'utf8')
            
        return Debugger(email=email,
            name=name,
            irc=nick,
            )

class MozDB(object):

    def __init__(self, url, echo=False):
        self.echo = echo
        self.engine = create_engine(dblocation, echo=self.echo)
        self.Session = sessionmaker(bind=self.engine) # This is like an on-the-fly class thing? Ugh yuck ew gross.
        # Make tables if they're not bad yet
        Base.metadata.create_all(self.engine)
        #self._test()

    def match_item(self, item, sesh=None, fields=[]):
        """Return whether this db contains an item matching the given one on all fields EXCEPT on the
        field id (which is assumed to be an auto-incrementing primary key).

        We also ignore any VarColumns.

        If sequence fields is provided, only try to match on fields with those names.
        """
        if sesh is None:
            session = self.Session()
        else:
            session = sesh
        kwargs = {}
        for col in item.__table__.columns:
            if fields and col.name not in fields:
                continue
            if not fields and (col.name == 'id' or isinstance(col, VarColumn)):
                continue
            kwargs[col.name] = getattr(item, col.name)
        match = session.query(type(item)).filter_by(**kwargs).first()
        return match

    def _test(self):
        session = self.Session()
        debugger = Debugger(email="foo@foo.com", name="Colin Morris", irc="colin")
        session.add(debugger)
        session.commit()

    def from_scratch(self):
        """Populate the db from nothing.
        It's important to source our data files in the right order, just because of the way things are set up. This
        method demonstrates the correct order.
        """
        self.add_bug_summary()
        self.add_bug_history()
        self.mozillians_enrich()
        self.add_alias_file()
        self.add_chat_logs()

    def add_chat_logs(self, dirname=chatters.CHATLOG_DIR):
        session = self.Session()
        for fname in os.listdir(dirname):
            if not fname.endswith('.log'):
                continue
            f = open(os.path.join(dirname, fname))
            log = chatters.Log(f)
            self._add_chat_log(log, session)
            f.close()

        session.commit()

    def db_from_nick(self, nick, session=None):
        """Return the debugger who uses the given IRC nick.
        """
        if session is None:
            sesh = self.Session()
        else:
            sesh = session
        match = sesh.query(Alias).filter_by(alias=nick)
        n = match.count()
        if n != 1:
            raise Exception("Expect to find one db matching %s but found %d" % (nick, n))
        return match.first().debugger

    def _add_chat_log(self, log, session):
        ext_to_db = {}

        # Increase the counts for the number of times we've seen the debuggers/alias in this log
        for ext in log.exterminators:
            debugger = self.db_from_nick(ext.nick, session)
            ext_to_db[ext] = debugger
            debugger.nirc += 1
            for alias in ext.aliases:
                al = session.query(Alias).filter_by(alias=alias).first()
                al.noccs += 1

        # Add the chatting interactions
        adj = log.adj_dict()
        for seme_ext in adj:
            seme = ext_to_db[seme_ext]
            seme_row = adj[seme_ext]
            for uke_ext in seme_row:
                uke = ext_to_db[uke_ext]
                n = seme_row[uke_ext]
                if n > 0:
                    chat = Chat(p1=seme.id, p2=uke.id, n=n, date=log.start.date())
                    session.add(chat)

        session.commit()

        
    def add_bug_summary(self, fname="bug_summary.csv"):
        """Add the bug summary file, containing information about relevant bugs scraped from bugzilla.
        Each row will give us a new bug for the bugs table, and possibly a new debugger - we look at
        the "assigned to" field and parse the info to add to the debuggers table.
        """
        session = self.Session()

        nbugs = session.query(Bug).count()
        if nbugs > 0:
            raise RedundantLoadException("bug summary is already loaded")

        f = open_data_file(fname)
        header = f.readline() # Skip header
        assert header.startswith('assigned_to')
        reader = csv.reader(f)
        for row in reader:
            br = BugRow(row)
            assigned = br.assigned_to_debugger()
            bug = br.to_bug()
            session.add(bug)
            if assigned is not None:
                # Check if there's already a debugger of this description in our table
                match = self.match_item(assigned, session, ['email'])
                if not match:
                    # If there isn't, add him
                    session.add(assigned)
                else:
                    # Otherwise just increment the count of times we've seen him
                    match.nassigned += 1

        f.close()
        session.commit()
    
    def add_bug_history(self, fname="bug_history.csv"):
        """Add the information stored in bug_history.csv ish file
        """
        session = self.Session()

        nbugevents = session.query(BugEvent).count()
        if nbugevents > 0:
            raise RedundantLoadException("bug history is already loaded")

        f = open_data_file(fname)
        f.readline() # ignore headers
        reader = csv.reader(f)
        for line in reader:
            row = BugHistoryRow(line)
            debugger = row.debugger()
            debugger_match = self.match_item(debugger, session, ['email'])
            if not debugger_match:
                session.add(row.debugger())
                session.flush()
            else:
                debugger_match.nbz += 1
                debugger = debugger_match

            bug_event = row.bug_event(debugger.id)
            session.add(bug_event)

        f.close()
        session.commit()

    def add_alias_file(self, fname="sorted_IRC_nicks.txt"):
        """
        The given filename should point to a file with sets of comma-separated aliases on each line.
        """
        session = self.Session()

        naliases = session.query(Alias).count()
        if naliases > 0:
            raise RedundantLoadException("Already loaded nicks")

        f = open_data_file(fname)
        errlog = open_log_file('ambiguous_nicksets.txt')
        ambiguous = 0
        for line in f:
            nicks = line.strip().split(',')
            assert nicks and isinstance(nicks, list)
            matches = session.query(Debugger).filter(Debugger.irc.in_(nicks))
            nmatches = matches.count()

            # Case 1: No matches
            if nmatches == 0:
                canonic_irc = min(nicks, key=lambda x: len(x))
                debugger = Debugger(irc = canonic_irc)
                session.add(debugger)
                session.flush() # Have to do this to get id
            # Case 2: Unique match
            elif nmatches == 1:
                debugger = matches.first()
            else:
                errlog.write(line)
                ambiguous += 1
                continue

            for nick in nicks:
                alias = Alias(dbid=debugger.id,
                    alias=nick,
                )
                session.add(alias)

        f.close()
        session.commit()
        print "Skipped %d ambiguous nicksets and wrote to logs/ambiguous_nicksets.txt" % (ambiguous)

    def mozillians_enrich(self, limit = 0):
        """Enrich and link our debuggers table using mozillians data, lazy-scraped.
        If limit is supplied, only enrich that many mozillians.
        """
        session = self.Session()
        n = 0
        commit_interval = 100 # The scraping takes a while, so we stagger commits to amortize our possible losses
        for debugger in session.query(Debugger).\
            filter(Debugger.nbz>0).\
            filter_by(irc=None).\
            filter_by(mozillians_searched=False):

            n += 1

            if self.echo:
                print
                try:
                    print "****Enriching: " + str(debugger)
                except UnicodeEncodeError:
                    # I'm so helpless
                    print "***Enriching someone with diacritics in their name."
                print

            moz = self.fetch_lazy_mozillian(debugger.email, session)
            if moz:
                if self.echo:
                    print
                    print "FOUND A MATCH!"
                    print
                self.mozillian_merge(debugger, moz)
            debugger.mozillians_searched = True

            if n == 2:
                session.commit()
            elif (n%commit_interval) == 0:
                session.commit()

            if limit and n >= limit:
                session.commit()
                return


        session.commit()

    def mozillians_enrich_byname(self):
        """This is sort of write-once, read-once, execute-once code. Don't worry about it.
        """
        session = self.Session()
        for debugger in session.query(Debugger).\
                filter_by(mozid=None).\
                filter_by(irc=None).\
                filter(Debugger.name != None):
            mozmatches = session.query(Mozillian).filter(or_(Mozillian.name == debugger.name,
                                     Mozillian.name == debugger.canon_name))
            nmatches = mozmatches.count()
            if not nmatches:
                continue
            elif nmatches > 1:
                print "WARNING: more than one match for " + str(debugger)
                raise Exception()
            match = mozmatches.first()
            print "Matching " + str(debugger) + '\n with ' + str(match)
            self.mozillian_merge(debugger, match)

        session.commit()


    def mozillian_merge(self, debugger, moz):
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

    def fetch_lazy_mozillian(self, field, value, sesh=None):
        """Those lazy mozillians...

        If we have a mozillian matching the given e-mail in the DB, return her. Otherwise,
        attempt to scrape her. If we don't find a match, return None.
        """
        if sesh is None:
            session = self.Session()
        else:
            session = sesh

        extant = session.query(Mozillian).filter_by({field:value}).first()
        # Case 1: this mozillian is already in the db. Return her (completed if necessary)
        if extant:
            if not extant.complete:
                extant.flesh_out()
            return extant

        # Case 2: not in DB. Scrape to find
        (mozillians, target) = Mozillian.scrape_matching_mozillians(field, value)
        for moz in mozillians:
            uname_match = self.match_item(moz, session, ['username'])
            # If we have a matching mozillian but this one is more complete, then update using this new info
            if uname_match and moz.complete and not uname_match.complete:
                uname_match._overwrite(moz)
                if target is moz:
                    # Also, if this is the target, then return the updated version of the existing moz
                    # rather than the new moz
                    target = uname_match
            # If there exists no matching mozillian, then add this one to the db
            elif not uname_match:
                session.add(moz)

        return target

    def write_matrices(self, windowsize=7, delta=1, dirname='../adj', limit=0):
        oneday = datetime.timedelta(days=delta) # This is now poorly named
        session = self.Session()
        start_date = session.query(func.min(Chat.date)).scalar()
        end_date = session.query(func.max(Chat.date)).scalar()

        start = start_date
        stop = start + datetime.timedelta(days=windowsize)

        n = 0

        while stop <= end_date:
            fname = str(start) + '.csv'
            path = os.path.join(dirname, fname)
            f = open(path, 'w')
            adj = adjacency.AdjacencyMatrix(start, stop, session)
            adj.save(f)
            f.close()

            start += oneday
            stop += oneday

            n += 1
            if limit and n >= limit:
                return





class BugHistoryRow(object):

    def __init__(self, row):
        (self.bugid, self.email, self.date) = row
        self.date = datify(self.date)

    def debugger(self):
        return Debugger(email=self.email, nbz=1)

    def bug_event(self, dbid):
        return BugEvent(bzid=self.bugid,
            dbid=dbid,
            date=self.date
        )
            
    
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-e",
        "--echo",
        dest="echo",
        action="store_true",
        help="Echo SQL",
    )
    parser.add_option("-i",
        "--interactive",
        dest="interactive",
        action="store_true",
        help="Interactive mode. Don't do anything.",
    )
    (options, args) = parser.parse_args()


    if len(args) < 1:
        dblocation = "sqlite:///:memory:"
    else:
        dblocation = "sqlite:///" + args[0]
    db = MozDB(dblocation, options.echo)
    try:
        db.add_bug_summary()
        db.add_bug_history()
    except RedundantLoadException:
        print "Skipping some step of loading process"

    if options.interactive:
        sys.exit(1)

    #db.add_alias_file()
    #db.add_chat_logs()
    #db.mozillians_enrich()
    #db.add_bug_history()
    db.write_matrices(30, 15, '../adj_monthly', 0)
    #db.write_matrices(30, '../adj_monthly', 1)
    #db.write_matrices(90, '../adj_quarterly', 1)

    #(mozs, targ) = Mozillian.scrape_mozillians_by_email("bob@bclary.com")
