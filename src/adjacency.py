"""
Code for outputting data in the form of comma-separated adjacency matrices,
given a DB session.
"""
from src.bug_events import BugEvent
from src.bugs import Bug
from src.debuggers import Debugger

__author__ = 'colin'

from models import *
from utils import *
from sqlalchemy import   func
from src.config import ADJ_DIR

class AdjacencyMatrix(object):

    def __init__(self, month, session):
        self.start = month.first
        self.stop = month.last
        self.session = session
        self._month = month

    @staticmethod
    def fname(month):
        name = "month" + str(month.id)
        return os.path.join(ADJ_DIR, name)

    @lazyprop
    def relevant_bugs(self):
        relevant_bugevents = self.session.query(BugEvent).filter(self.start <= BugEvent.date).\
        filter(BugEvent.date <= self.stop)
        res = set([be.bug for be in relevant_bugevents])
        print "Found %d relevant bugs" % (len(res))
        return res

    def _relevant_chatters(self):
        relevant_chats = self.session.query(Chat).filter(self.start <= Chat.date).\
        filter(Chat.date <= self.stop)
        semes = [chat.db1 for chat in relevant_chats]
        ukes = [chat.db2 for chat in relevant_chats]
        return set(semes + ukes) # remove duplicates

    def _relevant_bugactors(self):
        relevant_bugevents = self.session.query(BugEvent).filter(self.start <= BugEvent.date).\
        filter(BugEvent.date <= self.stop)
        return set([be.debugger for be in relevant_bugevents])

    @lazyprop
    def relevant_debuggers(self):
        res = self._relevant_chatters().union(self._relevant_bugactors())
        print "Found %d relevant debuggers" % (len(res))
        return res

    def get(self, row, col):
        if isinstance(row, Debugger):
            return self.get_nchats(row, col)
        elif isinstance(row, Bug):
            return self.get_nevents(row, col)
        else:
            raise ValueError

    def get_nchats(self, seme, uke):
        return self.session.query(func.sum(Chat.n)).\
               filter_by(p1=seme.id).filter_by(p2=uke.id).\
               filter(self.start <= Chat.date).\
               filter(Chat.date <= self.stop).scalar() or 0
        # Apparently if no rows are found, we get None instead of 0. How silly.

    def get_nevents(self, bug, debugger):
        """Return the number of times the given debugger has acted on the given bug in
        our time interval.
        """
        return self.session.query(BugEvent).\
        filter_by(bzid=bug.bzid).\
        filter_by(dbid=debugger.id).\
        filter(self.start <= BugEvent.date).\
        filter(BugEvent.date <= self.stop).count()

    def save(self):
        f = open(self.fname(self._month))
        writer = UnicodeWriter(f)
        writer.writerow([''] + map(unicode, self.relevant_debuggers) )

        # Chats
        for a in list(self.relevant_debuggers) + list(self.relevant_bugs):
            row = [unicode(a)]
            for b in self.relevant_debuggers:
                row.append(unicode(self.get(a, b)))
            writer.writerow(row)

        f.close()
