"""WORK IN PROGRESS.

Currently a bit of a mess, and badly named. Should maybe be hacked up
and spread around a bit.

March 18: This module is pretty much deprecated.
"""

__author__ = 'colin'

from alchemical_base import Base
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Unicode, Boolean
from sqlalchemy.orm import backref, relationship
import csv
from months import *

class Rowable(object):
    """
    Defines an interface for an object which can be exported to a row. Output is
    specified with methods of the following form:

    att_X(self, *args):
    should return a scalar quantity corresponding to name X

    atts_Y(self, *args):
    should return a vector quantity corresponding to names at self.Y_headers

    The above methods will all be passed the same arguments (the arguments that
    are passed to to_row).
    """

    def headers(self):
        res = []
        for meth in sorted(self.attmethods()):
            res.append(meth.__name__[4:])

        for meth in sorted(self.attsmethods()):
            meth_headers = getattr(self, meth.__name__[5:]+'_headers')
            res += meth_headers

        return res

    def attmethods(self):
        for methname in dir(self):
            meth = getattr(self, methname)
            if callable(meth) and meth.__name__.startswith('att_'):
                yield meth

    def attsmethods(self):
        for methname in dir(self):
            meth = getattr(self, methname)
            if callable(meth) and meth.__name__.startswith('atts_'):
                yield meth

    def to_row(self, *args):
        row = []
        for attmeth in self.attmethods():
            row.append(attmeth(*args))

        for attsmeth in self.attsmethods():
            row += attsmeth(*args)

        return row





class DebuggerMonth(Base):
    """Encompasses information about a particular debugger during a particular
    month.
    """

    dbid = Column(Integer, ForeignKey("Debugger"), index=True)
    bugid = Column(Integer, ForeignKey("Bug"), index=True)

    # NB: some of these actually aren't dependent on any particular debugger... (e.g. eff size)
    nbugs = Column(Integer)
    nbugevents = Column(Integer)
    nirc_links = Column(Integer)
    nirc_messages_directed = Column(Integer)
    nirc_messages_undirected = Column(Integer)
    constraint_irc = Column() # TODO
    closeness_irc = Column()
    clustering_irc = Column()
    indegree_irc = Column(Integer)
    outdegree_irc = Column(Integer)
    betweenness_irc = Column()
    effective_size_irc = Column()
    efficiency_irc = Column()
    alter_churn_irc = Column(Integer)
    effective_size_churn_irc = Column(Integer)


    debugger = relationship("Debugger")
    bug = relationship("Bug")


    @classmethod
    def from_db_month(cls, debugger, month, session):
        # probably need to pass in graph too?
        pass

#class BugState(Base):
#    """The state of a bug at a particular month.
#    """
#
#    __tablename__ = 'bugstates'
#
#    id = Column(Integer, primary_key=True)
#    monthid = Column(Integer, ForeignKey("Month"), index=True)
#    bugid = Column(Integer, ForeignKey("Bug"), index=True)

class BugMonth(object):

    headers = []

    def __init__(self, bug, month):
        self.bug = bug
        self.month = month

    id_headers = ['bugid', 'day', 'month', 'year']
    def atts_id(self):
        return [self.bug.bzid,
        self.month.first.day,
        self.month.first.month,
        self.month.first.year,
        ]


    def atts_assignee(self):
        pass

    def atts_debuggers(self):
        pass




def save_bugmonth_vars(session, fname):
    f = open(fname, 'w')
    out = csv.writer(f)
    for month in session.query(Month):
        # load adjacency matrix for this month into iGraph
        netwk = month.get_network()
        for bugmonth in month.bugmonths(session):
            out.writerow(bugmonth.to_row(session))

    f.close()
