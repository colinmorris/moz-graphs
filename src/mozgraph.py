__author__ = 'colin'
from igraph import Graph
import pickle
import os
from config import *
from models import *
from bug_events import BugEvent

class MozGraphNode(object):
    # TODO
    def __init__(self):
        raise NotImplementedError

class MozGraph(object): # TODO: Should this inherit from igraph.Graph? Probably yes
    """A graph of bugs and debuggers during a particular month.
    """

class MozGraph(object):

    def __init__(self, month, session):
        self.g = Graph()
        self.session = session
        self.month = month
        self.start = month.first
        self.stop = month.last
        # Mapping ids of debuggers and bugs to their associated vertices
        self.dbid_to_vertex = {}
        self.bid_to_vertex = {}
        self._populate_irc()
        self._populate_bugevents()

    @staticmethod
    def fname(month):
        name = "month" + str(month.id)
        return os.path.join(ADJ_DIR, name)

    def _populate_irc(self):
        for chat in self._relevant_chats():
            self.add_chat(chat)

    def _relevant_chats(self):
        return self.session.query(Chat).filter(self.start <= Chat.date).\
        filter(Chat.date <= self.stop)

    def add_chat(self, chat):
        """Encode a chat between two debuggers in this graph.
        """
        seme = self.add_debugger(chat.db1)
        uke = self.add_debugger(chat.db2)
        self.add_edge(seme, uke, chat.n)

    def add_debugger(self, debugger):
        """Add this debugger to the graph if it's not already present, and return it.
        """
        if debugger.id not in self.dbid_to_vertex:
            index = len(self.g.vs)
            self.g.add_vertex(id=debugger.id)
            self.dbid_to_vertex[debugger.id] = self.g.vs[index]
        return self.dbid_to_vertex[debugger.id]

    def add_edge(self, v1, v2, weight=1):
        self.g.add_edge(v1, v2, weight=weight)

    def _populate_bugevents(self):
        for bugevent in self._relevant_bugevents():
            self.add_bugevent(bugevent)

    def _relevant_bugevents(self):
        return self.session.query(BugEvent).filter(self.start <= BugEvent.date).\
        filter(BugEvent.date <= self.stop)

    def add_bugevent(self, bugevent):
        debugger = self.add_debugger(bugevent.debugger)
        bug = self.add_bug(bugevent.bug)
        self.add_edge(debugger, bug)

    def add_bug(self, bug):
        if bug.id not in self.bid_to_vertex:
            index = len(self.g.vs)
            self.g.add_vertex(id=bug.bzid)
            self.bid_to_vertex[bug.bzid] = self.g.vs[index]
        return self.bid_to_vertex[bug.bzid]

    @classmethod
    def load(cls, month, session):
        """If we already have a pickled graph for this month, load it. Otherwise, load
        one from scratch.
        """
        try:
            res = cls.load_pickled(month)
            # We definitely can't use the pickled session or month though!
            # Though technically I think we only need the session for building the graph?
            res.session = session
            res.month = month
            return res
        except IOError:
            return MozGraph(month, session)


    # XXX: Yeah, it doesn't work. Rather than spending hours trying to save minutes of the
    # computer's time, let's just leave this and create graphs from scratch every time we
    # need them.
    @classmethod
    def load_pickled(cls, month):
        # XXX: I'm not sure if this will work, since we're pickling some weird stuff.
        # Maybe del session? And other weird stuff.
        f = open(cls.fname(month), 'w')
        return pickle.load(f)

    def save(self):
        f = open(self.fname(self.month), 'w')
        pickle.dump(self, f)
        f.close()

class MozIRCGraph(MozGraph):
    """A graph of just debuggers during a particular month. The only links are
    debugger-debugger, i.e. IRC links. Most of our bugmonth variables only use
    the IRC network.
    """

    pass