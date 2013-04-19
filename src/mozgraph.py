__author__ = 'colin'
from igraph import Graph
import pickle
import os
from config import *
from models import *
from bug_events import BugEvent
from months import Month
from debuggers import Debugger
from bugs import Bug

class MozGraph(object):

    def __init__(self, month, session):
        self.g = Graph(directed=True)
        self.session = session
        self.month = month
        self.start = month.first
        self.stop = month.last
        # Mapping ids of debuggers and bugs to their associated vertices
        self.dbid_to_vertex = {}
        self.bid_to_vertex = {}
        self._populate_irc()
        self._populate_bugevents()

    def __getitem__(self, item):
        """XXX: This isn't really parallel with the implementation of getitem
        in MozIRCGraph (which came earlier). Anyways, takes a Bug or Debugger.
        """
        if isinstance(item, Debugger):
            return self.dbid_to_vertex[item.id]

        elif isinstance(item, Bug):
            return self.bid_to_vertex[item.bzid]

        else:
            raise ValueError("Expected a debugger or bug")

    @classmethod
    def all(cls, session):
        """A convenience method returning an iterator of all MozGraphs, (ranging
        over all Months in the given session).
        """
        for month in session.query(Month):
            yield cls.load(month, session)

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

    def effective_size(self, vertex):
        """A variable that igraph doesn't come with, and we have to implement ourselves. 
        Effective size of a node is the number of alters the node has, 
        minus the average number of ties that each alter has to other alters: 
        n - 2t/2,
        where n is the number of alters, and t is the number of ties among them.
        """
        if not isinstance(vertex, int):
            vertex = vertex.index
        neighbours = set(self.g.neighbors(vertex))
        total_metaneighbours = 0
        for neigh in neighbours:
            neighbours_squared = set(self.g.neighbors(neigh))
            total_metaneighbours += len(set.intersection(neighbours, neighbours_squared))

        nneighbs = len(neighbours)
        avg_metaneighbs = total_metaneighbours/(nneighbs+0.0)

        return nneighbs - avg_metaneighbs

    def disconnected_alters(self, entity):
        """Return the disconnected alters wrt the vertex connected to the given
        entity (a Bug or Debugger).
        """
        disconns = set([])
        vertex = self[entity]
        neighbs = set(self.g.neighbors(vertex)) # American spelling is going to bite me in the ass
        for neigh in neighbs:
            neighbours_squared = set(self.g.neighbors(neigh))
            if len(set.intersection(neighbs, neighbours_squared)) == 0:
                disconns.add(neigh)

        return disconns

    @staticmethod
    def effective_size_churn(entity, earlygraph, lategraph):
        """Joel: Effective size churn (number of different disconnected alters from prior month)

        @param entity: A Bug or Debugger corresponding to our focal node
        @param earlygraph: The graph that comes earlier (the 'prior month')
        @param lategraph: The graph corresponding to the focal timeframe
        """
        # We're guaranteed that there's a node for the given entity in the earlygraph,
        # but not in the late graph
        prior_disconns = earlygraph.disconnected_alters(entity)
        try:
            curr_disconns = lategraph.disconnected_alters(entity)
        except KeyError:
            return 0
        news = curr_disconns.difference(prior_disconns)
        return len(news)

    @staticmethod
    def alter_churn(entity, earlygraph, lategraph):
        e_vertex = earlygraph[entity]
        try:
            l_vertex = lategraph[entity]
        except KeyError:
            return 0

        e_neighbors = set(e_vertex.neighbors())
        l_neighbours = set(l_vertex.neighbours())
        news = l_neighbours.difference(e_neighbors)
        return len(news)


    def efficiency(self, vertex):
        """Another var that igraph doesn't come with.

        = effective size normed by actual size (i.e. eff_size/n_neighbours)
        """
        # get all nodes linked to vertex
        neighbours = self.g.neighbors(vertex)
        return self.effective_size(vertex)/(len(neighbours)+0.0)




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

    def _populate_bugevents(self):
        pass

    def __getitem__(self, item):
        """Shorthand for accessing the vertex associated with a particular
        debugger id.
        """
        return self.dbid_to_vertex[item]
