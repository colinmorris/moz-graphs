__author__ = 'colin'

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float, distinct
from sqlalchemy.orm import relationship
from src.alchemical_base import Base
from src.months import Month, Quarter
from src.bugs import Bug
from src.mozgraph import MozGraph, MozIRCGraph
from src.bug_events import BugEvent
from src.utils import *

# TODO: Add an import of this module to the two relevant spots in utils.py and run an alembic migration

class DebuggerQuarter(Base):
    """This is a highly artifical table that basically just exists because it makes
    certain calculations easier and more efficient.

    Stores information about a debugger's position in the network during a given quarter.

    (In practice, the table will probably only contain debuggers who have touched a bug)

    If a debugger is not active on IRC during a particular month, then there will not
    be a corresponding DM for that (month, debugger) pair.
    """

    __tablename__= 'debuggerquarters'

    id = Column(Integer, primary_key=True)

    dbid = Column(ForeignKey("debuggers.id"), index=True)
    first = Column(Date, index=True)
    #monthid = Column(ForeignKey('months.id'))

    # NB: Unless otherwise specified, these are all with respect to the IRC network rather than the full network
    constraint = Column(Float)
    closeness = Column(Float)
    clustering = Column(Float)
    indegree = Column(Integer)
    outdegree = Column(Integer)
    betweenness = Column(Float)
    effective_size = Column(Float)
    efficiency = Column(Float)
    churn = Column(Integer) # DEAD
    alter_churn = Column(Integer)
    nreported = Column(Integer)
    effective_size_churn = Column(Integer)

    debugger = relationship("Debugger")
    #month = relationship('Month')

def bandaid(session):
    """Fill in efficiency, alter_churn, and effective_size_churn

    To do this, we need to compare graphs to the ones immediately preceding them.
    For quarters, this is doubleplus tricky.
    """
    from bugmonth_variables import monthpairs
    months = session.query(Month)
    lastgraph = None
    graph = None
    first = True # Do something special on the first iter
    for (lastmonth, month) in monthpairs(months, offset=3):

        graph = MozIRCGraph(month, session)
        if lastgraph is None: # This should only happen on the firs iter
            assert first
            lastgraph = MozIRCGraph(lastmonth, session)


        for (dbid, vertex) in graph.dbid_to_vertex.iteritems():
            dm = session.query(DebuggerQuarter).filter_by(dbid=dbid).filter_by(first=month.first).\
                scalar()
            if dm is None:
                continue

            dm.efficiency = graph.efficiency(vertex)
            dm.alter_churn = MozGraph.alter_churn(dbid, lastgraph, graph)
            dm.effective_size_churn = MozGraph.effective_size_churn(dbid, lastgraph, graph)

            # I have no idea why this was here, but it seems wrong...
#            if first:
#                dm.efficiency = lastgraph.efficiency(vertex)
#                dm.alter_churn = None
#                dm.effective_size_churn = None

        first = False


        lastgraph = graph
    session.commit()

def populate_debuggerquarters(session):
    # For efficiency, we restrict our search to those debuggers who have touched a bug in some way
    first = lambda tup:tup[0]
    assignee_ids = set(map(first, session.query(distinct(Bug.assignee_id)).all()))
    bugeventful_ids = set(map(first, session.query(distinct(BugEvent.dbid)).all()))

    bugtouchers = set.union(assignee_ids, bugeventful_ids)

    n = 0
    for month in session.query(Month):
        ## XXX testing deleteme
        if month.first <= datetime.date(2008, 6, 4) <= month.last:
            print "asdff 233 should be in here"
        else:
            continue
        quarter = Quarter(first=month)
        graph = MozIRCGraph.load(quarter, session)
        print "Graph with %d vertices" % (len(graph.dbid_to_vertex))
        for (dbid, vertex) in graph.dbid_to_vertex.iteritems():
            if dbid not in bugtouchers:
                continue
            dq = DebuggerQuarter(dbid=dbid, first=quarter.first)

            dq.constraint = vertex.constraint()[0]
            dq.closeness = vertex.closeness()
            dq.clustering = graph.g.transitivity_local_undirected([vertex.index])[0]
            dq.indegree = vertex.indegree()
            dq.outdegree = vertex.outdegree()
            dq.betweenness = vertex.betweenness()
            dq.effective_size = graph.effective_size(vertex)

            session.add(dq)
            n += 1

    print "Added %d dms" % (n)
    #session.commit()

