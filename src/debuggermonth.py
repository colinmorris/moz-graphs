__author__ = 'colin'

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float, distinct
from sqlalchemy.orm import relationship
from src.alchemical_base import Base
from src.months import Month
from src.bugs import Bug
from src.mozgraph import MozGraph, MozIRCGraph
from src.bug_events import BugEvent
from src.utils import *

# TODO: Add an import of this module to the two relevant spots in utils.py and run an alembic migration

class DebuggerMonth(Base):
    """This is a highly artifical table that basically just exists because it makes
    certain calculations easier and more efficient.

    Stores information about a debugger's position in the network during a given month.

    (In practice, the table will probably only contain debuggers who have touched a bug)

    If a debugger is not active on IRC during a particular month, then there will not
    be a corresponding DM for that (month, debugger) pair.
    """

    __tablename__= 'debuggermonths'

    id = Column(Integer, primary_key=True)

    dbid = Column(ForeignKey("debuggers.id"))
    monthid = Column(ForeignKey('months.id'))

    GRAPH_METRICS = ["constraint", "closeness", "clustering",
                     "indegree", "outdegree", "betweenness", "effective_size",
                     ]

    # NB: Unless otherwise specified, these are all with respect to the IRC network rather than the full network
    constraint = Column(Float)
    closeness = Column(Float)
    clustering = Column(Float)
    indegree = Column(Integer)
    outdegree = Column(Integer)
    betweenness = Column(Float)
    effective_size = Column(Float)
    # TODO
    efficiency = Column(Float) # TODO: This is hard to calcualte. Let's leave it for later.
    churn = Column(Integer) # TODO: This should be on the full graph, not on just the IRC graph
    nreported = Column(Integer) # TODO: Not IRC

    debugger = relationship("Debugger")
    month = relationship('Month')

    def past_monthly_avg(self, var, session):
        """
        :param var: one of "constraint", "closeness" etc.
        :param session:
        :return: The average of var over the current month and all previous months
            up to when the debugger first entered the IRC network.
        """
        pass


@museumpiece
def populate_debuggermonths(session):
    # For efficiency, we restrict our search to those debuggers who have touched a bug in some way
    first = lambda tup:tup[0]
    assignee_ids = set(map(first, session.query(distinct(Bug.assignee_id)).all()))
    bugeventful_ids = set(map(first, session.query(distinct(BugEvent.dbid)).all()))

    bugtouchers = set.union(assignee_ids, bugeventful_ids)

    for month in session.query(Month):
        graph = MozIRCGraph.load(month, session)
        for (dbid, vertex) in graph.dbid_to_vertex.iteritems():
            if dbid not in bugtouchers:
                continue
            dm = DebuggerMonth(dbid=dbid, monthid=month.id)

            dm.constaint = vertex.constraint()
            dm.closeness = vertex.closeness()
            dm.clustering = graph.g.transitivity_local_undirected([vertex.index])[0]
            dm.indegree = vertex.indegree()
            dm.outdegree = vertex.outdegree()
            dm.betweenness = vertex.betweenness()
            dm.effective_size = graph.effective_size(vertex)

            session.add(dm)

    session.commit()

