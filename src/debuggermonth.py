__author__ = 'colin'

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float, distinct
from sqlalchemy.orm import relationship
from src.alchemical_base import Base
from src.months import Month
from src.bugs import Bug
from src.mozgraph import MozGraph, MozIRCGraph
from src.bug_events import BugEvent

# TODO: Add an import of this module to the two relevant spots in utils.py and run an alembic migration

class DebuggerMonth(Base):
    """This is a highly artifical table that basically just exists because it makes
    certain calculations easier and more efficient.

    Stores information about a debugger's position in the network during a given month.

    (In practice, the table will probably only contain debuggers who have touched a bug)
    """

    __tablename__= 'debuggermonths'

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

    debugger = relationship("Debugger")
    month = relationship('Month')


def populate_debuggermonths(session):
    # For efficiency, we restrict our search to those debuggers who have touched a bug in some way
    assignee_ids = set(session.query(distinct(Bug.assignee_id)).all())
    bugeventful_ids = set(session.query(distinct(BugEvent.bzid)).all())

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
            raise NotImplementedError("Need to implement effective size!")
            dm.effective_size = None

            session.add(dm)

    session.commit()

