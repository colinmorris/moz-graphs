__author__ = 'colin'

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float, distinct
from sqlalchemy.orm import relationship
from src.alchemical_base import Base
from src.months import Month
from src.bugs import Bug

# TODO: Add an import of this module to the two relevant spots in utils.py and run an alembic migration

class DebuggerMonth(Base):
    """This is a highly artifical table that basically just exists because it makes
    certain calculations easier and more efficient.

    Stores information about a debugger's position in the network during a given month.

    (In practice, the table will probably only contain debuggers who are ever assigned
    to a bug.)
    """

    __tablename__= 'debuggermonths'

    dbid = Column(ForeignKey("debuggers.id"))
    monthid = Column(ForeignKey('months.id'))

    GRAPH_METRICS = ["constraint", "closeness", "clustering",
                     "indegree", "outdegree", "betweenness", "effective_size",
                     ]

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
    # This is a mix of code and pseudo-code
    assignee_ids = set(session.query(distinct(Bug.assignee_id)).all())

    for month in session.query(Month):
        graph = # TODO: get graph corresponding to given month
        for debugger in graph.debuggers:
            if debugger.id in assignee_ids:
                dm = DebuggerMonth(dbid=debugger.id, monthid=month.id)
                # Fill in the graph stuff
                session.add(dm)

    session.commit()

