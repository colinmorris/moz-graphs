"""
We freeze all the variables described in "Debugger variables.docx" in a
single table, which we build up incrementally, and which is described in
 this module.

(The final step will be to export the table to a csv file.)

Overall idea: create skeleton rows with populate_bugmonths and then write a bunch
of functions to fill in fields piece by piece
"""
from src.bugs import Bug

__author__ = 'colin'

from alchemical_base import Base
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Unicode, Boolean, Enum, Float, func, distinct
from sqlalchemy.orm import backref, relationship
from months import Month
from bugstate import BugState
from bug_events import BugEvent
from models import Chat
import datetime

MONTHDELTA = datetime.timedelta(days=28)

class BugMonth(Base):

    GRAPH_METRICS = ["constraint", "closeness", "clustering",
                     "indegree", "outdegree", "betweenness", "effective_size",
                     ]

    __tablename__ = 'bugmonths'

    id = Column(Integer, primary_key=True)

    bugid = Column(ForeignKey('bugs.bzid'))
    monthid = Column(ForeignKey('months.id'))
    assigneeid = Column(ForeignKey('debuggers.id'))
    bsid = Column(ForeignKey('bugstates.id'))

    # __OUTCOME__
    status = Column(Enum(*BugState.STATUSES)) # UNCONFIRMED NEW ASSIGNED VERIFIED etc.
    resolution = Column(Enum(*BugState.RESOLUTIONS))

    # __CHARACTERISTICS__
    nmonths_since_reported = Column(Integer)
    importance = Column(String)
    n_dependencies = Column(Integer)
    n_blocks = Column(Integer)
    n_duplicates = Column(Integer)
    platform = Column(String)
    product = Column(String)
    assigned = Column(Boolean)


    # __ASSIGNEE__
    assignee_nbugs_prior_month = Column(Integer)
    assignee_nbugs_past_monthly_avg = Column(Float)
    assignee_nbugs_past_cumulative = Column(Integer)

    assignee_nhistoryevents_focal_prior_month = Column(Integer)
    assignee_nhistoryevents_focal_past_monthly_avg = Column(Float)
    assignee_nhistoryevents_focal_past_cumulative = Column(Integer)

    assignee_nhistoryevents_other_prior_month = Column(Integer)
    assignee_nhistoryevents_other_past_monthly_avg = Column(Float)
    assignee_nhistoryevents_other_past_cumulative = Column(Integer)


    assignee_nirc_links_prior_month = Column(Integer)
    assignee_nirc_links_past_monthly_avg = Column(Float)
    assignee_nirc_links_past_cumulative = Column(Integer)

    assignee_nirc_sent_prior_month = Column(Integer)
    assignee_nirc_sent_past_monthly_avg = Column(Float)
    assignee_nirc_sent_past_cumulative = Column(Integer)

    assignee_nirc_received_prior_month = Column(Integer)
    assignee_nirc_received_past_monthly_avg = Column(Float)
    assignee_nirc_received_past_cumulative = Column(Integer)

    # TODO: Graph stuff related to assignee


    # __BUG'S DEBUGGERS__
    ndebuggers_prior_month = Column(Integer)
    ndebuggers_past_monthly_average = Column(Float)
    ndebuggers_past_cumulative = Column(Integer)

    # TODO: Debugger churn

    # TODO: Is this overall? And fill in average variance etc. etc.
    nreported_bugs_prior_month = Column(Integer)

    #debuggers_bugs_contributedto

    bug = relationship("Bug")
    month = relationship("Month")
    assignee = relationship("Debugger")
    state = relationship("BugState")


    # Vars just used to make calculation of other vars easier, not used directly.

    # Float so that we can use half-months. Or maybe even other fractions?
    _age_in_months = Column(Float)



def populate_bugmonths(session):
    """TODO: Documentme
    """
    for month in session.query(Month):
        for bug in session.query(Bug):
            state = session.query(BugState).filter_by(monthid=month.id).\
                filter_by(bugid=bug.id).first()
            if state is None:
                continue
            bm = BugMonth(bugid=bug.id,
                monthid=month.id,
                assigneeid=bug.assignee_id,
                bsid=state.id,
            )
            session.add(bm)

    session.commit()

def enrich_outcome_characteristics(session):
    for bm in session.query(BugMonth):
        bm.status = bm.state.status
        bm.resolution = bm.state.resolution

        # TODO: This is rounding down. Is this desired behaviour?
        bm.nmonths_since_reported = (bm.month.first - bm.bug.reported).days / 28
        bm.importance = bm.state.importance
        bm.n_dependencies = bm.state.n_dependencies
        bm.n_blocks = bm.state.n_blocks
        bm.n_duplicates = bm.state.n_duplicates
        bm.platform = bm.state.platform
        bm.product = bm.state.product
        bm.assigned = bm.state.assigned

    session.commit()

def enrich_assignee(session):
    for bm in session.query(BugMonth):
        ass = bm.assignee
        if ass is None:
            continue


        bm._age_in_months = (bm.month.first - bm.bug.reported).days / 28.0

        ### Assignee's history w the focal bug
        focal_history = session.query(BugEvent).filter_by(dbid=ass.id).\
            filter_by(bug=bm.bug)

        bm.assignee_nhistoryevents_focal_prior_month = focal_history.\
            filter(BugEvent.date < bm.month.first).\
            filter(BugEvent.date >= bm.month.first-MONTHDELTA).count()

        bm.assignee_nhistoryevents_focal_past_cumulative = focal_history.\
        filter(BugEvent.date < bm.month.first).count()

        bm.assignee_nhistoryevents_focal_past_monthly_avg = \
            bm.assignee_nhistoryevents_focal_past_cumulative / bm._age_in_months

        ### Assignee's history w other bugs
        other_history = session.query(BugEvent).filter_by(dbid=ass.id).\
            filter(BugEvent.bug!=bm.bug)

        bm.assignee_nhistoryevents_other_prior_month = other_history.\
            filter(BugEvent.date < bm.month.first).\
            filter(BugEvent.date >= bm.month.first-MONTHDELTA).count()

        bm.assignee_nhistoryevents_other_past_cumulative = other_history.\
            filter(BugEvent.date < bm.month.first).count()

        bm.assignee_nhistoryevents_other_past_monthly_avg =\
            bm.assignee_nhistoryevents_other_past_cumulative / bm._age_in_months

        ### Sent and received IRC messages
        sent = session.query(Chat.id).filter_by(db1=ass)
        recvd = session.query(Chat.id).filter_by(db2=ass)

        bm.assignee_nirc_sent_prior_month = sent.\
            filter(Chat.date < bm.month.first).\
            filter(Chat.date >= bm.month.first-MONTHDELTA).count()
        bm.assignee_nirc_received_prior_month = recvd.\
            filter(Chat.date < bm.month.first).\
            filter(Chat.date >= bm.month.first-MONTHDELTA).count()

        bm.assignee_nirc_sent_past_cumulative = sent.filter(Chat.date < bm.month.first).count()
        bm.assignee_nirc_received_past_cumulative = recvd.filter(Chat.date < bm.month.first).count()

        bm.assignee_nirc_sent_past_monthly_avg = \
            bm.assignee_nirc_sent_past_cumulative / bm._age_in_months
        bm.assignee_nirc_received_past_monthly_avg = \
            bm.assignee_nirc_received_past_cumulative / bm._age_in_months


        ### Number of links (i.e. distinct conversational partners)
        semes = session.query(distinct(Chat.p1)).filter_by(p2=bm.assignee.id)
        ukes = session.query(distinct(Chat.p2)).filter_by(p1=bm.assignee.id)

        bm.assignee_nirc_links_prior_month =\
            semes.filter(Chat.date < bm.month.first).filter(Chat.date >= bm.month.first-MONTHDELTA).count()+\
            ukes.filter(Chat.date < bm.month.first).filter(Chat.date >= bm.month.first-MONTHDELTA).count()

        bm.assignee_nirc_links_past_cumulative =\
            semes.filter(Chat.date < bm.month.first).count() +\
            semes.filter(Chat.date < bm.month.first).count()

        bm.assignee_nirc_links_past_monthly_avg =\
            bm.assignee_nirc_links_past_cumulative / bm._age_in_months

def enrich_assignee_graph(session):
    """
    for each month:
        get the graph for that month
        for each bug active during this month:
            get the variables related to that bug's assignee wrt the current graph
            and save them
    """
    pass


def priormonth(query, currmonth, cls):
    return query.filter()



#if __name__ == '__main__':
#    session = utils.get_session()
#    populate_bugmonths(session)
