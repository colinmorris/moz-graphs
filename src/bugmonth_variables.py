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
from mozgraph import MozGraph, MozIRCGraph
from debuggermonth import DebuggerMonth
from utils import *
import itertools
from collections import defaultdict

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
    assignee_nbugs_prior_month = Column(Integer) # TODO
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


    # __BUG CONTEXT__
    # bugs
    n_unresolved_bugs_prior_month = Column(Integer)
    n_active_bugs_prior_month = Column(Integer)
    n_reported_bugs_prior_month = Column(Integer)
    n_resolved_bugs_prior_month = Column(Integer)

    #debuggers, chat and events
    n_debuggers_prior_month = Column(Integer)
    n_IRC_members_prior_month = Column(Integer)
    n_history_events_prior_month = Column(Integer)
    n_directed_chats_prior_month = Column(Integer)
    n_undirected_chats_prior_month = Column(Integer) # TODO

    ############ EVERYTHING ABOVE HERE IS IMPLEMENTED AND IN THE TABLE ##############
    ############ (unless otherwise noted) ############
    ############ EVERYTHING BELOW IS NOT  ############

    # Network
    network_diameter_prior_month = Column(Integer)
    network_average_path_length_prior_month = Column(Float)
    network_density_prior_month = Column(Float)
    network_clustering_prior_month = Column(Float)

    ## ASSIGNEE GRAPH STUFF
    assignee_constraint_prior_month = Column(Float)
    assignee_constraint_past_monthly_avg = Column(Float)

    assignee_closeness_prior_month = Column(Float)
    assignee_closeness_past_monthly_avg = Column(Float)

    assignee_clustering_prior_month = Column(Float)
    assignee_clustering_past_monthly_avg = Column(Float)

    assignee_indegree_prior_month = Column(Integer)
    assignee_indegree_past_monthly_avg = Column(Float)

    assignee_outdegree_prior_month = Column(Integer)
    assignee_outdegree_past_monthly_avg = Column(Float)

    assignee_betweenness_prior_month = Column(Float)
    assignee_betweenness_past_monthly_avg = Column(Float)

    assignee_effectivesize_prior_month = Column(Float)
    assignee_effectivesize_past_monthly_avg = Column(Float)


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


def enrich_bug_network(session):
    # Hoo boy. This is kind of tricky. Basically, we need to keep two different
    # sets of running averages because of our overlapping month windows. Probably
    # should apply a higher level abstraction here, but whatever.
    even_bug_to_graphvars = defaultdict(lambda: defaultdict())
    odd_bug_to_graphvars = defaultdict(lambda: defaultdict())
    accs = [even_bug_to_graphvars, odd_bug_to_graphvars]
    acc_index = 0
    for (month, nextmonth) in monthpairs(session.query(Month).order_by(Month.first)):
        acc = accs[acc_index]
        acc_index = (acc_index+1)%2

        graph = MozGraph.load(month, session)

        for bug in session.query(Bug):
            bm = session.query(DebuggerMonth).filter_by(monthid=nextmonth.id).\
                filter_by(bugid=bug.bzid).scalar()
            vertex = graph[bug]
            # YOUAREHERE
            constraint = vertex.constraint()
            closeness = vertex.closeness()
            clustering = graph.g.transitivity_local_undirected([vertex])[0]
            #size = vertex.size()
            #efficiency
            #eff_size_churn

            acc[bug.id]['constraint'] = None




@museumpiece
def populate_bugmonths(session):
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

@museumpiece
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

@museumpiece
def assignee_nbugs_bandaid(session):
    """We didn't add n_bugs in enrich_assignee, so we do it here.
    """
    for bm in session.query(BugMonth):
        ass = bm.assignee
        if ass is None:
            continue

        currmonth = bm.month

        # All the unique bugs the assignee has worked on
        ass_bugs = session.query(distinct(BugEvent.bzid)).filter_by(dbid=ass.id)

        bm.assignee_nbugs_prior_month = ass_bugs.\
            filter(BugEvent.date >= currmonth.first-MONTHDELTA).\
            filter(BugEvent.date < currmonth.first).\
            count()

        cumul = ass_bugs.\
            filter(BugEvent.date < currmonth.first).\
            count()

        # The hilarity of having all these variables prefixed by ass_ is not wasted on me

        bm.assignee_nbugs_past_cumulative = cumul
        try:
            ass_age = (currmonth.first - ass.firstmonth.first).days/28
            bm.assignee_nbugs_monthly_avg = cumul/(ass_age+0.0)
        except (ZeroDivisionError, AttributeError):
            bm.assignee_nbugs_monthly_avg = None

    session.commit()


@museumpiece
def enrich_assignee(session):
    for bm in session.query(BugMonth):
        ass = bm.assignee
        if ass is None:
            # If this bug is unassigned, then all its fields shall be null.
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

@museumpiece
def enrich_bugcontext_nograph(session):
    """All these 'bug context' variables are calculated on a per month basis and
    are completely agnostic of the bug under consideration, so this should make
    calculating them a little easier (both less complex, and less time-consuming).
    """
    for (month, nextmonth) in monthpairs(session.query(Month).order_by(Month.first)):
        n_unresolved = session.query(BugState).\
            filter_by(monthid=nextmonth.id).\
            filter_by(resolution='---').count()

        n_active = session.query(distinct(BugEvent.bzid)).\
            filter(BugEvent.date >= month.first).\
            filter(BugEvent.date <= month.last).count()

        n_reported = session.query(Bug).\
            filter(Bug.reported >= month.first).\
            filter(Bug.reported <= month.last).count()

        n_resolved = session.query(Bug).\
            filter(Bug.resolved >= month.first).\
            filter(Bug.resolved <= month.last).count()

        n_directed_chats = session.query(Chat).\
            filter(Chat.date >= month.first).\
            filter(Chat.date <= month.last).count()

        # TODO: undirected

        n_history_events = session.query(BugEvent).\
            filter(BugEvent.date >= month.first).\
            filter(BugEvent.date <= month.last).count()

        semes = set(session.query(distinct(Chat.p1)).\
            filter(Chat.date >= month.first).\
            filter(Chat.date <= month.last).all())

        ukes = set(session.query(distinct(Chat.p2)).\
            filter(Chat.date >= month.first).\
            filter(Chat.date <= month.last).all())

        n_irc_members = len(set.union(semes, ukes))

        bugtouchers = set(session.query(BugEvent.dbid).\
            filter(BugEvent.date >= month.first).\
            filter(BugEvent.date <= month.last).all())

        n_debuggers = len(set.union(semes, ukes, bugtouchers))

        for bm in session.query(BugMonth).filter_by(month=nextmonth):
            bm.n_unresolved_bugs_prior_month = n_unresolved
            bm.n_active_bugs_prior_month = n_active
            bm.n_reported_bugs_prior_month = n_reported
            bm.n_resolved_bugs_prior_month = n_resolved

            bm.n_directed_chats_prior_month = n_directed_chats
            bm.n_debuggers_prior_month = n_debuggers
            bm.n_IRC_members_prior_month = n_irc_members
            bm.n_history_events_prior_month = n_history_events

    session.commit()


@museumpiece
def enrich_bugcontext_graph(session):
    for (month, nextmonth) in monthpairs(session.query(Month).order_by(Month.first)):
        graph = MozIRCGraph.load(month, session)

        diameter = graph.g.diameter(True, True)
        apl = graph.g.average_path_length(True, True)
        density = graph.g.density()
        clustering = graph.g.transitivity_undirected()
        for bm in session.query(BugMonth).filter_by(month=nextmonth):
            bm.network_diameter_prior_month = diameter
            bm.network_average_path_length_prior_month = apl
            bm.network_density_prior_month = density
            bm.network_clustering_prior_month = clustering

    session.commit()


def enrich_assignee_graph(session):
    """
    for each assignee (of any bug):
        for each month from beginning to end:
            accumulate a monthly average of things
            if there are any bugmonths with this debugger as assignee, save vars
    """
    from src.debuggers import Debugger
    for dbid in session.query(distinct(Bug.assignee_id)):
        debugger = session.query(Debugger).filter_by(id=dbid).scalar()
        firstmonth = debugger.firstmonth
        assert firstmonth is not None
        attr_to_total = {'constraint':0, 'closeness': 0, 'clustering':0}
        # Invariant: this refers to the number of months over which the above dict has
        # been accumulated
        nmonths = 0

        month_nextmonth = monthpairs(session.query(Month).\
        filter(Month.first >= firstmonth.first).\
        order_by(Month.first))
        for (month, nextmonth) in month_nextmonth:

            dm = session.query(DebuggerMonth).filter_by(dbid=dbid).filter_by(monthid=month.id).scalar()
            for attr in attr_to_total:
                # TODO: Fill me in. But I'm too mentally exhausted for this. This is rly non-trivial.
                raise NotImplementedError()

            # ASSERTION: we now have the constraint, closeness etc. for the current month
            # And the past monthly average available with attr_to_total

def monthpairs(months):
    """[Jan, Jan.5, Feb, Feb.5, Mar...] ->
       [ (Jan, Feb), (Jan.5, Feb.5), (Feb, Mar)...]

    Basically, walk through two version of an iterator, one fastforwarded by 2.
    In this context, it lets us get a sequence of tuples of months, such that each
    tuple has a month and the month that follows it contiguously.

    (We need to fastforward by 2 rather than just 1 because we have overlapping
    'months' - see months.py)

    Modified version of a neat little recipe stolen from Python's itertools docs.
    """
    a, b = itertools.tee(months)
    next(b, None)
    next(b, None)
    return itertools.izip(a, b)