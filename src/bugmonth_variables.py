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
import logging

MONTHDELTA = datetime.timedelta(days=28)

class BugMonth(Base):
    """A sort of join table on bugs and months.

    We only have a bugmonth for (bug, month) tuples where the bug exists at the
    given month.

    Unless otherwise specified, we're talking about the state of the focal bug at
    the _beginning_ of the focal month.
    """
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
    assignee_nbugs_prior_month = Column(Integer) # TODO (wait, I think I did? Check.)
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


    # Network
    network_diameter_prior_month = Column(Integer)
    network_average_path_length_prior_month = Column(Float)
    network_density_prior_month = Column(Float)
    network_clustering_prior_month = Column(Float)

    ############ EVERYTHING ABOVE HERE IS IMPLEMENTED AND IN THE TABLE ##############
    ############ (unless otherwise noted) ############
    ############ EVERYTHING BELOW IS NOT  ############

    # Bug network stuff
    bug_constraint_prior_month = Column(Float)
    bug_constraint_past_monthly_avg = Column(Float)

    bug_closeness_prior_month = Column(Float)
    bug_closeness_past_monthly_avg = Column(Float)

    bug_clustering_prior_month = Column(Float)
    bug_clustering_past_monthly_avg = Column(Float)

    bug_effective_size_prior_month = Column(Float)
    bug_effective_size_past_monthly_avg = Column(Float)

    bug_efficiency_prior_month = Column(Float)
    bug_efficiency_past_monthly_avg = Column(Float)

    bug_effective_size_churn_prior_month = Column(Float)
    bug_effective_size_churn_past_monthly_avg = Column(Float)


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

    assignee_efficiency_prior_month = Column(Float)
    assignee_efficiency_past_monthly_avg = Column(Float)

    assignee_alter_churn_prior_month = Column(Integer)
    assignee_alter_churn_past_monthly_avg = Column(Float)
    assignee_alter_churn_cumulative = Column(Integer)

    assignee_effective_size_churn_prior_month = Column(Float)
    assignee_effective_size_churn_past_monthly_avg = Column(Float)
    assignee_effective_size_churn_cumulative = Column(Float)


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


    # __BOOKKEEPING __
    # Vars just used to make calculation of other vars easier, not used directly.

    # Float so that we can use half-months. Or maybe even other fractions?
    _age_in_months = Column(Float)

@museumpiece
def enrich_assignee_graph(session):
    """
    Okay, so basically this sets assignee graph variables like
    assignee_constraint_prior_month etc.. To do this efficiently, we need to
    do some crazy, stupid, beautiful things.
    :param session: EWISOTT
    :return: None
    """
    varnames = [
        'constraint', 'clustering', 'indegree', 'outdegree', 'betweenness',
        'effectivesize', 'efficiency', 'alter_churn', 'effective_size_churn',
    ]
    cumulative_vars = ['alter_churn', 'effective_size_churn']

    #assignees = [db for db in session.query(distinct(Bug.assignee)).all() if db.nirc > 0]
    assignees = set([bug.assignee for bug in session.query(Bug)])
    logging.info("Found %d unique assignees" % (len(assignees)))
    assignees = [assignee for assignee in assignees if assignee is not None and assignee.nirc > 0]
    logging.info("Enriching %d assignees who also chat" % (len(assignees)))
    # dbid -> varname -> accumulated value
    even_db_to_graphvars = defaultdict(lambda: defaultdict(list))
    odd_db_to_graphvars = defaultdict(lambda: defaultdict(list))
    accs = [even_db_to_graphvars, odd_db_to_graphvars]
    acc_index = 0
    prev_months_counter = doublecount(1)
    db_to_offset_dicts = [defaultdict(int), defaultdict(int)] # TODO : Do I need this?
    graph = None
    next_graph = None
    found_ass_ids = set([])
    for (month, nextmonth) in monthpairs(session.query(Month).order_by(Month.first)):
        acc = accs[acc_index]
        db_to_offset = db_to_offset_dicts[acc_index]
        acc_index = (acc_index+1)%2
        npastmonths = prev_months_counter.next()

        # Need to create both graphs from scratch on the first iteration
        if graph is None:
            graph = MozGraph.load(month, session)
            next_graph = MozGraph.load(nextmonth, session)
        else:
            graph = next_graph
            next_graph = MozGraph.load(nextmonth, session)

        # STEP 1: update our counts for each assignee
        for ass in assignees:
            try:
                vertex = graph[ass]
                found_ass_ids.add(ass.id)
            except KeyError:
                # This assignee hasn't showed up in the network yet, so don't hit him with zeroes
                if ass.id not in found_ass_ids:
                    continue
                else:
                    # Joel sayeth: add zeroes for intermediate gaps
                    for varname in varnames:
                        acc[ass.id][varname].append(0)
                    continue

            # 1a: Calculate the vars associated with this debugger
            constraint = vertex.constraint()[0]
            clustering = graph.g.transitivity_local_undirected([vertex])[0]
            indegree = vertex.indegree()
            outdegree = vertex.outdegree()
            betweenness = vertex.betweenness()
            effectivesize = graph.effective_size(vertex)
            efficiency = graph.efficiency(vertex)
            alter_churn = MozGraph.alter_churn(ass, graph, next_graph)
            effective_size_churn = MozGraph.effective_size_churn(ass, graph, next_graph)

            # 1b: update accumulator
            for varname in varnames:
                value = locals()[varname]
                acc[ass.id][varname].append(value)


        # STEP 2: set bm variables for all bms on this month
        for bm in session.query(BugMonth).filter_by(month=nextmonth):
            if bm.assignee is None or bm.assignee.nirc == 0:
                continue

            for varname in varnames:
                prior_name = 'assignee_' + varname + '_prior_month'
                avg_name = 'assignee_' + varname + '_past_monthly_avg'
                cum_name = 'assignee_' + varname + '_cumulative'

                vals = acc[bm.assigneeid][varname]
                try:
                    setattr(bm, prior_name, vals[-1])
                    setattr(bm, avg_name, sum(vals)/float(len(vals)))
                except IndexError:
                    # The assignee hasn't entered the IRC network yet, but will later
                    setattr(bm, prior_name, None)
                    setattr(bm, avg_name, None)
                if varname in cumulative_vars:
                    setattr(bm, cum_name, sum(vals))

    session.commit()




@museumpiece
def enrich_bug_network(session):
    """
    This function fills in variables relating to the focal bug's position
    in the network, such as...
        bug_constraint_prior_month
        bug_effective_size_churn_past_monthly_avg
        etc.
    """

    # Hoo boy. This is kind of tricky. Basically, we need to keep two different
    # sets of running averages because of our overlapping month windows. Probably
    # should apply a higher level abstraction here, but whatever.
    # bug id -> variable -> float value
    even_bug_to_graphvars = defaultdict(lambda: defaultdict(float))
    odd_bug_to_graphvars = defaultdict(lambda: defaultdict(float))
    accs = [even_bug_to_graphvars, odd_bug_to_graphvars]
    acc_index = 0
    # This was totally fucking wrong. Each bug needs to have its own count based
    # on when it first appears in the network. You fucking suck.
    prev_months_counter = doublecount(1)
    # This will keep a non-positive int that should be added to prev_months_counter
    # to get the actual number of past months for a given bug.
    # Ugh and we have to keep 2 again. This is the worst.
    bug_to_offset_dicts = [defaultdict(int), defaultdict(int)]
    graph = None
    next_graph = None
    for (month, nextmonth) in monthpairs(session.query(Month).order_by(Month.first)):
        acc = accs[acc_index]
        acc_index = (acc_index+1)%2
        bug_to_offset = bug_to_offset_dicts[acc_index]
        npastmonths = prev_months_counter.next()

        # Need to create both graphs from scratch on the first iteration
        if graph is None:
            graph = MozGraph.load(month, session)
            next_graph = MozGraph.load(nextmonth, session)
        else:
            graph = next_graph
            next_graph = MozGraph.load(nextmonth, session)

        for bug in session.query(Bug):
            bm = session.query(BugMonth).filter_by(monthid=nextmonth.id).\
                filter_by(bugid=bug.bzid).scalar()
            if bm is None:
                # TODO? I should maybe ensure that this isn't getting decremented after a bug is first encountered
                bug_to_offset[bug.id] -= 1
                continue
            try:
                vertex = graph[bug]
            except KeyError:
                # If this bug doesn't have any activity this month, then all these
                # variables are null
                continue
            # see: https://bugs.launchpad.net/igraph/+bug/1170016 (I made it!)
            constraint = vertex.constraint()[0]
            closeness = vertex.closeness()
            clustering = graph.g.transitivity_local_undirected([vertex])[0]
            effective_size = graph.effective_size(vertex)
            efficiency = graph.efficiency(vertex)
            effective_size_churn = MozGraph.effective_size_churn(bug, graph, next_graph)

            varnames = [
                'constraint', 'clustering', 'closeness', 'effective_size',
                'efficiency', 'effective_size_churn',
            ]
            for varname in varnames:
                value = locals()[varname]
                #assert isinstance(value, float) or\
                #    isinstance(value, int), "Got type %s for var %s" % (type(value), varname)
                acc[bug.id][varname] += value
                prior_name = 'bug_' + varname + '_prior_month'
                avg_name = 'bug_' + varname + '_past_monthly_avg'

                setattr(bm, prior_name, locals()[varname])
                avg_denom = float(npastmonths + bug_to_offset[bug.id])
                avg = acc[bug.id][varname]/avg_denom

                setattr(bm, avg_name, avg)


    session.commit()




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

def doublecount(n):
    """0, 0, 1, 1, 2, 2, 3, 3,...
    """
    count1 = itertools.count(n)
    count2 = itertools.count(n)
    return roundrobin(count1, count2)

def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    # Recipe credited to George Sakkis
    pending = len(iterables)
    nexts = itertools.cycle(iter(it).next for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = itertools.cycle(itertools.islice(nexts, pending))