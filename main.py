"""A playground from which to execute various table-altering functions,
like populate_X, or update_X.

I'm not going to put in a bunch of command line flags and stuff. I expect
to just call this like `python -i main.py`, and then do all the importing
and running that I need to do interactively. (The code I'm dealing with
here is mostly "run-once".
"""
from src.bugs import Bug

__author__ = 'colin'

from src import utils
import sys
import logging

def bug_reported_monkeypatch(session):
    """This is not a correct usage of the term "monkey patch". I just like it
    because it sounds funny.

    Anyways, add in the reported attribute to bugs, which was accidentally
    ommitted when populating.
    """
    from src import sqla
    import csv

    f = utils.open_data_file("bug_summary.csv")
    f.readline()
    reader = csv.reader(f)
    for row in reader:
        br = sqla.BugRow(row)

        bug = session.query(Bug).filter_by(bzid=br.bug).scalar()
        bug.reported = br.reported

    session.commit()

session = utils.get_session()

##### A bunch of silly little convenience methods intended to be run interactively (ideally only once) ######
def pop_bs():
    from src.bugstate import populate_bugstates
    populate_bugstates(session, False, 15)

def add_ids():
    from src.bugs import Bug
    Bug.add_assignee_ids(session)

def add_comments(interval=0):
    from src.bug_events import BugEvent
    BugEvent.scrape_comment_events(session, interval)


def pop_bm():
    from src.bugmonth_variables import populate_bugmonths, enrich_assignee
    #populate_bugmonths(session)
    enrich_assignee(session)
    print "Enriched assignees"
    session.commit()

def populate_dm():
    from src.debuggermonth import populate_debuggermonths
    populate_debuggermonths(session)
    print "Populated debuggermonths"
    session.commit()

def delete_bd_graph():
    from src.bugmonth_variables import BugMonth
    varnames = [
        'constraint', 'closeness', 'clustering', 'indegree', 'outdegree',
        'betweenness', 'effective_size', 'efficiency', 'alter_churn',
        'effective_size_churn',
                ]
    cum_varnames = ['alter_churn', 'effective_size_churn']

    hitlist = set([])
    for varname in varnames:
        prior_name = 'bugs_debuggers_' + varname + '_prior_month'
        hitlist.add(prior_name)
        avg_name = 'bugs_debuggers_' + varname + '_past_monthly_avg'
        hitlist.add(avg_name)
        cum_name = 'bugs_debuggers_' + varname + '_cumulative'
        if varname in cum_varnames:
            hitlist.add(cum_name)

    for bm in session.query(BugMonth):
        for varname in hitlist:
            setattr(bm, varname, None)

    session.commit()


logger = logging.getLogger()
logger.setLevel('INFO')
FORMAT = '%(asctime)s [%(levelname)s]: %(message)s'
logging.basicConfig(format=FORMAT)

#from src.debuggerquarter import *

import src.quarterly_var_recipes as q
import src.debuggermonth as dm
import src.debuggerquarter as dq
import src.bugmonth_variables as bmv

#dm.add_constraint(session)
bmv.enrich_bugs_debuggers_constraint(session)

#dq.populate_debuggerquarters(session)
#q.enrich_bugs_debuggers_graph_quarterly(session)
#q.enrich_bugs_debuggers_avgavg(session)
#q.enrich_bugs_debuggers_nograph(session)
#q.enrich_bugs_debuggers_lastbandaid(session)

#populate_debuggerquarters(session)
#bandaid(session)

#from src.bugmonth_variables import enrich_bugs_debuggers_avgavg as avgavg # underway now YOUAREHERE

#from src.bugmonth_variables import enrich_bugs_debuggers_graph as bandaid
#from src.bugmonth_variables import enrich_bugs_debuggers_nograph as bandaid
#from src.bugmonth_variables import enrich_assignee_lastbandaid as lastbandaid
#from src.bugmonth_variables import assignee_nbugs_bandaid as bandaid
#from src.bugs import Bug
#from src.undirected_chats import populate_undirected as pop

#add = lambda : Bug.add_reporter_ids(session)