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



import src.bugmonth_variables as bm
bandaid = bm.assignee_nbugs_bandaid
enrich = bm.enrich_bugcontext_graph