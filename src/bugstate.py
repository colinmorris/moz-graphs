"""This module is concerned with storing the status of bugs at various moments in time (specifically, at the beginning
of each of our pseudo-months). We get this information by scraping bugzilla, and carefully applying changes to the bug
in reverse order.
"""
from src.bugs import Bug

__author__ = 'colin'

from alchemical_base import Base
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Unicode, Boolean, Enum
from sqlalchemy.orm import backref, relationship
from scraping import bug_scrape as bs
import time
import utils
#from graph import MonthSet
from months import MonthSet
import models

NAP_TIME = 2

class BugState(Base):
    """The state of a bug at the beginning of a particular month.

    In particular, we're concerned with data that's encoded in Bugzilla.
    """

    __tablename__ = 'bugstates'

    id = Column(Integer, primary_key=True)
    monthid = Column(Integer, ForeignKey("months.id"), index=True, nullable=False)
    bugid = Column(Integer, ForeignKey("bugs.bzid"), index=True, nullable=False)

    STATUSES = ("UNCONFIRMED", "NEW", "READY", "ASSIGNED", "REOPENED",
                "RESOLVED", "VERIFIED", "CLOSED")
    RESOLUTIONS = ("---", "FIXED", "INVALID", "WONTFIX", "DUPLICATE",
        "WORKSFORME", "INCOMPLETE", "SUPPORT", "EXPIRED", "MOVED")

    PRIORITIES = ("--", "P1", "P2", "P3", "P4", "P5")
    status = Column(Enum(*STATUSES))
    resolution = Column(Enum(*RESOLUTIONS))
    importance = Column(Enum(*PRIORITIES))
    n_dependencies = Column(Integer, default=0)
    n_blocks = Column(Integer, default=0)
    n_duplicates = Column(Integer, default=0)
    platform = Column(String)
    product = Column(String)
    assigned = Column(Boolean)


    bug = relationship("Bug")
    month = relationship("Month")

    def copy(self):
        """Return a BugState that contains copies of all this BugState's columns,
         except for id.
        """
        # Unfortunately we can't just import copy because this comes with all kinds
        # of nasty SQLA internals
        kwargs = {}
        for col in self.__table__.columns:
            if col.name == 'id':
                continue
            kwargs[col.name] = getattr(self, col.name)
        return BugState(**kwargs)

    def apply_delta(self, historyevent):
        """Apply the results of the given HistoryEvent _in reverse_.
        That is, remove what's added, and add what's removed.
        """
        if historyevent.what == 'Product':
            self.product = historyevent.removed
        elif historyevent.what == 'Platform':
            self.platform = historyevent.removed
        elif historyevent.what == 'Assignee':
            self.assigned = 'nobody@mozilla' not in historyevent.removed
        elif historyevent.what == 'Depends on':
            nremoved = len(historyevent.removed.strip().split())
            nadded = len(historyevent.added.strip().split())
            self.n_dependencies += (nremoved - nadded)
        elif historyevent.what == 'Blocks':
            nremoved = len(historyevent.removed.strip().split())
            nadded = len(historyevent.added.strip().split())
            self.n_blocks += (nremoved - nadded)
        elif historyevent.what == 'Duplicates':
            # AFAIK these can only be added, not removed. TODO: investigate?
            assert historyevent.removed is None
            # Can only be added one at a time
            self.n_duplicates -= 1


        elif historyevent.what == 'Status':
            assert self.status == historyevent.added, (
                """Expected our status to be what was added:
                %s
                but was:
                %s""" % (historyevent.added, self.status))
            self.status = historyevent.removed
        elif historyevent.what == 'Resolution':
            # This is rather tricky and might not work
            assert self.resolution == historyevent.added
            self.resolution = historyevent.removed
        elif historyevent.what == "Priority":
            assert self.importance == historyevent.added, (
                """Expected our importance to be what was added:
                %s
                but was:
                %s""" % (historyevent.added, self.importance))
            assert historyevent.removed in self.PRIORITIES
            self.importance = historyevent.removed
        else:
            #print "Ignoring " + str(historyevent)
            pass


    @classmethod
    def from_bugpage(cls, bp):
        """Return a BugState corresponding to a bug page (an object representing the
        results of scraping the current state of a bug on Bugzilla).
        """
        # Have to do this because of utterly bizarre way attr getter methods are done in BugPage...
        status, importance, platform, product = map(
            lambda att: bp._parse_attr(att),
            (bp.att_status(), bp.att_importance(), bp.att_platform(), bp.att_product())
        )

        # "status" is actually a one or two word string. if two words, the second is resolution
        status_tokens = status.split()
        if len(status_tokens) == 2:
            (status, resolution) = status_tokens
        elif len(status_tokens) == 1:
            # This is kind of a design decision. I'm okay with using moz's triple dashes for now.
            (status, resolution) = status, "---"
        else:
            status = status_tokens[0]
            resolution = status_tokens[1]
            # This should be VERIFIED DUPLICATE of bug whatever
            assert status_tokens[2] == "of", "Couldn't parse status " + str(status)
        assert status in cls.STATUSES, str(status) + " not in set of allowable statuses"
        assert resolution in cls.RESOLUTIONS, str(resolution) + " not in set of allowable resolutions"

        # Importance has to be parsed too
        importance = importance.split()[0]
        assert importance in cls.PRIORITIES

        return BugState(
            bugid = int(bp.id),
            status = status,
            importance = importance,
            n_dependencies = bp.att_n_depends(),
            n_blocks = bp.att_nblocks(),
            n_duplicates = bp.att_n_duplicates(),
            platform = platform,
            product = product,
            assigned = 'nobody@mozilla.org' not in bp.att_assigned_to(),
            resolution = resolution,

        )

class Delta(object):

    @staticmethod
    def from_dupid(dupid):
        """Given the id of a duplicate bug, create and return a delta from the
        perspective of the original (duplicated) bug, reflecting the addition
        of a new duplicate.
        """
        duppage = bs.BugPage(dupid)
        res = Delta()
        res.what = 'Duplicates'
        res.removed = None
        res.added = dupid
        res.date = utils.datify(duppage.att_resolved())
        res.datetime = utils.datetimeify(duppage.att_resolved())
        res.who = None
        return res

def duplicate_deltas(bugpage):
    res = []
    for dupid in bugpage.att_duplicates():
        try:
            res.append( Delta.from_dupid(dupid))
        except bs.ForbiddenBugException:
            print "Skipping forbidden bug " + str(dupid)
            pass
    return res

def populate_bugstates(session, abbrev=False, commit_interval=None):
    """Populate the bugstates tables.

    Go through each bug in All_bugs.csv and scrape its status and history from
    bugzilla. Then walk backwards through the history, saving a "snapshot" of the
    bug's status for each month in the month table that the bug existed at.

    If Abbrev is true, we only attempt 1 bug.
    """

    # This code is really, really tricky. I don't know how I could have written it more clearly.
    # Maybe it should have been broken up more into subroutines.

    bugs = utils.open_data_file('All_bugs.csv')
    bugs.readline()
    bzids = [line.split(',')[0] for line in bugs]

    # XXX: There are 27 bugs in All_bugs.csv that are not in the DB for semi-obscure reasons (history not available)
    missing_f = utils.open_data_file('missing.txt')
    missing_ids = [line.strip() for line in missing_f]
    missing_f.close()

    if abbrev:
        bzids = bzids[:4] # This number just keeps going up as I incrementally debug
    bugs.close()

    # All Months
    months = MonthSet.from_session(session)
    niters = 0
    for bzid in bzids:
        if bzid in missing_ids:
            print "Skipping missing id " + bzid
            continue
        # I don't know how I got away for so long without casting this...
        bzid = int(bzid)

        # Skip any ids already in the db
        if session.query(BugState).filter_by(bugid=bzid).count():
            #print "Skipping existing bug with id " + str(bzid)
            continue

        niters += 1

        if commit_interval and niters % commit_interval == 0:
            session.commit()

        try:
            bp = bs.BugPage(bzid)
        except bs.ForbiddenBugException:
            print "Skipping forbidden bug "  +str(bzid)
            continue
        base = BugState.from_bugpage(bp)

        base_bug = session.query(Bug).filter_by(bzid=base.bugid).scalar()

        reported = base_bug.reported
        # Is this even right? There won't always be an appropriate month to use
        base.month = months.monthafter(reported)
        history = bs.BugHistory(bzid)

        # Add duplicate events, which we have to construct ourself (they're not in the history)
        history.events += duplicate_deltas(bp)
        #print "DON'T FORGET TO PUT THIS BACK IN"
        
        history.sort(True)
        state = base.copy() # Holds the current state of the bug
        # The first month when this bug exists. (For our purposes) (This is in fact
        # the first month following the month containing the date the bug
        # was reported. BugStates describe the bug at the BEGINNING of the month.)
        first_month_index = months.after(reported)
        try:
            # XXX: I guess history is in reverse chrono order?
            last_change = history[0].date
        except IndexError:
            # If there are no changes, then the last change was the creation of the bug
            last_change = base_bug.reported

        # This is the index into months at which changes stop happening to the bug
        first_stagnant_month_index = months.after(last_change)
        # For each month where nothing further happens, we can just save the final bug state

        for month in months[first_stagnant_month_index:]:

            state.month = month
            session.add(state)
            # Need to do this every time, or they'll have identical ids
            state = state.copy()

        # OFF BY ONE ERRORS ARE THE DEVIL
        # -1 means the last element in a Python list. Do the math.
        # This actually fucks us on BOTH indices. Jesus.
        if first_stagnant_month_index == 0:
            continue
        terminus = first_month_index-1 if first_month_index else None

        # Find the index of the "first" (last chronologically) event that falls in the range
        # of months, and initialize our first delta correspondingly
        # Also, apply and then throw away all later deltas
        history_index = 0
        delta = None
        last_month = months[first_stagnant_month_index-1]
        first_month = months[first_month_index]
        for (index, event) in enumerate(history):
            history_index = index
            if event.date > last_month.last:
                state.apply_delta(event)
            elif event.date < first_month.first:
                # We skipped right through the relevant time period
                delta = event
                break
            else:
                # We've just hit on the first event we want to consider
                delta = event
                break
        else:
            # Ah, the rare "else on for loop" construct in the wild! Execute if we didn't break.
            # In this case, there are no deltas left
            history_index += 1
            assert len(history) == history_index



        for month in months[first_stagnant_month_index-1:terminus:-1]:
            while history_index < len(history) and month.first <= delta.date <= month.last:

                state.apply_delta(delta)
                history_index += 1
                if history_index == len(history):
                    break
                delta = history[history_index]

            state.month = month
            session.add(state)
            state = state.copy()

        session.commit()

    if not abbrev:
        session.commit()
