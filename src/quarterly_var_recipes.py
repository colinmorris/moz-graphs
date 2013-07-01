"""A collection of recipes for populating certain bugmonth variables. Namely,
the new 'quarterly' variables for calculation circa jun 2013.

I'm just collecting them here to make organization less insane.
"""
from src.bug_events import BugEvent

from src.bugmonth_variables import BugMonth
import logging
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Unicode, Boolean, Enum, Float, func, distinct
from src.bugs import Bug
from src.utils import museumpiece
from src.months import Quarter
from src.models import Chat


@museumpiece
def enrich_bugs_debuggers_graph_quarterly(session):
    """
    for each bugmonth:
        get the set of debuggers active on this bug during this month
        get their constraints etc. for the previous month and store them
        for each month before that:
            add their constraints etc. for that month to a running sum
        save the average
    """
    varnames = [
        'constraint', 'closeness', 'clustering', 'indegree', 'outdegree',
        'betweenness', 'effective_size', 'efficiency', 'alter_churn',
        'effective_size_churn',
                ]
    cum_varnames = ['alter_churn', 'effective_size_churn']

    from src.debuggers import Debugger
    from src.debuggerquarter import DebuggerQuarter
    bmcount = 0
    interval = 10
    for bm in session.query(BugMonth):
        bmcount += 1
        if bmcount % interval == 0:
            logging.info("Done %d bugmonths" % (bmcount)) # Off by one but I don't care
            interval = min(1000, interval*10)

        # 1) Get the debuggers for this bugmonth
        dbids = session.query(distinct(BugEvent.dbid)).\
            filter_by(bzid=bm.bugid).\
            filter(BugEvent.date >= bm.month.first).\
            filter(BugEvent.date <= bm.month.last)

        debuggers = [session.query(Debugger).filter_by(id=dbid[0]).scalar() for dbid in dbids]
        # Only take talkative debuggers
        debuggers = [db for db in debuggers if db.nirc > 0]
        # This actually maps variables names to the sum of averages over a particular
        # number of months. This is as silly as it sounds.
        varname_to_sum = dict((name, 0) for name in varnames)

        lastmonth = bm.month.prev(session)
        if lastmonth is None:
            continue
        currquarter = Quarter(last=lastmonth)

        #currmonth = bm.month.prev(session)
        found_prior = False
        nquarters = 0
        while currquarter is not None:

            # 1. Filter out debuggers that are no longer relevant because of time of entry into IRC network
            def relevant(d):
                entry = session.query(func.min(DebuggerQuarter.first)).filter_by(dbid=d.id).scalar()
                if entry is None:
                    #logging.warning("Debugger missing - shouldn't have happened... " + str(d))
                    return False
                return entry <= currquarter.first
            debuggers = filter(relevant, debuggers)
            if debuggers == []:
                break

            # 2. Get sums of vars for this quarter
            # varname_to_sum keeps a macro-average, this keeps the 'local average'
            local_sums = dict((name, 0) for name in varnames)
            for db in debuggers:
                #TODO: Make debuggerquarter. Ugh.
                dm = session.query(DebuggerQuarter).filter_by(dbid=db.id).\
                    filter_by(first=currquarter.first).scalar()
                if dm is None:
                    continue
                for varname in varnames:
                    local_sums[varname] += getattr(dm, varname) or 0 # This is kind of a cheat

            # 3 Convert those sums to avgs and add them to running sum
            for varname in varnames:
                avg = local_sums[varname]/len(debuggers)
                varname_to_sum[varname] += avg

            # 3.5 If this is the first iteration, then store the avgs in the prior month vars
            if not found_prior:
                found_prior = True
                for varname in varnames:
                    prior_name = 'bugs_debuggers_' + varname + '_prior_quarter'
                    value = varname_to_sum[varname]
                    setattr(bm, prior_name, value)

            # 4 Increment nquarters and take another step backward through months
            nquarters += 1
            currquarter = currquarter.prev()

        # 5 Set the past_monthly_avg and cumulative vars
        # 5.1 But if we didn't iterate over any months, then leave them as None
        if nquarters == 0:
            continue

        for varname in varnames:
            avg_name = 'bugs_debuggers_' + varname + '_past_quarterly_avg'
            cum_name = 'bugs_debuggers_' + varname + '_quarterly_cumulative'
            avg = varname_to_sum[varname]/(nquarters+0.0)
            setattr(bm, avg_name, avg)
            if varname in cum_varnames:
                setattr(bm, cum_name, varname_to_sum[varname])


    session.commit()




def enrich_bugs_debuggers_avgavg(session):
    """So called because these vars are averages and averages of averages.
    """
    import numpy
    vars = ['n_bugs_contributed_to', 'n_history_events_focal',
            'n_history_events_other', 'n_irc_links', 'n_irc_messages_directed',]
            #'n_irc_messages_undirected']

    bmcount = 0
    interval = 10
    for bm in session.query(BugMonth):
        bmcount += 1
        if bmcount % interval == 0:
            logging.info("Done %d bugmonths" % (bmcount)) # Off by one but I don't care
            interval = min(1000, interval*10)

        # 1) Get the debuggers for this bugmonth
        first = lambda tup: tup[0]
        dbids = map(first,
            session.query(distinct(BugEvent.dbid)).\
                filter_by(bzid=bm.bugid).\
                filter(BugEvent.date >= bm.month.first).\
                filter(BugEvent.date <= bm.month.last).all()
        )


        if dbids == []:
            # Nothing to do here
            continue

        currmonth = bm.month.prev(session)
        if currmonth is None:
            continue
        currquarter = Quarter(last=currmonth)
        found_prior = False
        nmonths = 0
        varname_to_vals = dict((name, []) for name in vars)
        while currquarter is not None:
            # Calculate stuff
            varname_to_local_arr = dict((name, []) for name in vars)

            # N bugs contributed to
            varname_to_vals['n_bugs_contributed_to'].append(0)
            for dbid in dbids:
                nbugs = session.query(distinct(BugEvent.bzid)).filter_by(dbid=dbid).\
                    filter(BugEvent.date <= currquarter.last).\
                    filter(BugEvent.date >= currquarter.first).count()
                if not found_prior:
                    varname_to_local_arr['n_bugs_contributed_to'].append(nbugs)
                varname_to_vals['n_bugs_contributed_to'][-1] += nbugs
            varname_to_vals['n_bugs_contributed_to'][-1] = \
                varname_to_vals['n_bugs_contributed_to'][-1] / float(len(dbids))

            # N history events focal
            if currquarter.last < bm.bug.reported:
                pass # Don't add anything to our accumulated list
            else:
                varname_to_vals['n_history_events_focal'].append(0)
                for dbid in dbids:
                    nevents = session.query(BugEvent).filter_by(dbid=dbid).\
                        filter(BugEvent.date <= currquarter.last).\
                        filter(BugEvent.date >= currquarter.first).\
                        filter_by(bzid=bm.bugid).count()
                    if not found_prior:
                        varname_to_local_arr['n_history_events_focal'].append(nevents)
                    varname_to_vals['n_history_events_focal'][-1] += nevents
                    varname_to_vals['n_history_events_focal'][-1] = \
                    varname_to_vals['n_history_events_focal'][-1] / float(len(dbids))

            # n history events other
            varname_to_vals['n_history_events_other'].append(0)
            for dbid in dbids:
                nevents = session.query(BugEvent).filter_by(dbid=dbid).\
                    filter(BugEvent.date <= currquarter.last).\
                    filter(BugEvent.date >= currquarter.first).\
                    filter(BugEvent.bzid != bm.bugid).count()
                if not found_prior:
                    varname_to_local_arr['n_history_events_other'].append(nevents)
                varname_to_vals['n_history_events_other'][-1] += nevents
            varname_to_vals['n_history_events_other'][-1] = \
                varname_to_vals['n_history_events_other'][-1] / float(len(dbids))

            # n_irc_links
            varname_to_vals['n_irc_links'].append(0)
            relchats = session.query(Chat).\
                filter(Chat.date <= currquarter.last).\
                filter(Chat.date >= currquarter.first)
            for dbid in dbids:
                nout = relchats.filter_by(p1=dbid).count()
                nin = relchats.filter_by(p2=dbid).count()
                if not found_prior:
                    varname_to_local_arr['n_irc_links'].append(nout+nin)
                varname_to_vals['n_irc_links'][-1] += nout + nin
            varname_to_vals['n_irc_links'][-1] = \
                varname_to_vals['n_irc_links'][-1] / float(len(dbids))

            # n_irc_messages_directed
            varname_to_vals['n_irc_messages_directed'].append(0)
            for dbid in dbids:
                nsent = session.query(func.sum(Chat.n)).\
                    filter_by(p1=dbid).\
                    filter(Chat.date <= currquarter.last).\
                    filter(Chat.date >= currquarter.first).scalar() or 0
                if not found_prior:
                    varname_to_local_arr['n_irc_messages_directed'].append(nsent)
                varname_to_vals['n_irc_messages_directed'][-1] += nsent
            varname_to_vals['n_irc_messages_directed'][-1] = \
                varname_to_vals['n_irc_messages_directed'][-1] / float(len(dbids))



            # Save prior variables on the first iteration
            if not found_prior:
                found_prior = True
                for varname in vars:
                    avg_name = 'bugs_debuggers_' + varname + '_prior_quarter_avg'
                    variance_name = 'bugs_debuggers_' + varname + '_prior_quarter_variance'
                    avg = numpy.mean(varname_to_local_arr[varname])
                    variance = numpy.var(varname_to_local_arr[varname])
                    setattr(bm, avg_name, avg)
                    setattr(bm, variance_name, variance)

            currquarter = currquarter.prev()
            nmonths += 1

        # Save aggregate variables
        for varname in vars:
            avg_name = 'bugs_debuggers_' + varname + '_past_quarterly_avg'
            cum_name = 'bugs_debuggers_' + varname + '_quarterly_cumulative'
            avg = numpy.mean(varname_to_vals[varname])
            cum = sum(varname_to_vals[varname])
            setattr(bm, avg_name, avg)
            setattr(bm, cum_name, cum)

    session.commit()






def enrich_bugs_debuggers_nograph(session):
    """Like below, but filling in the non-graph variables.

    Actually, we're just filling in n_debuggers and debugger_churn, because the
    others are special.
    """
    vars = ['n_debuggers', 'debugger_churn',] #'n_reported_bugs',
          #  'n_bugs_contributed_to', 'n_history_events_focal',
          #  'n_history_events_other', 'n_irc_links', 'n_irc_messages_directed',
          #  'n_irc_messages_undirected']
    bmcount = 0
    interval = 10
    for bm in session.query(BugMonth):
        bmcount += 1
        if bmcount % interval == 0:
            logging.info("Done %d bugmonths" % (bmcount)) # Off by one but I don't care
            interval = min(1000, interval*10)

        currmonth = bm.month.prev(session)
        prevmonth = currmonth.prev(session) if currmonth else None # Need this for churn
        found_prior = False
        nmonths = 0
        varname_to_sum = dict((name, 0) for name in vars)
        # TODO: Stop walking backwards when we get to the bug's reported date?
        while currmonth is not None:

            # These vars are heterogeneous enough that I'm gonna kind of deal with
            # them each individually rather than the more structured approach I've
            # taken elsewhere

            # N DEBUGGERS
            curr_debugger_ids = set(session.query(distinct(BugEvent.dbid)).\
                filter(BugEvent.date <= currmonth.last).\
                filter(BugEvent.date >= currmonth.first).\
                filter(BugEvent.bzid==bm.bugid).all())

            n_debuggers = len(curr_debugger_ids)
            varname_to_sum['n_debuggers'] += n_debuggers

            # DEBUGGER CHURN
            if prevmonth is None:
                debugger_churn = 0
            else:
                prev_debugger_ids = set(session.query(distinct(BugEvent.dbid)).\
                filter(BugEvent.date <= prevmonth.last).\
                filter(BugEvent.date >= prevmonth.first).\
                filter(BugEvent.bzid==bm.bugid).all())
                debugger_churn = len(curr_debugger_ids.difference(prev_debugger_ids))

            varname_to_sum['debugger_churn'] += debugger_churn



            # Set prior month vars
            if not found_prior:
                found_prior = True
                for (var, value) in varname_to_sum.items():
                    prior_name = 'bugs_debuggers_' + var + '_prior_quarter'
                    setattr(bm, prior_name, value)




            currmonth = prevmonth
            prevmonth = prevmonth.prev(session) if prevmonth else None
            nmonths += 1

        if nmonths == 0:
            continue

        # Now set sumulative and avg vars
        for (var, sum) in varname_to_sum.items():
            cum_name = 'bugs_debuggers_' + var + '_quarterly_cumulative'
            avg_name = 'bugs_debuggers_' + var + '_past_quarterly_avg'
            setattr(bm, cum_name, sum)
            denom = nmonths
            if var == 'debugger_churn':
                denom -= 1
            if denom == 0:
                continue
            setattr(bm, avg_name, sum/float(denom))

    session.commit()



def enrich_bugs_debuggers_lastbandaid(session):
    import numpy
    from src.undirected_chats import UndirectedChat
    from src.debuggers import Debugger
    rep = session.query(Bug)
    undir = session.query(func.sum(UndirectedChat.n))
    for bm in session.query(BugMonth):

        # 1) Get the debuggers for this bugmonth
        first = lambda tup: tup[0]
        # This looks wrong. Why am I looking at debuggers of the prev month?
        dbids = map(first,
            session.query(distinct(BugEvent.dbid)).\
                filter_by(bzid=bm.bugid).\
                filter(BugEvent.date >= bm.month.first).\
                filter(BugEvent.date <= bm.month.last).all()
        ) # TODO: Make subset just for chats

        # dbid -> date of first irc event
        chatters = {}
        for dbid in dbids:
            db = session.query(Debugger).filter_by(id=dbid).scalar()
            if db.nirc == 0:
                continue
            first_irc = session.query(func.min(UndirectedChat.day)).filter_by(dbid=dbid).scalar()
            if first_irc is None:
                logging.warning(u"Debugger %s has nirc > 0 but no rows in undirectedchats" % (unicode(db)))
            else:
                chatters[dbid] = first_irc



        if dbids == []:
            # Nothing to do here
            continue

        currmonth = bm.month.prev(session)
        found_prior = False
        nmonths = 0
        rep_avgs = [] # TODO: Stopping condition for each of these
        chat_avgs = []
        while currmonth is not None:
            # Calculate stuff
            nmonths += 1
            reps = []
            chats = []

            # Reported
            for dbid in dbids:
                nreps = rep.filter_by(reporter_id=dbid).\
                    filter(Bug.reported <= currmonth.last).\
                    filter(Bug.reported >= currmonth.first).count()
                reps.append(nreps)

            # Undirected messages
            # Exclude debuggers s.t. first irc month > currmonth
            for (dbid, first) in chatters.iteritems():
                if first > currmonth.last:
                    continue
                nchats = undir.filter_by(dbid=dbid).\
                    filter(UndirectedChat.day <= currmonth.last).\
                    filter(UndirectedChat.day >= currmonth.first).scalar()
                chats.append(nchats or 0)

            rep_avg = numpy.mean(reps)
            chat_avg = numpy.mean(chats)
            rep_avgs.append(rep_avg)
            chat_avgs.append(chat_avg)
            if not found_prior:
                found_prior= True
                bm.bugs_debuggers_n_reported_bugs_prior_quarter_avg = rep_avg
                bm.bugs_debuggers_n_reported_bugs_prior_quarter_variance = numpy.var(reps)

                bm.bugs_debuggers_n_irc_messages_undirected_prior_quarter_avg = chat_avg
                bm.bugs_debuggers_n_irc_messages_undirected_prior_quarter_variance = numpy.var(chats)

            currmonth = currmonth.prev(session)

        if nmonths == 0:
            continue
        # Set non-prior vars
        bm.bugs_debuggers_n_reported_bugs_cumulative = sum(rep_avgs)
        bm.bugs_debuggers_n_reported_bugs_past_quarterly_avg = numpy.mean(rep_avgs)

        bm.bugs_debuggers_n_irc_messages_undirected_past_quarterly_avg = numpy.mean(chat_avgs)
        bm.bugs_debuggers_n_irc_messages_undirected_cumulative = sum(chat_avgs)

    session.commit()
