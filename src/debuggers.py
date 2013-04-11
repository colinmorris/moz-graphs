from sqlalchemy import Column, Integer, String, Unicode, ForeignKey, Boolean
from sqlalchemy.orm import relationship, backref
from src import utils, mozillians
from src.alchemical_base import Base
from src.models import VarColumn
#from src.bug_events import BugEvent
from src.months import MonthSet
from src.utils import museumpiece

__author__ = 'colin'

class Debugger(Base):
    __tablename__ = 'debuggers'

    abbrev_str = False

    id = Column(Integer, primary_key=True)
    # NTS: Need to remove uniqueness constraint?! Nevermind, I think I can have multiple null values.
    email = Column(String, unique=True, index=True)
    name = Column(Unicode)
    # A debugger can have many irc nicks (pointed to in the alias table).
    # This field represents a "canonical" IRC nick. If possible it's the one they list in
    # their `assigned to' field on BZ, or in their mozillian profile.
    # Otherwise, choose a random alias from chat logs (actually, we choose the shortest one)
    irc = Column(String)
    nirc = VarColumn(Integer, default=0) # Number of irc logs this debugger appears in
    nbz = VarColumn(Integer, default=0) # Number of bugzilla events they're involved in
    nassigned = VarColumn(Integer, default=0) # Number of bugs we find this dev is assigned to in our bugzilla data
    mozid = Column(Integer, ForeignKey('mozillians.id'))
    mozillians_searched = VarColumn(Boolean, default=False)

    # Whether or not this Debugger ever touches a bug (if False, they're just a chatter)
    bugtoucher = Column(Boolean)
    # The first month in which this debugger touches a bug.
    firstmonthid = Column(Integer, ForeignKey('months.id'))

    # I made this one-to-one, right? Shaky on syntax
    mozillian = relationship("Mozillian", uselist=False, backref=backref("debugger"))
    firstmonth = relationship("Month")

    linktype = None # Do I need this column?

    def __str__(self):
        return "Db<%s [%s] [%s] [:%s]>" % (self.id, self.name, self.email, self.irc)

    def __unicode__(self):
        if self.abbrev_str:
            return u"Db<%s" % (self.id)
        else:
            return u"Db<%s [%s] [%s] [:%s]>" % (self.id, self.name, self.email, self.irc)

    def __repr__(self):
        return str(self)

    @property
    def canon_name(self):
        return utils.canon_name(self.name)

    @staticmethod
    def parse_assignedto_str(assgn):
        """
        Given a string in the format of the "assigned to" field in Bugzilla,
        return a corresponding Debugger object with as many attributes filled
        in as can be inferred (up to name, e-mail, and irc)

        Returns None if assgn is "Nobody..."
        """
        # General long form:
        #   Christian :Biesinger (don't email me, ping me on IRC) <cbiesinger@gmail.com>
        #   e-mail is always* in angle brackets. Everything before <> is name, and user-provided (i.e. not guaranteed
        #   to follow a particular format, but the above is approximately conventional).

        #   * I lied. Not always in angle brackets. But if there are no angle brackets, then the whole string
        #       should just be an e-mail. No name.
        # Null assignee:
        #   Nobody; OK to take it and work on it <nobody@mozilla.org>
        if assgn == 'Nobody; OK to take it and work on it <nobody@mozilla.org>':
            return None

        # Case where we only have an e-mail
        if not assgn.endswith(">"):
            assert len(assgn.split()) == 1, assgn
            assert '@' in assgn
            return Debugger(email=assgn.strip())
        closeang = assgn.rindex(">")
        openang = assgn.rindex("<")
        email = assgn[openang+1:closeang]
        assert "@" in email, email
        name = assgn[:openang]

        # Colons are conventionally used to denote an irc nick
        colons = name.count(":")
        if colons > 1:
            print "[WARN] Don't know how to deal with this many colons!" + name + " Choosing the first mentioned nick."

        if colons >= 1:
            colindex = name.find(":")
            colend = name.find(" ", colindex)
            if colend == -1:
                colend = len(name)

            if colend - colindex > 1:
                irc = name[colindex+1:colend]
                # Strip trailing parens or brackets
                if irc[-1] in ']}>)':
                    irc = irc[:-1]
            else:
                irc = None
        else:
            irc = None

        basename = utils.rmparens(name)

        return Debugger(email=email, name=basename, irc=irc)


    @classmethod
    def get_or_create_debugger(cls, session, debugger):
        """As below, but takes a debugger object.
        """
        existing = session.query(cls).filter_by(email=debugger.email)
        if existing.count() == 1:
            return existing.scalar()

        session.add(debugger)
        session.flush()
        return debugger



    # XXX: This is mostly deprecated now. I don't like it.
    @classmethod
    def get_or_create(cls, session, **kwargs):
        """If a debugger exists matching the given keyword args, return it.
        Otherwise, create a new Debugger, add it to the session, and return
        it.
        """
        assert set(kwargs.keys()) == set(['email', 'name']), \
            "Currently only acccepting e-mail and name keyword args."
        existing = session.query(cls).filter_by(email=kwargs['email'])
        nexisting = existing.count()
        if nexisting == 1:
            ex = existing.scalar()
            if ex.name != kwargs['name']:
                utils.gate("Merge debugger with existing debugger with name %s, not matching given name %s."
                    % (ex.name, kwargs['name']))
            return ex
        elif nexisting > 1:
            raise ValueError("Given keyword arguments were not specific enough to" +
                             " get a unique result. Got " + str(nexisting))

        # Otherwise, create a debugger matching the given kwargs
        new_db = Debugger(**kwargs)
        new_db.moz_enrich(session)
        session.add(new_db)
        session.flush()
        return new_db

    def moz_enrich(self, session):
        self.mozillian = mozillians.Mozillian.fetch_lazy_mozillian("email", self.email, session)
        utils.gate("Merging " + str(self.mozillian) + "\nwith " + str(self))
        utils.mozillian_merge(self, self.mozillian)
        self.mozillians_searched = True

@museumpiece
def enrich_bugtouchers(session):
    from src.bug_events import BugEvent
    months = MonthSet.from_session(session)
    for db in session.query(Debugger):
        first_event = session.query(BugEvent).filter_by(dbid=db.id).order_by(BugEvent.date).first()
        if first_event is None:
            db.bugtoucher = False
        else:
            db.bugtoucher = True
            # TODO: Current month, or month after? Using month after now
            # to stay consistent with our implementation of bugs, but maybe
            # check with Joel on this?
            try:
                db.firstmonthid = months.monthafter(first_event.date).id
            except IndexError:
                db.bugtoucher = False

    session.commit()