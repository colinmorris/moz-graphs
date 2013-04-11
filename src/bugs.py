from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from src.alchemical_base import Base
from src.debuggers import Debugger
import utils

__author__ = 'colin'

class Bug(Base):
    __tablename__ = 'bugs'

    bzid = Column(Integer, primary_key=True, autoincrement=False)
    importance = Column(String)
    n_cc_list = Column(Integer)
    n_depends = Column(Integer)
    n_duplicates = Column(Integer)
    n_history = Column(Integer)
    n_keywds = Column(Integer)
    nblocks = Column(Integer)
    nvoters = Column(Integer)
    product = Column(String)
    reported = Column(Date)
    resolved = Column(Date)
    status = Column(String)
    verified = Column(Date)

    # TODO: Update table to add this column (nontrivial)
    # if null then assigned to no-one
    assignee_id = Column(ForeignKey('debuggers.id'), nullable=True)


    assignee = relationship("Debugger")

    def __str__(self):
        return "Bug<%s>" % (self.bzid)

    def __unicode__(self):
        return u"Bug<%s>" % (self.bzid)

    def __repr__(self):
        return str(self)

    @property
    def id(self):
        """I'm only doing this because I constantly try to call it id instead of bzid, since that's the pkey name I
        use for every other model..."""
        return self.bzid


    ####________ONESHOT SCRIPTS___________####

    @staticmethod
    def add_assignee_ids(session, heal=False):
        """
        Assignee ids were not originally part of the bug schema. Have to add it in by reparsing bug_summary.csv
        """
        # I feel like this is a pattern that's come up a lot. There's probably some code reuse that can be done here.
        bugsum = utils.bug_summary_dict()
        for bugd in bugsum:
            assigned_str = bugd["assigned_to"]
            id = bugd["bug"]
            bug = session.query(Bug).filter_by(bzid=id).scalar()

            assigned = Debugger.parse_assignedto_str(assigned_str)
            if assigned is None:
                # assigned_id should already be None/NULL, so don't need to do anything
                continue

            matches = session.query(Debugger).filter_by(email=assigned.email)
            nmatches = matches.count()

            # Case 1: This is a new debugger
            if nmatches == 0:
                print "[INFO] Adding new debugger " + str(assigned)
                session.add(assigned)
                #session.flush() #XXX: Do I need this? I actually don't think so.
                bug.assignee = assigned
            # Case 2: An existing debugger
            elif nmatches == 1:
                found = matches.scalar()
                bug.assignee = found

                # May want to overwrite
                if assigned.irc and assigned.irc != found.irc and heal:
                    print "-----------------------------------------------------------"
                    resp = raw_input("Based on str {%s}, replace irc {%s} with {%s}? (Y/N/O(ther)\n>" % (assigned_str, found.irc, assigned.irc))
                    if resp in 'yY':
                        found.irc = assigned.irc
                    elif resp in 'oO':
                        newirc = raw_input("Enter new IRC nick:")
                        found.irc = newirc


                if assigned.name and assigned.name != found.name and heal:
                    print "-----------------------------------------------------------"
                    resp = raw_input("Based on str\n%s\nreplace name\n%s\nwith\n%s\n? (Y/N/O(ther)\n>" % (assigned_str, found.name, assigned.name))
                    if resp in 'yY':
                        found.name = assigned.name
                    elif resp in 'oO':
                        newname = raw_input("Enter new name:")
                        found.name = newname

            else:
                raise Exception("Got two+ debuggers with the same e-mail (%s). This should never happen." % (assigned.email))