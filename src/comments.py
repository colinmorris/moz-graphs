from src.debuggers import Debugger

__author__ = 'colin'

from alchemical_base import Base
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Unicode, Boolean, Enum, DateTime
from sqlalchemy.orm import backref, relationship
import utils
from scraping import bug_scrape as bs

class BzComment(Base):
    """A comment made on the bugzilla page of a particular bug.
    """

    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True)
    bugid = Column(Integer, ForeignKey("bugs.bzid"), index=True, nullable=False)
    when = Column(DateTime)
    dbid = Column(ForeignKey('debuggers.id'), index=True)
    # This is redundant but convenient
    monthid = Column(ForeignKey('months.id'), index=True)

    bug = relationship("Bug")
    debugger = relationship("Debugger")
    month = relationship("Month")

    @classmethod
    def from_html(cls, session, bugid, frag):

        vcard = frag.find("span", class_="bz_comment_user").span
        email = vcard.a["href"]
        assert email.startswith("mailto:")
        email = email[7:]

        name = vcard.a.span.string().strip()

        when = frag.find("span", class_="bz_comment_time").string().strip()
        when = utils.datetimeify(when)

        debugger = Debugger.get_or_create(session, name=name, email=email)

        comm = cls(bugid=bugid, when=when)
        comm.debugger = debugger
        return comm



def populate_comments(session, abbrev=False):
    bugs = utils.open_data_file('All_bugs.csv')
    bugs.readline()
    bzids = [line.split(',')[0] for line in bugs]
    if abbrev:
        bzids = bzids[:1]
    bugs.close()

    for bzid in bzids:
        bzid = int(bzid)

        # Skip any ids already in the db
        if session.query(BzComment).filter_by(bugid=bzid).count():
            print "Skipping existing bug with id " + str(bzid)
            continue

        try:
            bp = bs.BugPage(bzid)
        except bs.ForbiddenBugException:
            print "Skipping forbidden bug "  +str(bzid)
            continue

        for div in bp.comment_divs():
            comment = BzComment.from_html(session, bzid, div)
            session.add(comment)

        session.commit()
