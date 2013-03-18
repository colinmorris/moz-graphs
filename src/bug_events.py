from sqlalchemy import Column, Integer, ForeignKey, Date, Enum
from sqlalchemy.orm import relationship
from src.alchemical_base import Base
from src.bugs import Bug
from scraping.bug_scrape import BugPage
from src.debuggers import Debugger
from src import utils

__author__ = 'colin'

class BugEvent(Base):
    """
    An interaction between a Bug and a Debugger, occurring on a particular date.

    Right now, we're considering "history" events of any kind, and also comments
    on the bug's Bugzilla page.
    """
    __tablename__ = "bugevents"

    id = Column(Integer, primary_key=True)
    bzid = Column(Integer, ForeignKey('bugs.bzid'), nullable=False, index=True)
    dbid = Column(Integer, ForeignKey('debuggers.id'), nullable=False, index=True)
    date = Column(Date, index=True)

    # XXX: new
    orig = Column(Enum(["hist", "comment"]), default="hist")

    debugger = relationship("Debugger")
    bug = relationship("Bug")


    @classmethod
    def scrape_comment_events(cls, session, commit_interval=40):

        # This ended up being quite tricky! Most of the tricky logic here is just to manage
        # periodic commits (rather than one big commit at the end), and complementary
        # measures to avoid excessive server hits.

        # We MUST:
        #   - never commit when only a portion of a bug's comments have been added
        #   - save a bug's id in scraped_comments.txt when (and only when) all its comments are committed

        # We cache bugs whose comments have been scraped so that we don't have to do a server hit just to see whether
        # or not we've already done the comments (by which time it's too late)
        scraped_comments_fname = "scraped_comments.txt"
        scraped_comments_f = utils.open_data_file(scraped_comments_fname)
        scraped_bugids = set([line.strip() for line in scraped_comments_f])
        scraped_comments_f.close()

        scraped_comments_f = utils.open_data_file(scraped_comments_fname, 'a')
        scraped_comments_buffer = ''

        ncomms = 0
        next_milestone = 10
        commit_due = False
        nskipped = 0
        for bug in session.query(Bug):
            if str(bug.bzid) in scraped_bugids:
                nskipped += 1
                continue
            page = BugPage(bug.bzid)
            for comment in page.comments():
                comment_event = BugEvent(date=comment.date)

                # Use existing debugger if there is one, otherwise a fresh debugger which is added to the session
                debugger = Debugger.get_or_create_debugger(session, comment.author)

                comment_event.debugger = debugger
                comment_event.bug = bug
                comment_event.orig = "comment"
                session.add(comment_event)
                ncomms += 1
                if commit_interval and ncomms % commit_interval:
                    # Only commit on comment boundaries, to avoid orphans
                    commit_due = True

                if ncomms == next_milestone:
                    print "Added %d comments" % (ncomms)
                    print "Skipped %d existing bugs" % (nskipped)
                    next_milestone *= 2

            scraped_comments_buffer += str(bug.bzid)+'\n'
            if commit_due:
                session.commit()
                scraped_comments_f.write(scraped_comments_buffer)
                scraped_comments_buffer = ''
                commit_due = False


        # If we're periodically committing, then make sure we clean up our scraps at the end.
        if commit_interval:
            session.commit()

        scraped_comments_f.close()