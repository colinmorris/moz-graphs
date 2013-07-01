from src.utils import museumpiece

__author__ = 'colin'

from sqlalchemy import Column, Integer, Date, func
from alchemical_base import Base
import bisect
import datetime
from models import *



class Month(Base):
    """A contiguous block of 28 days (in practice, but could be more or less),
    not necessarily corresponding to the calendar definition of "month". In
    practice, we'll be creating a bunch of these covering overlapping periods.
    """
    __tablename__ = 'months'


    id = Column(Integer, primary_key=True)
    first = Column(Date)
    last = Column(Date) # Should be first+27

    # TODO: design question, should I include month-specific network vars here
    # like effective size?

    def __cmp__(self, other):
        """Compare two Months based on start date.
        """
        return cmp(self.first, other.first)

    @property
    def fname(self, ext='csv'):
        return str(self.first) + '.' + ext

    def bugmonths(self, session):
        pass

    def prev(self, session):
        """Return the latest preceding NON-OVERLAPPING month, or None if none
        exists.
        """
        #if self.id <= 2:
        #    raise ValueError("No previous month in table.")
        last = session.query(Month).filter_by(id = self.id-2).scalar()
        assert last is None or (self.first - last.first).days == 28
        return last

    def next(self, session):
        """Return the next NON-OVERLAPPING month, or None if none
        exists.
        """
        #if self.id <= 2:
        #    raise ValueError("No previous month in table.")
        next = session.query(Month).filter_by(id = self.id+2).scalar()
        assert next is None or (next.first - self.first).days == 28
        return next

    def get_network(self):
        raise NotImplementedError

    @staticmethod
    def sorted_months(session):
        months = session.query(Month).all()
        months.sort()
        return months

    def __str__(self):
        return "Month with id %d from %s to %s" % (self.id, str(self.first), str(self.last))

    def __repr__(self):
        return str(self)

class Quarter(object):

    quarterspan = datetime.timedelta(days=28*3-1)

    def __init__(self, **kwargs):
        # Hackety hack hack hack
        if 'next' in kwargs:
            self.last = kwargs['next'].first - datetime.timedelta(days=1)
            self.first = self.last - self.quarterspan
        elif 'first' in kwargs:
            self.first = kwargs['first'].first
            self.last = self.first + self.quarterspan
        elif 'last' in kwargs:
            self.last = kwargs['last'].last
            self.first = self.last - self.quarterspan
        else:
            raise ValueError("Specify either the first or the last month")

    def prev(self):
        return Quarter(next=self)


class MonthSet(object):
    """A sorted collection of Months."""

    def __init__(self, months):
        self.months = months
        self.months.sort()

        self.firsts = [month.first for month in self.months]
        self.lasts = [month.last for month in self.months]

    def __len__(self):
        return len(self.months)

    @staticmethod
    def from_session(session):
        months = session.query(Month).all()
        return MonthSet(months)

    def after(self, date, end=False, eq=False):
        """Return the starting index for months beginning after the given date.

        If end is True, then return months ENDING after the given date (i.e.
        return one extra result).

        If eq is true then after becomes after or equal to.
        """
        array = self.lasts if end else self.firsts
        if eq:
            return bisect.bisect_left(array, date)
        else:
            return bisect.bisect_right(array, date)

    def monthafter(self, date, end=False, eq=False):
        """Return the first month beginning after the given date.

        If end is True, then return month ENDING after the given date.

        If eq is true then after becomes after or equal to.

        @throws: Indexerror, if this date is in the last month
        """
        array = self.lasts if end else self.firsts

        # This is kind of tricky. We're relying on the parallel structure of array and self.months
        if eq:
            return self[bisect.bisect_left(array, date)]
        else:
            return self[bisect.bisect_right(array, date)]

    def __getitem__(self, item):
        # Are we okay with just returning a list, or should it be another MonthSet?
        return self.months.__getitem__(item)

@museumpiece
def populate_months(session):
    if session.query(Month).count() > 0:
        raise Exception("Months table is already populated.")

    demimonth = datetime.timedelta(days=14)

    first_chat = session.query(func.min(Chat.date)).scalar()
    first_bugevent = session.query(func.min(BugEvent.date)).scalar()
    start_date = max(first_chat, first_bugevent)
    print "First chat is " + str(first_chat)
    print "First bug event is " + str(first_bugevent)
    print "Starting months on " + str(start_date)

    last_chat = session.query(func.max(Chat.date)).scalar()
    last_bugevent = session.query(func.max(BugEvent.date)).scalar()
    end_date = min(last_chat, last_bugevent)
    print "Last chat is " + str(last_chat)
    print "Last bug event is " + str(last_bugevent)
    print "End months on or around " + str(end_date)


    start = start_date
    end = start_date + datetime.timedelta(days=27) # start + 27 days = 28 day span

    while end < end_date:
        month = Month(first=start, last=end)
        session.add(month)
        start += demimonth
        end += demimonth

    session.commit()

