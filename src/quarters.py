__author__ = 'colin'

# Do I actually need this? Not clear yet.

# DEPRECATED?

from sqlalchemy import Column, Integer, Date, func
from alchemical_base import Base
import bisect
import datetime
from models import *

class Quarter(Base):
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