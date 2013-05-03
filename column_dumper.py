import sqlalchemy.orm.attributes
import os
from src.utils import get_session

__author__ = 'colin'

dump_dir = 'bugmonth_data'

# Dealing with numerical data, so let's use a histogram

class ColumnDumper(object):

    def __init__(self, cls):
        self.cls = cls
        self.session = get_session()

    def dump_columns(self, ignore=[]):
        for (name, attr) in self.cls.__dict__.iteritems():
            if name in ignore:
                continue
            if isinstance(attr, sqlalchemy.orm.attributes.InstrumentedAttribute):
                self.dump_column(name, attr)


    def is_null(self, column):
        return self.session.query(column).filter(column != None).count() == 0

    def dump_column(self, name, col):
        #if self.is_null(col):
        #    print "Skipping empty column " + name
        #    return
        print "Dumping column " + name
        fname = os.path.join(dump_dir, name + '.dat')
        f = open(fname, 'w')
        self._dump_column_onewrite(col, f)

        f.close()

    def _dump_column_multiwrite(self, col, f):
        for tup in self.session.query(col):
            value = tup[0]
            f.write(str(value) + ' ')

    def _dump_column_onewrite(self, col, f):
        res = self.session.query(col).all()
        f.write(' '.join(str(tup[0]) for tup in res if tup[0] is not None))

if __name__ == '__main__':
    from src.bugmonth_variables import BugMonth
    ignore = [
        'id', 'bugid', 'monthid', 'assigneeid', 'bsid', 'status', 'resolution',
        'importance', 'platform', 'product', 'assigned',
        'state', 'assignee', 'bug', 'month'
    ]
    dumper = ColumnDumper(BugMonth)
    dumper.dump_columns(ignore)

