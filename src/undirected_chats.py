__author__ = 'colin'

from src.alchemical_base import Base
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from collections import defaultdict
from chatters import *

class UndirectedChat(Base):
    """Each row actually records the number of undirected chatS
    (i.e. messages) in a given day.
    """

    __tablename__ = 'undirectedchats'

    id = Column(Integer, primary_key=True)
    dbid = Column(Integer, ForeignKey('debuggers.id'), nullable=False, index=True)
    day = Column(Date, nullable=False)
    n = Column(Integer, nullable=False)


def populate_undirected(session):
    # Step 1: Map debuggers to their nirc per day
    from models import Alias
    nick_to_day_to_nirc = defaultdict(lambda : defaultdict(int))
    for fname in os.listdir(CHATLOG_DIR):
        if not fname.endswith('.log'):
            continue
        f = open(os.path.join(CHATLOG_DIR, fname))
        log = Log(f, lite=True)
        day = log.start.date()
        for line in log.lines:

            if isinstance(line, SystemMessageLine):
                continue
            # Otherwise it's a message line or an emote line, which we treat equivalently
            alias = line.actor.nick
            if alias is None:
                continue
            nick_to_day_to_nirc[alias][day] += 1

        f.close()

    # Step 2: Add to database
    for (nick, dic) in nick_to_day_to_nirc.iteritems():
        alias = session.query(Alias).filter_by(alias=nick).scalar()
        if alias is None:
            continue
        dbid = alias.dbid
        for (day, n) in dic.iteritems():
            chat = UndirectedChat(dbid=dbid, day=day, n=n)
            session.add(chat)

    session.commit()


