from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import backref, relationship

from alchemical_base import Base

class VarColumn(Column):
        """A column whose field may be reasonably expected to be subject to updates over time.
        For example, a column counting the number of IRC logs a debugger appears in may start
        at 0/1 and be incremented each time the debugger is seen in another log.
        """
        pass


class Alias(Base):
    __tablename__ = 'aliases'
    
    id = Column(Integer, primary_key=True)
    dbid = Column(Integer, ForeignKey('debuggers.id'))
    alias = Column(String)
    noccs = VarColumn(Integer, default=0)
    
    debugger = relationship("Debugger", backref=backref("aliases"))


class Chat(Base):
    __tablename__ = "chats"
    
    id = Column(Integer, primary_key=True) 
    p1 = Column(ForeignKey('debuggers.id'))
    p2 = Column(ForeignKey('debuggers.id'))
    n = Column(Integer, default=1)
    date = Column(Date)

    # I switched from this to the other thing to try to fix a bug. It didn't fix it,
    # but I don't think it broke anything either.
    db1 = relationship("Debugger", primaryjoin="Debugger.id==Chat.p1")
    db2 = relationship("Debugger", primaryjoin="Debugger.id==Chat.p2")
    #db1 = relationship("Debugger", foreign_keys=[p1])
    #db2 = relationship("Debugger", foreign_keys=[p2])
