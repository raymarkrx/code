import sqlalchemy

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker



print sqlalchemy.__version__
engine = create_engine('mysql://jack:Jack1@34@10.91.227.145:3308/test', echo=True)
print engine
Base = declarative_base()
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    password = Column(String)
    def __repr__(self):
        return "<User(name='%s', fullname='%s', password='%s')>" % (self.name, self.fullname, self.password)

Session = sessionmaker(bind=engine)
session = Session()
#ed_user = User(id=2,name='ed2', fullname='Ed Jones2', password='edsprd2')

#session.add(ed_user)
#session.commit()
#our_user = session.query(User)
#print dir(our_user)
#print our_user.all()    

