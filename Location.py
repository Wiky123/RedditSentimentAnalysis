from sqlalchemy import create_engine, Column, String, Integer, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define your database connection
DATABASE_URL = "sqlite:///sites_crawled(reddit)9 copy 3.db3"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Define your table structures as SQLAlchemy ORM classes
class Submission(Base):
    __tablename__ = 'submissions'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    text = Column(Text)
    author = Column(String)
    url = Column(String)
    posted_at = Column(String)
    edited_at = Column(String, nullable=True)
    votes = Column(Integer)
    sentiment = Column(String)
    positive_prob = Column(Float)
    negative_prob = Column(Float)
    location = Column(String)  # Added new column for location

# Dictionary mapping IDs to locations
id_to_location = {
    5: 'both',
    6: 'neither',
    7: 'sbu',
    14: 'sbu',
    15: 'sbu',
    16: 'sbu',
    17: 'sbu',
    18: 'sbu',
    27: 'sbu',
    34: 'both',
    41: 'sbu',
    43: 'both',
    45: 'sbu',
    48: 'sbu',
    49: 'sbu',
    52: 'sbu',
    55: 'neither',
    58: 'neither',
    59: 'neither',
    60: 'neither',
    61: 'neither',
    62: 'neither',
    64: 'neither',
    69: 'neither',
    72: 'neither',
    75: 'neither',
    84: 'neither',
    87: 'neither',
    89: 'neither',
    95: 'neither',
    99: 'neither',
    100: 'neither',
    101: 'neither',
    103: 'neither',
    106: 'neither',
    120: 'neither',
    136: 'neither',
    141: 'neither',
    152: 'neither',
    159: 'neither',
    160: 'neither',
    167: 'neither',
    181: 'neither',
    184: 'neither',
    188: 'both',
    189: 'neither',
    190: 'neither',
    197: 'both',
    198: 'neither',
    199: 'sbu'

}

# Update location column in submissions
session = Session()

for submission in session.query(Submission).all():
    if submission.id in id_to_location:
        submission.location = id_to_location[submission.id]

session.commit()
session.close()
