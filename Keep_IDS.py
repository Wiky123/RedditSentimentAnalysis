from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, ForeignKey, Float

# Database URL has been updated as per your input
DATABASE_URL = "sqlite:///sites_crawled(reddit)9 copy 2.db3"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Base definition, ensure your class definitions are compatible with your database schema
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

class Comment(Base):
    __tablename__ = 'comments'
    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submissions.id'))
    author = Column(String)
    body = Column(Text)
    posted_at = Column(String)
    edited_at = Column(String, nullable=True)
    votes = Column(Integer)
    sentiment = Column(String)
    positive_prob = Column(Float)
    negative_prob = Column(Float)

class Reply(Base):
    __tablename__ = 'replies'
    id = Column(Integer, primary_key=True)
    comment_id = Column(Integer, ForeignKey('comments.id'))
    author = Column(String)
    body = Column(Text)
    posted_at = Column(String)
    edited_at = Column(String, nullable=True)
    votes = Column(Integer)
    sentiment = Column(String)
    positive_prob = Column(Float)
    negative_prob = Column(Float)

# List of submission IDs to keep
#keep_ids = {9, 28, 35, 37, 39, 43, 46, 53, 54}
keep_ids = {4, 5, 6, 7, 14, 15, 16, 17, 18, 27, 34, 41, 43, 45, 48, 49, 52, 55, 58, 59, 60, 61, 62, 64, 69, 72, 75, 84, 87, 89, 95, 98, 99, 100, 101, 103, 106, 120, 136, 141, 152, 159, 160, 167, 181, 184, 188, 189, 190, 196, 197, 198, 199}

# Delete submissions not in the keep_ids
session.query(Submission).filter(~Submission.id.in_(keep_ids)).delete(synchronize_session='fetch')

# Delete comments not linked to kept submissions
session.query(Comment).filter(~Comment.submission_id.in_(keep_ids)).delete(synchronize_session='fetch')

# For replies, first determine which comments are kept (linked to kept submissions)
kept_comments_ids = session.query(Comment.id).filter(Comment.submission_id.in_(keep_ids)).subquery()
session.query(Reply).filter(~Reply.comment_id.in_(kept_comments_ids)).delete(synchronize_session='fetch')

# Commit the changes to save them in the database
session.commit()
session.close()
print("Database cleanup complete. Only selected submissions and their related data have been retained.")
