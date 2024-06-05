from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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

# Create the new merged database
engine = create_engine('sqlite:///MergedDatabase.db')
Base.metadata.create_all(engine)

def transfer_data(source_db_url, target_engine):
    source_engine = create_engine(source_db_url)
    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()
    
    TargetSession = sessionmaker(bind=target_engine)
    target_session = TargetSession()

    # Transfer Submissions
    submissions = source_session.query(Submission).all()
    for submission in submissions:
        target_session.merge(submission)  # merge avoids duplicate entries

    # Transfer Comments
    comments = source_session.query(Comment).all()
    for comment in comments:
        target_session.merge(comment)

    # Transfer Replies
    replies = source_session.query(Reply).all()
    for reply in replies:
        target_session.merge(reply)

    try:
        target_session.commit()
    except Exception as e:
        print(f"Failed to commit changes: {e}")
        target_session.rollback()
    finally:
        target_session.close()
        source_session.close()

# List of your database files
db_files = ['sqlite:///sites_crawled(reddit)Elders.db3', 'sqlite:///sites_crawled(reddit)Children.db3', 'sqlite:///sites_crawled(reddit)SB.db3', 'sqlite:///sites_crawled(reddit)Suicide.db3']

for db in db_files:
    transfer_data(db, engine)
