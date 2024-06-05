from sqlalchemy import create_engine, Column, Float, String, text
from sqlalchemy.orm import sessionmaker, declarative_base
import praw
import datetime as dt
from sqlalchemy import create_engine, Column, String, Integer, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
import re
import time
import prawcore

DATABASE_URL = "sqlite:///sites_crawled(reddit)9 copy 2.db3"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Assuming your existing classes are named Submission, Comment, and Reply
# You'll need to define them here if you're running this in a new script
DATABASE_URL = "sqlite:///sites_crawled(reddit)9 copy 2.db3"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

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

class Comment(Base):
    __tablename__ = 'comments'
    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submissions.id'))
    author = Column(String)
    body = Column(Text)
    posted_at = Column(String)
    edited_at = Column(String, nullable=True)
    votes = Column(Integer)

class Reply(Base):
    __tablename__ = 'replies'
    id = Column(Integer, primary_key=True)
    comment_id = Column(Integer, ForeignKey('comments.id'))
    author = Column(String)
    body = Column(Text)
    posted_at = Column(String)
    edited_at = Column(String, nullable=True)
    votes = Column(Integer)

def add_sentiment_columns():
    with engine.connect() as conn:
        # Use the text() construct to ensure the command is executable
        conn.execute(text("ALTER TABLE submissions ADD COLUMN sentiment TEXT"))
        conn.execute(text("ALTER TABLE submissions ADD COLUMN positive_prob FLOAT"))
        conn.execute(text("ALTER TABLE submissions ADD COLUMN negative_prob FLOAT"))
        conn.execute(text("ALTER TABLE submissions ADD COLUMN location TEXT"))
        
        
        conn.execute(text("ALTER TABLE comments ADD COLUMN sentiment TEXT"))
        conn.execute(text("ALTER TABLE comments ADD COLUMN positive_prob FLOAT"))
        conn.execute(text("ALTER TABLE comments ADD COLUMN negative_prob FLOAT"))
        
        conn.execute(text("ALTER TABLE replies ADD COLUMN sentiment TEXT"))
        conn.execute(text("ALTER TABLE replies ADD COLUMN positive_prob FLOAT"))
        conn.execute(text("ALTER TABLE replies ADD COLUMN negative_prob FLOAT"))

# Call the function to make changes
add_sentiment_columns()
