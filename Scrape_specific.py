#Script to specified keywords in the text.
import praw
import datetime as dt
from sqlalchemy import create_engine, Column, String, Integer, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
import re
import time
import prawcore


# Reddit API setup remains the same
# PRAW (Python Reddit API Wrapper) setup
reddit = praw.Reddit(
    client_id='fg6SFLQnOu_ZrLfHzDc2Bg',
    client_secret='yAjtEx1jagJi7WnrSw6H_EQXUInrAg',
    user_agent='mac:MyRedditApp:0.1 (by u/Wik_workspace)',
    username='Wik_workspace',
    password='Sathwik16.com'
)

# SQLAlchemy setup
DATABASE_URL = "sqlite:///sites_crawled(reddit)9.db3"
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

class Keyword(Base):
    __tablename__ = 'keywords'
    id = Column(Integer, primary_key=True)
    keyword = Column(String, nullable=False)
    submission_id = Column(Integer, ForeignKey('submissions.id'))

class KeywordCount(Base):
    __tablename__ = 'keyword_counts'
    keyword = Column(String, primary_key=True)
    count = Column(Integer, default=0)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def safe_api_call(call, *args, **kwargs):
    while True:
        try:
            return call(*args, **kwargs)
        except prawcore.exceptions.TooManyRequests as e:
            print(f"Rate limit exceeded. Waiting for a fixed time.")
            time.sleep(60)

def unix_time_to_datetime(unix_time):
    if unix_time:
        return dt.datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
    return None

def save_comment(comment, submission_id):
    comment_creation_time = unix_time_to_datetime(comment.created_utc)
    comment_edit_time = unix_time_to_datetime(comment.edited) if comment.edited else None
    db_comment = Comment(
        submission_id=submission_id,
        author=str(comment.author),
        body=comment.body,
        posted_at=comment_creation_time,
        edited_at=comment_edit_time,
        votes=comment.ups
    )
    session.add(db_comment)
    session.flush()
    comment.replies.replace_more(limit=None)
    for reply in comment.replies:
        save_reply(reply, db_comment.id)

def save_reply(reply, comment_id):
    reply_creation_time = unix_time_to_datetime(reply.created_utc)
    reply_edit_time = unix_time_to_datetime(reply.edited) if reply.edited else None
    db_reply = Reply(
        comment_id=comment_id,
        author=str(reply.author),
        body=reply.body,
        posted_at=reply_creation_time,
        edited_at=reply_edit_time,
        votes=reply.ups
    )
    session.add(db_reply)

def find_keywords_in_text(text, keywords):
    """Identify if keywords are present in the text."""
    found_keywords = []
    for keyword in keywords:
        if keyword.lower() in text.lower():
            found_keywords.append(keyword)
    return list(set(found_keywords))  # Remove duplicates

def update_keyword_tracking(keywords, submission_id):
    for keyword in keywords:
        # Check if the keyword is already tracked for the submission
        existing_keyword = session.query(Keyword).filter_by(keyword=keyword, submission_id=submission_id).first()
        if not existing_keyword:
            # Add the new keyword to the keywords table
            keyword_record = Keyword(keyword=keyword, submission_id=submission_id)
            session.add(keyword_record)

            # Update or add the keyword count in the keyword counts table
            keyword_count = session.query(KeywordCount).filter_by(keyword=keyword).first()
            if keyword_count:
                keyword_count.count += 1
            else:
                session.add(KeywordCount(keyword=keyword, count=1))
    session.commit()

def save_submission(submission, search_queries):
    """Saves submission and its related comments and replies to the database."""
    # Convert submission creation and edit times
    submission_creation_time = unix_time_to_datetime(submission.created_utc)
    submission_edit_time = unix_time_to_datetime(submission.edited) if submission.edited else None

    # Create a new Submission object
    db_submission = Submission(
        title=submission.title,
        text=submission.selftext,
        author=str(submission.author),
        url=f"https://reddit.com{submission.permalink}",
        posted_at=submission_creation_time,
        edited_at=submission_edit_time,
        votes=submission.ups
    )
    session.add(db_submission)
    session.flush()  # Flush to assign an ID to db_submission without committing the entire session

    submission_id = db_submission.id

    # Process comments
    try:
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            save_comment(comment, submission_id)
    except prawcore.exceptions.TooManyRequests:
        print("Rate limit hit while processing comments. Waiting...")
        time.sleep(60)  # Adjust based on your rate limit strategy

    # Keyword tracking
    text_entities = [submission.title + ' ' + submission.selftext] + [comment.body for comment in submission.comments.list()]
    for text in text_entities:
        keywords_found = find_keywords_in_text(text, search_queries)
        update_keyword_tracking(keywords_found, submission_id)

    # Commit the session to save all changes
    session.commit()

# Main data collection logic setup

processed_submissions = set()
subreddit = reddit.subreddit('longisland')

search_queries = [
    "Stony Brook", "Stonybrook", "Suicide", "Young Children", "Seniorcare", 
    "Senior Care", "Senior", "Elder", "Elderly", "Elder Care", "Eldercare", "Aged", "Aging", "Hip", "Knee"
]



for search_query in search_queries:
    submissions = subreddit.search(search_query, sort='new', time_filter='year')
    for submission in submissions:
        if submission.id not in processed_submissions:
            processed_submissions.add(submission.id)
            try:
                save_submission(submission, search_queries)
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(60)

        


session.close()
print("Data collection and storage complete with targeted phrase tracking.")

