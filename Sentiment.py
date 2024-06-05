from sqlalchemy import create_engine, Column, String, Integer, Text, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from transformers import pipeline

# Define your database connection
DATABASE_URL = "sqlite:///sites_crawled(reddit)9 copy 2.db3"
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

# Make sure your database schema is up to date
Base.metadata.create_all(engine)

# Initialize the sentiment analysis pipeline
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

def perform_sentiment_analysis():
    session = Session()

    # Helper function for text truncation
    def truncate_text(text):
        max_length = 512 - 2  # Accounting for special tokens
        tokenized_text = sentiment_pipeline.tokenizer.tokenize(text)
        if len(tokenized_text) > max_length:
            tokenized_text = tokenized_text[:max_length]
        return sentiment_pipeline.tokenizer.convert_tokens_to_string(tokenized_text)

    # Helper function to convert scores to percentages
    def to_percentage(score):
        return round(score * 100, 2)  # Round to two decimal places for precision

    # Update Submissions
    for submission in session.query(Submission):
        text = truncate_text(submission.text if submission.text else submission.title)
        result = sentiment_pipeline(text)[0]
        submission.sentiment = result['label']
        positive_score = to_percentage(result['score']) if result['label'] == "POSITIVE" else to_percentage(1 - result['score'])
        negative_score = 100 - positive_score  # Ensure they sum up to 100%
        submission.positive_prob = positive_score
        submission.negative_prob = negative_score

    # Repeat the process for Comments and Replies
    for comment in session.query(Comment):
        truncated_body = truncate_text(comment.body)
        result = sentiment_pipeline(truncated_body)[0]
        comment.sentiment = result['label']
        positive_score = to_percentage(result['score']) if result['label'] == "POSITIVE" else to_percentage(1 - result['score'])
        negative_score = 100 - positive_score
        comment.positive_prob = positive_score
        comment.negative_prob = negative_score

    for reply in session.query(Reply):
        truncated_body = truncate_text(reply.body)
        result = sentiment_pipeline(truncated_body)[0]
        reply.sentiment = result['label']
        positive_score = to_percentage(result['score']) if result['label'] == "POSITIVE" else to_percentage(1 - result['score'])
        negative_score = 100 - positive_score
        reply.positive_prob = positive_score
        reply.negative_prob = negative_score

    session.commit()
    session.close()



perform_sentiment_analysis()
