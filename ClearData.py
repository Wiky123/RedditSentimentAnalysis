from sqlalchemy import create_engine, MetaData

# Database URL
DATABASE_URL = "sqlite:///sites_crawled(reddit)9.db3"

# Creating an engine
engine = create_engine(DATABASE_URL)

# Reflecting the database schema
meta = MetaData()
meta.reflect(bind=engine)

# Code to drop all tables
for table in reversed(meta.sorted_tables):
    table.drop(engine)

# Confirming if all tables are dropped
meta.clear()
meta.reflect(bind=engine)
tables_remaining = meta.tables.keys()
print("Tables remaining:", tables_remaining)
