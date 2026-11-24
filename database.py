from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Aici definim numele fișierului bazei de date (bambu_manager.db)
SQLALCHEMY_DATABASE_URL = "sqlite:///./bambu_manager.db"

# Creăm motorul care comunică cu SQLite
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
