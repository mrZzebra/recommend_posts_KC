from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String, desc

class Post(Base):
    __tablename__ = "post"
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    res_list = []
    for el in (
        session.query(Post)
        .filter(Post.topic == "business")
        .order_by(desc(Post.id))
        .limit(10)
        .all()
    ):
        res_list.append(el.id)
    print( res_list)



