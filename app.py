#cd app_itself
#uvicorn app:app --reload

import pandas as pd
import pickle as pkl
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import FastAPI, Depends
from schema import PostGet, Response
from table_post import Post
from database import get_db
from models_features import Xs_test, Xs_control
from features_upload import load_features,load_posts
from exp_group import get_exp_group

app = FastAPI()

model_test = pkl.load(open("catboost_model2.pkl", 'rb'))
model_control = pkl.load(open("sklearn_model2.pkl", 'rb'))

df_user = load_features()
df_post = load_posts()

@app.get("/post/recommendations/", response_model=Response)
def get_post_recomendations(id: int, time: datetime, limit: int = 10,
                            db: Session = Depends(get_db)) -> PostGet:
    df_user_1 = df_user[df_user['user_id']==id]
    df_user_1['key'] = 1
    df_post['key'] = 1
    df_user_1 = pd.merge(df_user_1, df_post, on='key')  # .drop('key')

    df_user_1['timestamp'] = pd.to_datetime(time)
    df_user_1['hour'] = pd.to_datetime(df_user_1['timestamp']).dt.hour
    df_user_1['month'] = pd.to_datetime(df_user_1['timestamp']).dt.month
    df_user_1['weekday'] = pd.to_datetime(df_user_1['timestamp']).dt.weekday

    user_group = get_exp_group(id)
    if user_group == 'test':
        df_user_1['forecast'] = model_test.predict_proba(df_user_1[Xs_test])[:, 0]
    elif user_group == 'control':
        df_user_1['forecast'] = model_control.predict_proba(df_user_1[Xs_control])[:, 0]
    else:
        raise ValueError('unknown group')


    return Response(
        recommendations=(db.query(Post.id, Post.text, Post.topic)
        .filter(Post.id.in_(tuple(df_user_1.sort_values('forecast').head(5)['post_id'])))
        .limit(limit)
        .all()),
        exp_group=user_group,
    )