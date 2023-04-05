import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from database_url import db_url

engine = create_engine(
    db_url
)

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        db_url
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

#initial tables generation
def upload_features():
    df_user = batch_load_sql('''SELECT * FROM "user_data" ''')

    df_user['age'] = np.where(df_user['age'] > 70, 70, df_user['age'])

    countries_tier1 = ['Ukraine', 'Kazakhstan', 'Belarus']
    countries_tier2 = ['Turkey', 'Azerbaijan', 'Finland']
    # tier_3 - others
    df_user['country'] = np.where(df_user['country'] == 'Russia', df_user['country'],
                                  np.where(df_user['country'].isin(countries_tier1), 'tier_1',
                                           np.where(df_user['country'].isin(countries_tier2), 'tier_2',
                                                    'tier_3')))

    cities_by_nusers = df_user.city.value_counts().reset_index()

    cities_tier0 = ['Moscow', 'Saint Petersburg']
    cities_tier1 = list(
        cities_by_nusers[(cities_by_nusers['city'] < 3000) & (cities_by_nusers['city'] > 1500)]['index'])
    cities_tier2 = list(
        cities_by_nusers[(cities_by_nusers['city'] < 1500) & (cities_by_nusers['city'] > 1000)]['index'])
    cities_tier3 = list(cities_by_nusers[(cities_by_nusers['city'] < 1000) & (cities_by_nusers['city'] > 500)]['index'])
    cities_tier4 = list(cities_by_nusers[(cities_by_nusers['city'] < 500) & (cities_by_nusers['city'] > 100)]['index'])
    df_user['city'] = np.where(df_user['city'].isin(cities_tier0), df_user['city'],
                               np.where(df_user['city'].isin(cities_tier1), 'tier_1',
                                        np.where(df_user['city'].isin(cities_tier2), 'tier_2',
                                                 np.where(df_user['city'].isin(cities_tier3), 'tier_3',
                                                          np.where(df_user['city'].isin(cities_tier4), 'tier_4',
                                                                   'tier_5')))))

    dum_cols = list(df_user.select_dtypes(include='object').columns) + ['exp_group']
    for col in dum_cols:
        # print(col)
        df_user = pd.get_dummies(df_user, columns=[col], prefix=[col], drop_first=True)

    df_user.to_sql('ksenia_pismerova_features_lesson_22',if_exists='replace', con=engine)
    return(df_user)

def upload_posts():
    df_post = batch_load_sql('''SELECT post_id, topic FROM "post_text_df" ''')
    df_post = pd.get_dummies(df_post, columns=['topic'], prefix=['topic'], drop_first=True)

    df_post.to_sql('ksenia_pismerova_posts_lesson_22', if_exists='replace', con=engine)
    return(df_post)

#just load tables that already exist to pandas
def load_features():
    df_user = batch_load_sql('''SELECT * FROM "ksenia_pismerova_features_lesson_22" ''')
    return(df_user)

def load_posts():
    df_post = batch_load_sql('''SELECT * FROM "ksenia_pismerova_posts_info_features_dl" ''')
    df_post['topic_dum'] = df_post['topic'].copy()
    df_post = pd.get_dummies(df_post, columns=['topic_dum'], prefix=['topic'], drop_first=True)
    return(df_post)

if __name__ == "__main__":
    upload_features()
    upload_posts()