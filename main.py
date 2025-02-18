import json
from datetime import datetime, timedelta
import pandas as pd
import requests
import sqlalchemy as sa
from sqlalchemy import MetaData
import contextlib
import table_valued_postgres_insert
import numpy as np


def get_NYT_book_list_by_date(date):
    api = 'https://api.nytimes.com/svc/books/v3/lists/full-overview.json?api-key=cNtAPJ3xzjfm0lLW7bUF6ABCHzzyNUfS&published_date=' + date
    response = requests.get(api).text
    response_info: object = json.loads(response)
    all_lists = response_info["results"]["lists"]
    best_sellers_date = response_info["results"]["bestsellers_date"]
    published_date = response_info["results"]["published_date"]
    nested_dict =[{}]
    for ls in all_lists:
        for x in ls["books"]:
            inner_dict = {"list_name": ls["list_name"]}
            inner_dict["best_sellers_date"] = best_sellers_date
            inner_dict["published_date"] =  published_date
            inner_dict.update(x)
            nested_dict.append(inner_dict)

    df = pd.DataFrame(nested_dict)
    return df

def get_book_list():
    stmt = 'SELECT * FROM public.master_book_list'
    # engine = table_valued_postgres_insert.login_postgres()
    with engine.begin() as conn:
        book_list = pd.read_sql(stmt, conn)

    return book_list

def get_nyt_list_types():
    #engine = login_postgres()
    meta = MetaData(engine)
    meta.reflect()

    with engine.begin() as conn:
        list_types = pd.read_sql('SELECT * FROM nyt_list_types', conn)

    return list_types

def merge_primaryISBNS() -> pd.DataFrame:
    isbn10_df = df_NYT_books[['author','title','primary_isbn10']]
    isbn10_df['isbn_type'] = '10'
    isbn13_df = df_NYT_books[['author','title', 'primary_isbn13']]
    isbn13_df['isbn_type'] = '13'
    isbn10_df = isbn10_df.rename(columns={'primary_isbn10':'isbn'})
    isbn13_df = isbn13_df.rename(columns={'primary_isbn13':'isbn'})
    combo = pd.concat([isbn10_df, isbn13_df])
    cleaned_df = combo.filter(items=['author', 'title', 'isbn', 'isbn_type']).drop_duplicates()
    print(cleaned_df)
    cleaned_df = pd.merge(cleaned_df, book_list, how='left', on=['title', 'author'])
    print('merging isbns')
    print(book_list)
    insert_df = cleaned_df[['book_id', 'isbn', 'isbn_type']]

    return insert_df

def insert_ISBNs():
    pk = ['book_id', 'isbn', 'isbn_type']
    df_to_write = merge_primaryISBNS()
    table_valued_postgres_insert.tt_write_to_db('books', 'public', 'isbns', pk, True, df_to_write)

def insert_author_titles():
    pk = ['title', 'author']
    author_title_df = df_NYT_books[['title', 'author']]
    table_valued_postgres_insert.tt_write_to_db('books', 'public', 'master_book_list', pk, True, author_title_df)

def create_merged_NYT_list():
    books_with_id =pd.merge(pd.merge(df_NYT_books, nyt_list_types, how='left', on='list_name'),
                            book_list, how='left', on=['title', 'author'])
    nyt_insert_df = books_with_id[['book_id', 'best_sellers_date', 'published_date', 'age_group', 'rank',
                                       'rank_last_week', 'updated_date', 'weeks_on_list', 'nyt_list_id']]
    nyt_insert_df = nyt_insert_df.rename(columns={"published_date":"list_published_date",
                                                                  "rank":"this_week_rank", "rank_last_week":"last_week_rank"})
    nyt_insert_df['insert_date'] = datetime.today().strftime('%Y-%m-%d')
    nyt_insert_df['insert_time'] = datetime.now().strftime('%H:%M:%S')
    nyt_insert_df = nyt_insert_df.replace(r'^\s*$', np.nan, regex=True)

    return nyt_insert_df

def insert_NYT_bestsellers():
    df_to_insert = create_merged_NYT_list()
    nyt_pk = ['book_id', 'best_sellers_date', 'nyt_list_id']
    table_valued_postgres_insert.tt_write_to_db('books', 'public', 'nyt_bestsellers', nyt_pk, True, df_to_insert)

def get_nyt_rankings_from_db():
    # engine = table_valued_postgres_insert.login_postgres()
    meta = MetaData(engine)
    meta.reflect()

    with engine.begin() as conn:
        rankings_from_db = pd.read_sql('SELECT * FROM nyt_bestsellers', conn)

    return rankings_from_db



if __name__ == '__main__':
    engine = table_valued_postgres_insert.login_postgres()
    book_list = pd.DataFrame()
    pd.set_option('mode.chained_assignment', None)

    date = '2025-02-13'
    start_date = datetime.strptime(date, '%Y-%m-%d')

    engine = table_valued_postgres_insert.login_postgres()

    df_NYT_books = get_NYT_book_list_by_date(start_date.strftime('%Y-%m-%d'))

    nyt_list_types = get_nyt_list_types()

    rankings_list = get_nyt_rankings_from_db()
    title_author_df = df_NYT_books[['title','author', 'primary_isbn10', 'primary_isbn13']]
    insert_author_titles()
    book_list = get_book_list()
    insert_ISBNs()
    insert_NYT_bestsellers()