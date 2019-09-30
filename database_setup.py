#!/usr/bin/env python3
import psycopg2
import json
import csv
import sys
from psycopg2.extras import RealDictCursor
from itertools import islice
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def read_file(file_path):
    '''Read CSV file and get the header'''
    with open(file_path, 'r') as f:
        fString = f.read()
    header = fString.split('\n')[0].split(',')
    return header


def create_db():
    con = psycopg2.connect(dbname='postgres')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = con.cursor()
    cur.execute("CREATE DATABASE FIlMS")
    con.commit()
    print("Database has been created")
    con.close()


def get_data(*qureies):
    ''' This function used to connect to the database'''
    try:
        con = psycopg2.connect(dbname="films")
    except psycopg2.Error as e:
        print ("Unable to connect!")
        print (e.pgerror)
        print (e.diag.message_detail)
        sys.exit(1)
    else:
        cur = con.cursor()
        for qurey in qureies:
            cur.execute(qurey)
        d = cur.fetchall()
        con.close()
    return d


def connect_database(query):
    '''Connect to postgrelsql DB using psycopg2 DB-API '''
    try:
        con = psycopg2.connect(dbname="films", user='vagrant')
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        print("The query has been excuted")
    except psycopg2.Error as e:
        print("Unable to connect!")
        # print the error message
        print(e.pgerror)
    else:
        con.close()


def copy(file, table):
    # connect_database('''\'COPY AIRPORT FROM 'Movie_Actors.csv' DELIMITER ';' CSV HEADER;''')
    con = psycopg2.connect(dbname="films")
    cur = con.cursor()
    csv_file_name = file
    sql = "COPY {} FROM STDIN DELIMITER ',' CSV HEADER".format(table)
    cur.copy_expert(sql, open(csv_file_name, "r"))
    con.commit()
    print("The file has been copied to the table")


def create_writer_table(file):
    '''Create a table '''

    # Use read file function
    header = read_file(file)
    print(header)
    # unpack the header into the table values
    createTable = '''CREATE TABLE WRITER(
                  {} integer, {} text, {} text,
                  {} text); '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    copy(file, "WRITER")


def create_actor_table(file):
    '''Create a table '''

    # Use read file function
    header = read_file(file)
    print(header)
    # unpack the header into the table values
    createTable = '''CREATE TABLE ACTOR(
                  {} text, {} text, {} text);
                  '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    copy(file, "ACTOR")


def create_rating_table(file):
    '''Create a table '''

    # Use read file function
    header = read_file(file)
    print(header)
    # unpack the header into the table values
    createTable = '''CREATE TABLE RATING(
                  {} integer, {} text, {} text,
                  {} text); '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    copy(file, "RATING")

def create_genre_table(file):
    '''Create a table '''

    # Use read file function
    header = read_file(file)
    print(header)
    # unpack the header into the table values
    createTable = '''CREATE TABLE GENRE(
                  {} integer, {} text, {} text); '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    copy(file, "GENRE")

def create_movie_table(file):
    '''Create a table '''

    # Use read file function
    header = read_file(file)
    print(header)
    # unpack the header into the table values
    createTable = '''CREATE TABLE MOVIE(
                  {} text, {} text, {} text,
                  {} text, {} text, {} text,
                  {} text, {} text, {} text,
                  {} text, {} text, {} text,
                  {} text, {} text, {} text,
                  {} text, {} text, {} text
                  );'''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    copy(file, "MOVIE")

def creat_all():

    create_db()
    create_writer_table('datasets/Movie_Writer.csv')
    create_actor_table('datasets/Movie_Actors.csv')
    create_genre_table('datasets/Movie_Genres.csv')
    create_rating_table('datasets/Movie_AdditionalRating.csv')
    create_movie_table('datasets/Movie_Movies.csv')


if __name__ == '__main__':
    creat_all()
