#!/usr/bin/env python3
import psycopg2
import json
import csv
import sys
from psycopg2.extras import RealDictCursor
from itertools import islice
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# Read the file and return the header
def read_file(file_path):
    '''Read CSV file and get the header'''
    with open(file_path, 'r') as f:
        fString = f.read()
    header = fString.split('\n')[0].split(',')
    return header

# Create DB
def create_db():
    '''Connect to postgres and create films DB '''
    con = psycopg2.connect(dbname='postgres')

    cur = con.cursor()
    cur.execute("CREATE DATABASE movie")
    con.commit()
    print("Database has been created")
    con.close()


def get_data(*qureies):
    '''connect to the database and run some qureies'''
    try:
        con = psycopg2.connect(dbname="movie")
        cur = con.cursor()
    except psycopg2.Error as e:
        print ("Unable to connect!")
        print (e.pgerror)
        print (e.diag.message_detail)
        sys.exit(1)
    else:
        for qurey in qureies:
            cur.execute(qurey)
        d = cur.fetchall()
        con.close()
    return d


def connect_database(query):
    '''Connect to postgrelsql DB using psycopg2 DB-API '''
    try:
        con = psycopg2.connect(dbname="movie", user='vagrant')
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        print("The query has been excuted")
    except psycopg2.Error as e:
        print("Unable to connect!")
        # print the error message
        print (e.pgerror)
        print (e.diag.message_detail)
        sys.exit(1)
    else:
        con.close()


def copy(file, table):
    '''Load the data from CSV to DB table'''
    try:
        con = psycopg2.connect(dbname="movie")
        cur = con.cursor()
    except psycopg2.Error as e:
        print ("Unable to connect!")
        print (e.pgerror)
        print (e.diag.message_detail)
        sys.exit(1)
    else:
        # Copy the CSV file to DB table
        csv_file_name = file
        sql = "COPY {} FROM STDIN DELIMITER ',' CSV HEADER".format(table)
        cur.copy_expert(sql, open(csv_file_name, "r"))
        con.commit()
        print("The file has been copied to the table")


def create_writer_table(file):
    '''Create writer table '''
    # Use read file function
    header = read_file(file)
    # unpack the header into the table values
    createTable = '''CREATE TABLE WRITER(
                  {} integer, {} text, {} text,
                  {} text); '''.format(*header)
    # connect to the database and create the table
    connect_database(createTable)
    # copy the CSV file to the table
    copy(file, "WRITER")


def create_actor_table(file):
    '''Create actor table '''
    # Use read file function
    header = read_file(file)
    # unpack the header into the table values
    createTable = '''CREATE TABLE ACTOR(
                  {} text, {} text, {} text);
                  '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    # copy the CSV file to the table
    copy(file, "ACTOR")


def create_rating_table(file):
    '''Create rating table '''
    # Use read file function
    header = read_file(file)
    # unpack the header into the table values
    createTable = '''CREATE TABLE RATING(
                  {} integer, {} text, {} text,
                  {} text); '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    # copy the CSV file to the table
    copy(file, "RATING")


def create_genre_table(file):
    '''Create genre table '''
    # Use read file function
    header = read_file(file)
    # unpack the header into the table values
    createTable = '''CREATE TABLE GENRE(
                  {} integer, {} text, {} text); '''.format(*header)
    # connect to the database and run the query
    connect_database(createTable)
    # copy the CSV file to the table
    copy(file, "GENRE")

def create_movie_table(file):
    '''Create movie table '''
    # Use read file function
    header = read_file(file)
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
    # copy the CSV file to the table
    copy(file, "MOVIE")

def creat_all():
    '''Call all the function and create DB'''
    create_db()
    create_writer_table('datasets/Movie_Writer.csv')
    create_actor_table('datasets/Movie_Actors.csv')
    create_genre_table('datasets/Movie_Genres.csv')
    create_rating_table('datasets/Movie_AdditionalRating.csv')
    create_movie_table('datasets/Movie_Movies.csv')


if __name__ == '__main__':
    creat_all()
