#!/usr/bin/env python3
import psycopg2
import csv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMI

def read_file():
    '''Read CSV file and get the header'''
    with open(r"Movie_Actors.csv", 'r') as f:
        fString = f.read()
    header = fString.split('\n')[0].split(',')
    return header


def create_db():
    con = psycopg2.connect(dbname='postgres')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = con.cursor()
    cur.execute("CREATE DATABASE MOVIES")


def connect_database(query):
    '''Connect to postgrelsql DB using psycopg2 DB-API '''
    try:
        pg = psycopg2.connect(dbname="MOVIES")
        c = pg.cursor()
        c.execute(query)
        pg.commit()
    except psycopg2.Error as e:
        print("Unable to connect!")
        # print the error message
        print(e.pgerror)
    else:
        pg.close()
