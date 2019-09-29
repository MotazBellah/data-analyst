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
