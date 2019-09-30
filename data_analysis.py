import psycopg2
from database_setup import get_data

def display_data():
    # query = "select Title from movie where Director='Rose Cummings';"
    x = get_data("select Director, count(*)  as num from movie where Director is not null and Type = 'movie' group by Director order by num DESC limit 10;")
    return x

# print(display_data())

for i in display_data():
    print(i[0].decode('ascii', 'ignore'), i[1])
