import psycopg2
from database_setup import get_data

def top_directors():
    # query = "select Title from movie where Director='Rose Cummings';"
    f= open("top_directors.txt","w+")

    x = get_data('''select Director, count(Title)  as num from movie where
                 Director is not null and Type = 'movie' group by Director
                 order by num DESC limit 10;''')
    if x:
        f.write("The top 10 Directors are the following: \n")
        c = 1
        for i in x:
            f.write("{}. {} directed {} movies. \n".format(c, i[0], i[1]))
            c += 1
    else:
        f.write("There are no Directors!")
    f.close()
