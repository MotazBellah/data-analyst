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

def actor_movie():
    f= open("actor_movie.txt","w+")
    x = get_data('''select movie.Title, actor.Actors from movie
                 join actor on movie.imdbID = actor.imdbID limit 100;''')

    if x:
        c = 1
        for i in x:
            f.write("{}. Actor {} performed  in the '{}' film. \n".format(c, i[1], i[0]))
            c += 1
    else:
        f.write("There are no movies!")
    f.close()

def get():
    f= open("team_movie.txt","w+")
    x = get_data('''select writer.Person, movie.Director, actor.Actors, movie.Title
                 from actor inner join writer on actor.imdbID = writer.imdbID
                 inner join movie on movie.imdbID = actor.imdbID order by movie.Released limit 10;''')
    for i in x:
        f.write("The '{}' film: \n".format(i[3]))
        f.write("\t {} as the Director \n".format(i[1]))
        f.write("\t {} as the Actor \n".format(i[2]))
        f.write("\t {} as the Writer \n\n".format(i[0]))

    f.close()

get()
