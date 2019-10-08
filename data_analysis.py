import psycopg2
from database_setup import get_data

def top_directors():
    '''Get the top 10 Directors'''

    file= open("top_directors.txt","w+")

    top_ten = get_data('''select Director, count(Title)  as num from movie where
                       Director is not null and Type = 'movie' group by Director
                       order by num DESC limit 10;''')
    if top_ten:
        file.write("The top 10 Directors are the following: \n")
        c = 1
        for i in top_ten:
            file.write("{}. {} directed {} movies. \n".format(c, i[0], i[1]))
            c += 1
    else:
        file.write("There are no Directors!")
    file.close()

def actor_movie():
    '''Get the movie and actor '''
    file= open("actor_movie.txt","w+")
    # get in which movie act the actor
    actor = get_data('''select movie.Title, actor.Actors from movie
                     join actor on movie.imdbID = actor.imdbID limit 100;''')

    if actor:
        c = 1
        for i in actor:
            file.write("{}. Actor {} performed  in the '{}' film. \n".format(c, i[1], i[0]))
            c += 1
    else:
        file.write("There are no movies!")
    file.close()

def get_teamwork():
    '''Get the film team '''
    file= open("team_movie.txt","w+")
    # Fetch the team who work on certain movie
    team = get_data('''select writer.Person, movie.Director, actor.Actors, movie.Title
                 from actor inner join writer on actor.imdbID = writer.imdbID
                 inner join movie on movie.imdbID = actor.imdbID order by movie.Released limit 10;''')
    for i in team:
        file.write("The '{}' film: \n".format(i[3]))
        file.write("\t {} as the Director \n".format(i[1]))
        file.write("\t {} as the Actor \n".format(i[2]))
        file.write("\t {} as the Writer \n\n".format(i[0]))

    file.close()

if __name__ == '__main__':
    top_directors()
    actor_movie()
    get_teamwork()
