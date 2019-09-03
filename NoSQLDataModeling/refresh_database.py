import cassandra
from cassandra.cluster import Cluster

from sql_queries import *



def create_database():
    """
        This function connect to the cassandra server and refresh the sparkifydb database by dropping it if it exists, then create it.

        Parameters: 
            None

        Returns:
            None

    """
    try: 
        cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
        session = cluster.connect()
    except Exception as e:
        print(e)
    
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkifydb 
            WITH REPLICATION = 
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )

    except Exception as e:
        print(e)
    
    try:
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    return session, cluster
    

    
def drop_tables(session):
    """
        This function loops through the drop_table_queries (loaded from the sql_queries.py file) and execute all the drop table statement against the cassandra database.

        Parameters: 
            session: The session object created when making the database connection

        Returns:
            None
    """
    for query in drop_table_queries:
        try:
            #print(query)
            session.execute(query)
        except Exception as e:
            print(e)

def create_tables(session):
    """
        This function loops through the create_table_queries (loaded from the sql_queries file) and execute all the create table queries against the cassandra database.

        Parameters: 
             session: The session object created when making the database connection

        Returns:
            None
    """
    for query in create_table_queries:
        try:
            #print(query)
            session.execute(query)
        except Exception as e:
            print(e)

def main():
    """
        This is the main function of the create_tables.py script.  It calls the drop_tables method to drop all the tables then calls the create_tables method to create all the tables.

        Parameters: 
            None

        Returns:
            None
    """
    session, cluster = create_database()
    
    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()
    print("Refresh completed!!!")


if __name__ == "__main__":
    main()