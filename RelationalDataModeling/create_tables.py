import psycopg2
from sql_queries import create_table_queries, drop_table_queries
""" 
        The function to add two Complex Numbers. 
  
        Parameters: 
            num (ComplexNumber): The complex number to be added. 
          
        Returns: 
            ComplexNumber: A complex number which contains the sum. 
        """

def create_database():
"""
    This function connect to the local postgres server and refresh the sparkifydb database by dropping it if it exists, then create it.

    Parameters: 
        None
        
    Returns:
        None
        
"""
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
"""
    This function loops through the drop_table_queries (loaded from the sql_queries.py file) and execute all the drop table statement against the postgres database.
    
    Parameters: 
        cur (cursor): The cursor object created when making the database connection
        conn (connection): The connection object created to connect to the database
        
    Returns:
        None
"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
"""
    This function loops through the create_table_queries (loaded from the sql_queries file) and execute all the create table queries against the postgres database.
    
    Parameters: 
        None
        
    Returns:
        None
"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
"""
    This is the main function of the create_tables.py script.  It calls the drop_tables method to drop all the tables then calls the create_tables method to create all the tables.
    
    Parameters: 
        None
        
    Returns:
        None
"""
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()