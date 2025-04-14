from airflow.models.connection import Connection
from airflow import settings

def create_db_conn(host, user, port, password):
    """Creates a connection between Airflow and a database in Postgres

    Args:
        host (str): Database host
        user (str): Database user
        port (str): Database port
        password (str): User password
        database (str): Database name

    Returns:
        str: connection id
    """    
    # Creates connection URI
    c = Connection(
        conn_id="postgres_localhost",
        conn_type="postgres",
        description="Connects to postgres DB",
        host=host,
        login=user,
        port=port,
        password=password,
    )
    session = settings.Session()

    # Check if connection exists, if not creates a new one
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == c.conn_id).first()
    )

    if str(conn_name) == str(c.conn_id):
        return c.conn_id

    session.add(c)
    session.commit()
    
    return c.conn_id