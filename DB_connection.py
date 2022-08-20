class sql:
    """
    NAME
        sql

    DESCRIPTION
        The class "sql" was used to execute query in MYSQL workbench.

    PARAMETER
        hostname, username, password

    METHOD:
        It has one method query_execute
    """

    def __init__(self, host, user, passwrd):
        """
        :param host: Enter a hostname
        :param user: Enter a username
        :param passwrd: Password should be in string. If it is a number, then convert it into a string
        """
        import logging
        logging.basicConfig(filename='assignment.log', level=logging.INFO,
                            format="%(levelname)s %(name)s %(asctime)s %(message)s ")
        logging.info('\n')
        logging.info('NEW LOG STARTED')
        logging.info('Call to class SQL')
        self.hostname = host
        self.user = user
        self.passwrd = passwrd

    def query_execute(self, query):
        """
        Use print statement to print the output of the query in form of DataFrame.
        :param query: Query to be executed 
        :return: Query result in form of a data frame 
        """
        import logging
        logging.basicConfig(filename='assignment.log', level=logging.INFO,
                            format="%(levelname)s %(asctime)s %(message)s ")
        try:
            import mysql.connector as conn
            db_sql = conn.connect(host=self.hostname, user=self.user, passwd=self.passwrd)
            logging.info('Connection to local DB successful')
            cursor = db_sql.cursor()
            logging.info('Cursor created successfully')
            cursor.execute(query)
            logging.info('Query successfully executed')
            logging.debug('Extracting Data')
            lst = []
            col_len = 0
            for i in cursor:
                lst.append(i)
                col_len = len(i)
            logging.debug('importing pandas')
            import pandas as pd
            col = ['Col{}'.format(i + 1) for i in range(col_len)]
            df = pd.DataFrame(lst, columns=col)
            logging.info('Data extracted successfully')
            db_sql.commit()
            logging.info('Committing the query')
            cursor.close()
            logging.info('cursor was closed')
            if len(df.index) == 0:
                return None
            else:
                logging.info('Returned Data to user')
                return df
        except Exception as e:
            logging.exception(e)
            import sys
            a, b, c = sys.exc_info()
            return a, b, c.tb_lineno


