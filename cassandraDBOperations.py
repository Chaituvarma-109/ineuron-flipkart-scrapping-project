from typing import List

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CassandraDBManagement:
    def __init__(self, username: str, password: str, path: str, keyspace: str) -> None:
        """
        This is constructor or initializer function.

        Args:
            username: Username for the DataBase.
            password: Password for the DataBase.
        """
        self.username = username
        self.password = password
        self.config = {'secure_connect_bundle': f'{path}'}
        self.keyspace = keyspace

    def get_cassandra_client_object(self):
        """
        This function establishes the connection with Cassandra Database.

        Returns:
            This will return the session object.
        """
        auth_provider = PlainTextAuthProvider(self.username, self.password)
        cluster = Cluster(cloud=self.config, auth_provider=auth_provider)
        session = cluster.connect()
        return session

    def create_keyspace(self, class_name: str, r_factor: int) -> None:
        """
        This function creates KEYSPACE in specified DATABASE.

        Args:
            class_name: this specifies the type of strategy for the keyspace we are creating.
            r_factor: this integer which determines the data to replicate in data centres.

        Returns:
            Will not return anything
        """
        class_replication_factor = {'class': class_name, 'replication_factor': r_factor}
        query = f"create keyspace if not exists {self.keyspace} with replication = {class_replication_factor};"
        session = self.get_cassandra_client_object()
        session.execute(query)

    def create_table(self, table_name: str, values: str) -> None:
        """
        This function creates the TABLE in specified KEYSPACE.

        Args:
            table_name: this is table name of table that we are creating.
            values: Values are attributes of the table that we are creating.

        Returns:
            Will not return anything
        """
        select_keyspace = f"use {self.keyspace};"
        query = f"create table if not exists {self.keyspace}.{table_name} ({values});"
        session = self.get_cassandra_client_object()
        session.execute(select_keyspace)
        session.execute(query)

    def delete_table(self, table_name: str) -> None:
        """
        This function deletes the specified table from the keyspace

        Args:
            table_name: Name of the Table to delete

        Returns:
             Will not return anything
        """
        query = f"drop table if exists {self.keyspace}.{table_name};"
        session = self.get_cassandra_client_object()
        session.execute(query)

    def delete_keyspace(self):
        """
        This function Drops(delete) KEYSPACE present in DATABASE.

        Returns:
             Will not return anything
        """
        query = f"drop keyspace if exists {self.keyspace};"
        session = self.get_cassandra_client_object()
        session.execute(query)

    def insert_data(self, table_name: str, columns: str, values: str) -> None:
        """
        This function inserts the DATA into table.

        Args:
            table_name: Names of the TABLE to be inserted.
            columns: Names of the COLUMNS in the TABLE we ae inserting.
            values: DATA to insert into COLUMNS of the TABLE.

        Returns:
            Will not return anything.
        """
        query = f"insert into {self.keyspace}.{table_name} ({columns}) values ({values})"
        session = self.get_cassandra_client_object()
        session.execute(query)

    def update_data(self, table_name: str, update_col: str, values: str, primary_key_col: str, primary_val: int,
                    where: bool = False) -> None:
        """
        This function UPDATES the DATA present in the specified COLUMN or COLUMNS of a TABLE.

        Args:
            table_name: name of the table to update date.
            update_col: these are specific column names to update the data
            values: these are values to update.
            primary_key_col: this is name of primary key column.
            primary_val: this is value of specified primary_key_column.
            where: condition argument.

        Returns:
            Will not return Nothing.
        """
        session = self.get_cassandra_client_object()
        if where:
            query = f"update {self.keyspace}.{table_name} set {update_col}={values}"
            session.execute(query)
        else:
            query = f"update {self.keyspace}.{table_name} set {update_col}={values} where {primary_key_col}=" \
                    f"{primary_val}"
            session.execute(query)
