import atexit
import json
import re
from typing import Union

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from pysnowflake.common import Template, SnowflakeObjectType


class Snowflake:
    def __init__(self, **kwargs):
        self._con = None
        self._con_settings = kwargs
        if warehouse := self._con_settings.get('warehouse'):
            self._current_warehouse = warehouse
        if database := self._con_settings.get('database'):
            self._current_database = database
        if schema := self._con_settings.get('schema'):
            self._current_schema = schema

        self.user = User(self)
        self.role = Role(self)
        self.template = Template()

    def _connect(self):
        con = snowflake.connector.connect(**self._con_settings)
        self._con = con
        atexit.register(self.con.close)

    @property
    def con(self) -> snowflake.connector.connection:
        if not self._con:
            self._connect()
        return self._con

    @property
    def cursor(self) -> snowflake.connector.cursor:
        return self.con.cursor()

    @property
    def warehouse(self) -> str:
        if not self._con:
            return self._current_warehouse
        warehouses = self.query('SHOW WAREHOUSES', return_type='df')
        self._current_warehouse = warehouses[warehouses['is_default'] == 'Y']['name']
        return self._current_warehouse

    def _create_from_json(self, file_path: str, object_type: SnowflakeObjectType = SnowflakeObjectType.TABLE):
        with open(file_path, 'r') as f:
            data = json.load(f)
        data['columns'] = [re.sub(r'\n+', ' ', self.template.load_template('column', col)).strip() for col in data['columns']]
        query = re.sub('\n+', '\n', self.template.load_template('create', data=data, type=object_type)).strip()
        return self.query(query, return_type='log')

    def create_table_from_json(self, file_path: str) -> str:
        return self._create_from_json(file_path)

    def create_view_from_json(self, file_path: str) -> str:
        return self._create_from_json(file_path, object_type=SnowflakeObjectType.VIEW)

    def create_task_from_json(self, file_path: str) -> str:
        return self._create_from_json(file_path, object_type=SnowflakeObjectType.TASK)

    def insert_data(self, table_name: str, data: pd.DataFrame) -> None:
        _, _, _, output = write_pandas(self._con, data, table_name)
        return output

    def get_data(self, query: str) -> pd.DataFrame:
        return self.query(query, return_type='df')

    def truncate_table(self, table_name: str) -> str:
        return self.query(f'TRUNCATE TABLE {table_name}', return_type='log')

    def drop(self, name: str, object_type: SnowflakeObjectType = SnowflakeObjectType.TABLE) -> str:
        return self.query(f'DROP {object_type} IF EXISTS {name}', return_type='log')

    def create_schema(self, schema_name: str, replace: bool = False) -> str:
        query = f'CREATE SCHEMA IF NOT EXISTS {schema_name}'
        if replace:
            query = f'CREATE OR REPLACE SCHEMA {schema_name}'
        return self.query(query, return_type='log')

    def create_database(self, database_name: str, replace: bool = False) -> str:
        query = f'CREATE DATABASE IF NOT EXISTS {database_name}'
        if replace:
            query = f'CREATE OR REPLACE DATABASE {database_name}'
        return self.query(query, return_type='log')

    def query(self, query: str, return_type: str = None) -> Union[pd.DataFrame, str, None]:
        cur = self.cursor
        if query.strip().endswith('.sql'):
            with open(query, 'r') as f:
                query = f.read().strip()
        cur.execute(query)
        result = None
        if return_type == 'df':
            result = cur.fetch_pandas_all()
        elif return_type == 'list':
            result = cur.fetchall()
        elif return_type == 'log' or not return_type:
            result = cur.fetchone()[0]
        cur.close()
        return result

    def use(self, warehouse: str = None, database: str = None, schema: str = None):
        if warehouse:
            self._current_warehouse = warehouse
            self.query(f'USE WAREHOUSE {warehouse}')
        if database:
            self._current_database = database
            self.query(f'USE DATABASE {database}')
        if schema:
            self._current_schema = schema
            self.query(f'USE SCHEMA {schema}')


class User:
    def __init__(self, sf: 'Snowflake'):
        self.sf = sf

    def create(self, user_name: str, password: str, email: str, role: str, default_warehouse: str = None):
        query = f"CREATE USER {user_name} IF NOT EXISTS PASSWORD='{password}' DEFAULT_ROLE = {role} " \
                f"MUST_CHANGE_PASSWORD = TRUE EMAIL = '{email}' default_warehouse = {default_warehouse};"
        return self.sf.query(query, return_type='log')

    def remove(self, user_name: str):
        return self.sf.query(f"DROP USER IF EXISTS {user_name}", return_type='log')

    def reset_password(self, user_name: str):
        return self.sf.query(f"ALTER USER IF EXISTS {user_name} RESET PASSWORD", return_type='log')

    def add_role(self, user_name: str, role: str):
        return self.sf.query(f"GRANT ROLE {role} TO USER {user_name}", return_type='log')

    def remove_role(self, user_name: str, role: str):
        return self.sf.query(f"REVOKE ROLE {role} FROM USER {user_name}", return_type='log')

    def describe(self, user_name: str) -> pd.DataFrame:
        return self.sf.query(f'DESCRIBE USER {user_name}', return_type='df')

    def all(self) -> pd.DataFrame:
        return self.sf.query('SHOW USERS', return_type='df')


class Role:
    def __init__(self, sf: 'Snowflake'):
        self.sf = sf

    def all(self) -> pd.DataFrame:
        return self.sf.query('SHOW ROLES', return_type='df')

    def create(self, role_name: str, comment: str = '', tag: str = ''):
        return self.sf.query(f'CREATE ROLE IF NOT EXISTS \'{role_name}\' COMMENT = \'{comment}\' TAG = \'{tag}\'',
                             return_type='log')

    def remove(self, role_name: str):
        return self.sf.query(f'DROP ROLE IF EXISTS \'{role_name}\'', return_type='log')

    def grant_privilege_to_all_tables(self, privilege: str, schema: str, role: str):
        return self.sf.query(f'GRANT {privilege} ON ALL TABLES IN SCHEMA {schema} TO ROLE {role}', return_type='log')

    def grant_privilege(self, privilege: str, object_name: str, object_type: str, role: str):
        return self.sf.query(f'GRANT {privilege} ON {object_type} {object_name} TO ROLE {role}', return_type='log')

    def grant_imported_privileges(self, database, role):
        return self.sf.query(f'GRANT IMPORTED PRIVILEGES ON DATABASE {database} TO ROLE {role}', return_type='log')

    def revoke_privilege(self, privilege: str, object_name: str, object_type: str, role: str):
        return self.sf.query(f'REVOKE {privilege} ON {object_type} {object_name} FROM ROLE {role}', return_type='log')
