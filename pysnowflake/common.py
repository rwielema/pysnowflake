import os
from enum import Enum
from typing import Any

import jinja2


class Template:
    def __init__(self, template_folder_path: str = None):
        self._template_folder = template_folder_path
        if not self._template_folder:
            self._template_folder = os.path.join(os.getcwd(), 'templates')
        self.ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(self._template_folder))

    def set_template_folder(self, template_folder_path: str):
        self._template_folder = template_folder_path
        self.ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(self._template_folder))

    def load_template(self, template_name: str, data: Any, **kwargs):
        files = self.ENV.list_templates()
        file = [file for file in files if file.startswith(template_name)]
        if not file:
            raise ValueError(f'{template_name} not found in {self._template_folder}')
        template = self.ENV.get_template(file[0])
        return template.render(data=data, **kwargs)


class SnowflakeObjectType(Enum):
    VIEW = 'VIEW'
    MATERIALIZED_VIEW = 'MATERIALIZED VIEW'
    TABLE = 'TABLE'
    SCHEMA = 'SCHEMA'
    DATABASE = 'DATABASE'
    WAREHOUSE = 'WAREHOUSE'
    ROLE = 'ROLE'
    USER = 'USER'
    SHARE = 'SHARE'
    PIPE = 'PIPE'
    TASK = 'TASK'
    STAGE = 'STAGE'
    FUNCTION = 'FUNCTION'
    STREAM = 'STREAM'

    def __eq__(self, other):
        if isinstance(other, SnowflakeObjectType):
            return self.value == other.value
        return self.value == other.upper()

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
