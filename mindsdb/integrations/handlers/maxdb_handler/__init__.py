from mindsdb.integrations.libs.const import HANDLER_TYPE

try:
    from .maxdb_handler import (
        MaxDBHandler as Handler,
        connection_args,
        connection_args_example,
    )
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
from .__about__ import __version__ as version, __description__ as description


title = 'Sap MaxDB'
name = 'maxdb'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'connection_args', 'connection_args_example', 'import_error', 'icon_path'
]
