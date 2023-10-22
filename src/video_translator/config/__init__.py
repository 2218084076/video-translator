"""
Configuring center.
Use https://www.dynaconf.com/
"""

import sys
from pathlib import Path

from dynaconf import Dynaconf

_base_dir = Path(__file__).resolve().parent.parent.parent

_settings_files = [
    Path(__file__).parent / 'settings.yml',
]

_external_files = [
    Path(sys.prefix, 'etc', 'video-translator', 'settings.yml'),
]

settings = Dynaconf(
    envvar_prefix=False,
    settings_files=_settings_files,
    load_dotenv=True,
    lowercase_read=False,
    includes=_external_files,
    basedir=_base_dir,
)
