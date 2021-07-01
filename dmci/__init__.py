"""
DMCI : Main Package Init
========================

Copyright 2021 MET Norway

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

__package__ = "dmci"
__version__ = "0.1"

import os
import sys
import logging

from dmci.config import Config


def _init_logging(log_obj):
    """Call to initialise logging
    """
    # Read environment variables
    want_level = os.environ.get("DMCI_LOGLEVEL", "INFO")
    log_file = os.environ.get("DMCI_LOGFILE", None)

    # Determine log level and format
    if hasattr(logging, want_level):
        log_level = getattr(logging, want_level)
    else:
        print("Invalid logging level '%s' in environment variable DMCI_LOGLEVEL" % want_level)
        log_level = logging.INFO

    if log_level < logging.INFO:
        msg_format = "[{asctime:}] {name:>28}:{lineno:<4d} {levelname:8s} {message:}"
    else:
        msg_format = "{levelname:8s} {message:}"

    log_format = logging.Formatter(fmt=msg_format, style="{")
    log_obj.setLevel(log_level)

    # Create stream handlers
    h_stdout = logging.StreamHandler()
    h_stdout.setLevel(log_level)
    h_stdout.setFormatter(log_format)
    log_obj.addHandler(h_stdout)

    if log_file is not None:
        h_file = logging.FileHandler(log_file, encoding="utf-8")
        h_file.setLevel(log_level)
        h_file.setFormatter(log_format)
        log_obj.addHandler(h_file)

    return


# Logging Setup
# Must be called before the CONFIG object is created
logger = logging.getLogger(__name__)
_init_logging(logger)

# Create config object
CONFIG = Config()


def api_main():
    """This is the main entry point for the api process.
    """
    from dmci.api import App

    if not CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None)):
        sys.exit(1)

    dmci_app = App()
    sys.exit(dmci_app.run())

# END api_main entry point
