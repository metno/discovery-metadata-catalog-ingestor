import os
import sys

# Note: This line forces the test suite to import the dmci package in the current source tree
sys.path.insert(
    1, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                    os.path.pardir)))

from dmci.api import App
from dmci import CONFIG

CONFIG.readConfig()

app = App()._app
# app._app.run()%     
