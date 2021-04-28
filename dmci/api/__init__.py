"""
DMCI : Api init
=================

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

from flask import Flask, request

def validate_mmd(data):
    # Takes in bytes-object data
    # Gives msg when both validating and not validating
    if data == bytes("<xml: notXml", "utf-8"):
        return False, "Fails"
    return True, "Checks out"


    

app = Flask(__name__)
@app.route('/', methods=['POST'])
def base():
    data = request.get_data()

    result, msg = validate_mmd(data)


    if result:
        return msg, 200
    else:
        return msg, 500
