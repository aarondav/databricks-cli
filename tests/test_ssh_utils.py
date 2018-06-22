# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import mock
from requests import Response
from requests.exceptions import HTTPError
import ssl

import databricks_cli.utils as utils
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.ssh_utils import create_websocket

observed_msg_type = None
observed_msg_bytes = None
def test_create_websocket():

    def on_message(ws, msg_type, msg_bytes):
        global observed_msg_type, observed_msg_bytes
        observed_msg_type = msg_type
        observed_msg_bytes = msg_bytes

    api_client = ApiClient(user = "admin", password = "admin", host = "https://my-host", verify = True)
    ws, sslopt = create_websocket(api_client, "my-cluster", on_message)
    assert ws.url == "wss://my-host/ws-cluster-proxy/my-cluster"
    assert ws.header['Authorization'] == 'Basic YWRtaW46YWRtaW4='
    assert sslopt == None

    ws.on_message(ws, bytearray([1, 2, 3]))
    assert observed_msg_type == 1
    assert observed_msg_bytes == bytearray([2, 3])

def test_create_websocket_noverify():
    api_client = ApiClient(user = "admin", password = "admin", host = "https://my-host", verify = False)
    ws, sslopt = create_websocket(api_client, "my-cluster", None)
    assert sslopt == {"cert_reqs": ssl.CERT_NONE}
