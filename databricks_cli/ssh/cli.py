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

from json import loads as json_loads

import click
import ssl
import struct
import termios
import sys
from tabulate import tabulate
import tty
from select import select
import traceback

from databricks_cli.click_types import OutputClickType, JsonClickType, JobIdClickType
from databricks_cli.jobs.api import JobsApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base, \
    truncate_string
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.version import print_version_callback, version


import websocket
try:
    import thread
except ImportError:
    import _thread as thread
import time

STREAM_INDICATOR_OUTPUT = 1
STREAM_INDICATOR_ERROR = 2
STREAM_INDICATOR_STATUS = 3

INPUT_INDICATOR_STDIN = 1

def on_data(ws, message, opcode, initial):
    # print("[Received: %s, opcode=%s, len=%s]" % (type(message), opcode, len(message)))
    pass

exit_code = None

def on_message(ws, message):
    global exit_code
    try:
        # WARNING: python2 compatability problems!!
        bytes = bytearray(message)
        stream_indicator = bytes[0]
        # print("INDICATOR=%s" % stream_indicator)
        # ignore length
        str_msg = bytearray(message[5:])
        if stream_indicator == STREAM_INDICATOR_OUTPUT:
            # sys.stdout.write(str_msg)
            # sys.stdout.flush()
            pass
        elif stream_indicator == STREAM_INDICATOR_ERROR:
            sys.stderr.write(str_msg)
            sys.stderr.flush()
        elif stream_indicator == STREAM_INDICATOR_STATUS:
            exit_code = struct.unpack("<L", bytearray(bytes[1:5]))[0]
            # print("exit code set: %s" % exit_code)
        else:
            sys.stderr.write("INDICATOR=%s" % stream_indicator)
            raise Error("Unknown stream indicator: %s" % stream_indicator)
    except Exception, err:
        traceback.print_exc()

def on_error(ws, error):
    sys.stderr.write("Error: %s" % error)

def on_close(ws):
    # sys.stderr.write("### closed ###")
    pass

def has_stdin():
    return select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])

def stdin_loop(ws):
    old_settings = termios.tcgetattr(sys.stdin)
    try:
        tty.setcbreak(sys.stdin.fileno())
        while True:
            readables, writeables, exceptions = select([sys.stdin], [], [], 0.001)
            # print("Have bytes? %s" % has_stdin())
            buf = bytearray()
            buf.append(INPUT_INDICATOR_STDIN)
            while has_stdin() and len(buf) < 1024:
                b=sys.stdin.read(1)
                # print("Now has_stdin=%s" % has_stdin())
                buf.append(b)
            if len(buf) > 1:
                ws.send(str(buf), websocket.ABNF.OPCODE_BINARY)
                sys.stderr.write("Sent %s bytes" % len(buf))
    except Exception, err:
        traceback.print_exc()
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("command")
@profile_option
@eat_exceptions
@provide_api_client
def exec_cmd(api_client, command):
    """
    Creates a job.

    The specification for the json option can be found
    https://docs.databricks.com/api/latest/jobs.html#create
    """
    global exit_code

    def on_open(ws):
        try:
            thread.start_new_thread(stdin_loop, (ws,))
        except Exception, err:
            traceback.print_exc()

        ws.send('{ "command": { "command" : "%s" } }' % command)

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(#"wss://%s/" % api_client.host,
                              "ws://localhost:5059/",
                              header = api_client.default_headers,
                              on_data = on_data,
                              on_message = on_message,
                              on_error = on_error,
                              on_open = on_open,
                              on_close = on_close)
    # TODO: no_verify
    # ssl_opt = {}
    # if not api_client.verify:
    #     ssl_opt = {"cert_reqs": ssl.CERT_NONE}
    ws.run_forever()
    if exit_code:
        print("Exiting %s" % exit_code)
        sys.exit(exit_code)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with jobs.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@profile_option
@eat_exceptions
def ssh_group():
    """
    Utility to interact with jobs.

    This is a wrapper around the jobs API (https://docs.databricks.com/api/latest/jobs.html).
    Job runs are handled by ``databricks runs``.
    """
    pass


ssh_group.add_command(exec_cmd, name='exec')
