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

import atexit
import click
import json
import os
import struct
import termios
from select import select
import sys
import time
import tty
import traceback
import websocket

try:
    from shutil import get_terminal_size
except ImportError:
    from backports.shutil_get_terminal_size import get_terminal_size

try:
    import thread
except ImportError:
    import _thread as thread

from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS
from databricks_cli.ssh_utils import create_websocket, send_control_message, \
  MESSAGE_OUTPUT_FRAME, MESSAGE_ERROR_FRAME, MESSAGE_EXITED, \
  MESSAGE_INPUT_FRAME, MESSAGE_TERMSIZE


exit_code = None
command_to_run = None
last_termsize = None
allocate_tty = False

def log_debug(msg):
    print(msg)

def on_connect(ws):
    log_debug("Connect completed")
    commandJson = json.dumps({
        "command_list": command_to_run, # TODO
        "allocate_pty": allocate_tty,
    })
    ws.send(commandJson)
    thread.start_new_thread(maintain_termsize, (ws,))
    thread.start_new_thread(stdin_loop, (ws,))

def on_text_message(ws, message_str):
    response = json.loads(message_str)
    assert response["connected"], "Invalid connect message: %s" % message_str
    on_connect(ws)

def on_binary_message(ws, message_type, message_bytes):
    log_debug("Binary message receieved: %s" % message_type)
    if message_type == MESSAGE_OUTPUT_FRAME:
        sys.stdout.write(message_bytes)
        sys.stdout.flush()
    elif message_type == MESSAGE_ERROR_FRAME:
        sys.stderr.write(message_bytes)
        sys.stderr.flush()
    elif message_type == MESSAGE_EXITED:
        # TODO: This is not correct, negative numbers are exploded.
        # print("MESSAGE: %s %s" % (message_type, message_bytes))
        exit_code = int(message_bytes.decode("UTF-8"))
    else:
        raise Error("Unknown stream indicator: %s" % message_type)

def stdin_loop(ws):
    try:
        tty.setraw(sys.stdin.fileno())
        while True:
            readables, writeables, exceptions = select([sys.stdin], [], [], 0.025)
            bytes_read = os.read(sys.stdin.fileno(), 1024)
            buf = bytearray(bytes_read)
            if buf:
                send_control_message(ws, MESSAGE_INPUT_FRAME, buf)
    except Exception, err:
        traceback.print_exc()

def send_termsize(ws):
    global last_termsize
    term_size = get_terminal_size((80, 20))

    if last_termsize == term_size:
        return

    last_termsize = term_size
    termsize_buf = bytearray.fromhex('{:08x}'.format(term_size.columns))
    termsize_buf += bytearray.fromhex('{:08x}'.format(term_size.lines))
    ws.send(str(termsize_buf), websocket.ABNF.OPCODE_BINARY)
    send_control_message(ws, MESSAGE_TERMSIZE, termsize_buf)

def maintain_termsize(ws):
    try:
        while True:
            send_termsize(ws)
            time.sleep(0.25)
    except Exception, err:
        traceback.print_exc()

def restore_terminal_at_exit():
    old_settings = termios.tcgetattr(sys.stdin)
    def restore_terminal():
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

    atexit.register(restore_terminal)



@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--tty", "-t", is_flag=True, default=False, help="Allocate a tty")
@click.argument("cluster-id")
@click.argument("command", nargs=-1)
@profile_option
@eat_exceptions
@provide_api_client
def shell_cmd(api_client, tty, cluster_id, command):
    """
    Creates a job.

    The specification for the json option can be found
    https://docs.databricks.com/api/latest/jobs.html#create
    """
    global exit_code, command_to_run, allocate_tty
    allocate_tty = tty
    command_to_run = list(command)
    if not command_to_run:
        command_to_run = ["/bin/bash"]
    restore_terminal_at_exit()

    ws, sslopt = create_websocket(api_client, cluster_id, on_text_message, on_binary_message)
    log_debug("Created websocket")
    ws.run_forever(sslopt=sslopt)
    log_debug("Ran forever")

    if exit_code:
        sys.exit(exit_code)
