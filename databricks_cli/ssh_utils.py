
import ssl
import sys
import traceback
import websocket

try:
    import thread
except ImportError:
    import _thread as thread

MESSAGE_OUTPUT_FRAME = 2
MESSAGE_ERROR_FRAME = 3
MESSAGE_EXITED = 4

MESSAGE_INPUT_FRAME = 64
MESSAGE_TERMSIZE = 65
MESSAGE_INPUT_CLOSED = 66

def send_control_message(ws, message_type, message_bytes):
    full_msg = bytearray([message_type]) + message_bytes
    ws.send(str(full_msg), websocket.ABNF.OPCODE_BINARY)


def create_websocket(api_client, cluster_id, on_text_message, on_binary_message):
    def on_error(ws, error):
        sys.stderr.write("Remote connection hung up with error: %s" % error)
        traceback.print_exc()

    def internal_on_data(ws, message, opcode, ignored):
        try:
            if opcode == ABNF.OPCODE_TEXT:
                return on_text_message(message)
            bytes = bytearray(message)
            stream_indicator = bytes[0]
            msg_bytes = bytearray(message[1:])
            on_binary_message(ws, stream_indicator, msg_bytes)
        except Exception, err:
            traceback.print_exc()
            pass

    host = api_client.host
    host = host.replace("https://", "wss://")
    host = host.replace("http://", "ws://")
    if not host.endswith("/"):
        host += "/"
    host += "ws-cluster-proxy/" + cluster_id
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(host,
                                header = api_client.default_headers,
                                on_data  = internal_on_data,
                                on_error = on_error)
    sslopt = None
    if not api_client.verify:
        sslopt = {"cert_reqs": ssl.CERT_NONE}
    return ws, sslopt
