import asyncio
import logging
import json
import traceback
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

from azure.identity.aio import ManagedIdentityCredential
from azure.eventhub.aio import EventHubConsumerClient #, EventData, TransportType
from azure.eventhub import TransportType
import aiofiles
from localcheckpoint_aio import LocalFileCheckpointStore

from config import CONFIG
logging.basicConfig(level=logging.ERROR)

FILEWRITER = {}
MAX_EVENTS=-1
PARSE_TIME=True
DEBUG=False
VERBOSE=False
HTTP_PROXY = {}
CLIENT_ID = None

logger = None
events_received = 0

class AsyncWriter():
    """ Async Writer for the log file """
    def __init__ (self, log, mode, encoding):
        self.log = log
        self.mode = mode
        self.encoding = encoding

        if mode == "w":
            ## Truncate
            pass

    async def write(self, data):
        """ Write to the log file """
        async with aiofiles.open(self.log, "a", encoding=self.encoding) as f:
            await f.write(data)

class SyncWriter():
    """  Sync Writer for the log file to be used on an async loop"""
    def __init__(self, log, mode, encoding):
        self.log = log
        self.mode = mode
        self.encoding = encoding
        self.fp = open(log, mode, encoding=encoding)

    async def write (self, data):
        """ Write to the log file """
        self.fp.write(data)

class NullWriter():
    """ Null Writer for the log file """
    async def write(self, data):
        """ Write to the log file """
        return 0

def debug_prnt(data=None, **kwargs):
    """ Print debug information """
    if DEBUG:
        if data:
            print(data)
        if ex := kwargs.get('ex', None):
            traceback.print_exception(ex)

def vprint(data):
    """ Print verbose information """
    if VERBOSE:
        print(data)

def open_logs(out, mode):
    """ Open the log descriptors """
    if not out:
        FILEWRITER["RAW"] = NullWriter()
        FILEWRITER["EVENT"] = NullWriter()
        return

    if out.lower() in [ "raw", "all" ]:
        print(f"Enabling RAW log output -> {str(Path(__file__).parent/ 'raw_events.log')}")
        FILEWRITER["RAW"] = SyncWriter(str(Path(__file__).parent/ "raw_events.log"), mode, encoding="utf-8")
    else:
        FILEWRITER["RAW"] = NullWriter()

    if out.lower() in  ["events" , "all" ]:
        print("Enabling Events log output")
        FILEWRITER["EVENT"] = SyncWriter("events.log", mode, encoding="utf-8")
    else:
        FILEWRITER["EVENT"] = NullWriter()


def do_time_parse(time, time_s):
    """ Parse the time string using a datetime format string
        time: The time to parse
        time_s: The format string to use
        =======
        return: The parsed datetime object or None if it fails
    """
    try:
        return datetime.strptime(time, time_s)
    except ValueError:
        pass
    except Exception as e:
        print(f"do_time_parse: {e}")
        return None

def get_time(event_record):
    """ Extracts the time of the record
        event_record: The event record to extract the time from
        =======
        return: The parsed datetime object or None if it fails
    """
    time_fields = [ 'time', 'eventTimestamp', 'EventTimeString']
    time = None

    for i in time_fields:
        time = event_record.get(i,  None)
        if time:
            break

    #print(f"This is time:  {time}")

    if not time:
        return None

    try:
        tm = time
        return datetime.fromisoformat(tm.replace('Z', '+00:00'))
    except ValueError:
        pass
    except Exception as e:
        debug_prnt("get_time: Can't extract the timestamp from the event record (ISOfmt)", ex=e)

    try:
        time_pstrings = [ '%m/%d/%Y %I:%M:%S.%f %p %z', '%m/%d/%Y %I:%M:%S %p %z', "%m/%d/%Y %I:%M:%S %p"]

        for t in time_pstrings:
            m_time = do_time_parse(time, t)
            if m_time:
                #vprint(f"Found time:{m_time}")
                return m_time
        print("No time parsed.")
    except ValueError:
        pass
    except Exception as e:
        debug_prnt("get_time: Can't extract the timestamp from the event record (datetime fmt)", ex=e)


    vprint(f"[!] Can't parse time: {time}")
    return None

async def on_event(partition_context, event):
    """ Callback function to process the events received
        partition_context: The partition context
        event: The event received
        =======
    """
    global events_received
    logger.debug("Received event on partition: %s", partition_context.partition_id)

    event_base = {
                'enqueued_time': event.enqueued_time,
                'offset': event.offset
            }

    event_body = event.body_as_json()

    if args.raw:
        print(event_body)

    await FILEWRITER["RAW"].write(json.dumps(event_body) + "\n")

    if not 'records' in event_body:
        print(f"No record found in event: {event_body}")
    else:
        events_received += 1
        for event_record in event_body['records']:
            delay = 0
            record = event_base.copy()
            time = get_time(event_record)

            if time:
                delay = int((event.enqueued_time - time).total_seconds())

            if delay ==  0:
                print(f"MY TIME: {time} / {event.enqueued_time}")
                print(f"DELAY:{delay}")
                print(event_record)
                print("")
            record.update({
                            "time": time,
                            "operationName": event_record.get('operationName', None),
                            "correlationId": event_record.get('correlationId', None),
                            "activityId": event_record.get('ActivityId', None),
                            "category": event_record.get('category', None),
                            "TaskName": event_record.get("TaskName", None),
                            "delay": delay
                    })

            r = []
            for k, v in record.items():
                if isinstance(v, str) and " " in v:
                    v = f"\"{v}\""
                elif " " in str(v):
                    v = f"\"{str(v)}\""
                r.append(f"{k}={v}")

            if delay > args.delay:
                await FILEWRITER["EVENT"].write(" ".join(r) + "\n")
                print(" ".join(r))


    await partition_context.update_checkpoint(event)

async def main():
    """ Main async loop """
    checkpoint_store = LocalFileCheckpointStore(file_path="./checkpoint_store")
    checkpoint_store.DEBUG = args.verbose

    print("Creating connection ")
    #credential = DefaultAzureCredential()
    credential = ManagedIdentityCredential(client_id=CLIENT_ID)
    consumer = EventHubConsumerClient(
        fully_qualified_namespace=f"{conn[args.ns]['namespace']}.servicebus.windows.net",
        eventhub_name=args.eventhub_name,
        consumer_group=args.consumer_group,
        checkpoint_store=checkpoint_store,
        credential=credential,
        connection_verify=CONFIG.get('ca_certs', None),
        http_proxy=HTTP_PROXY,
        transport_type=TransportType.AmqpOverWebsocket,
    )

    async with consumer:
        await consumer.receive(on_event=on_event, starting_position="-1")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Event Hub Reader")
    parser.add_argument("-t", "--test", action="store_true", help="Use test environment")
    parser.add_argument("-M", "--max-events", type=int, default=-1, help="Maximum number of events to receive")
    parser.add_argument("-w", "--overwrite", action="store_true", help="Overwrite the log file")
    parser.add_argument("-c", "--consumer-group", type=str, default="sen-consumer-1", help="Consumer group to use")
    parser.add_argument("-i", "--client-id", type=str, default=None, help="ID of the managed identity to use. Has precedence over the config file.")
    parser.add_argument("-n", "--ns", type=str, help="Namespace to use")
    parser.add_argument("-e", "--eventhub-name", type=str, help="Event Hub name to use")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-p", "--proxy", action="store_true", help="Use HTTP proxy")
    parser.add_argument("-l", "--list", action="store_true", help="List available eventhubs/namespaces")
    parser.add_argument("-R", "--raw", action="store_true", help="Dump the raw event")
    parser.add_argument("-D", "--delay", type=int, default=0, help="Minimum delay to print the events (in seconds)")
    parser.add_argument("-o", "--out", type=str, choices=["raw", "all", "events"], help="Write to file the events")

    args = parser.parse_args()

    if args.verbose:
        print("Verbose logging enabled.")
        logging.basicConfig(level=logging.INFO, force=True)

    if args.debug:
        print("Debug logging enabled.")
        logging.basicConfig(level=logging.DEBUG, force=True)

    logger = logging.getLogger("azure.eventhub")


    logger.info("Starting Event Hub Reader")
    ENVIRON = "PROD" if not args.test else "TEST"
    MAX_EVENTS = args.max_events

    conn = CONFIG[ENVIRON]

    print(f"Using {ENVIRON} environment.")

    if not args.ns or args.ns not in conn:
        print("Available namespaces:")
        print("\n".join(conn.keys()))
        sys.exit()


    if args.list or not args.ns or args.eventhub_name not in conn[args.ns]["available_eh"]:
        if args.eventhub_name:
            print(f"Event Hub {args.eventhub_name} not available in namespace {args.ns}")
        print(f"Available Event Hubs: {', '.join(conn[args.ns]['available_eh'])}")
        sys.exit(-1)

    if args.proxy:
        HTTP_PROXY = CONFIG.get("http_proxy", {})

    MODE = "a" if not args.overwrite else "w"

    if not args.ns and not args.eventhub_name:
        parser.error('--name and --namespace are required.')

    if not args.client_id and "client_id" not in CONFIG:
        print("Client ID not set. Please set the 'client_id' in the config file or use --client-id/-i argument.")
        sys.exit(-1)

    CLIENT_ID = args.client_id if args.client_id else CONFIG.get("client_id")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        open_logs(args.out, MODE)
        # Run the main method.
        loop.run_until_complete(main())

    except KeyboardInterrupt:
        print(f"Total events received: {events_received}. Exiting... ")
        loop.stop()
