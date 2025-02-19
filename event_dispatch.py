# %%
import blpapi as bp
from pprint import pprint,pformat
from datetime import datetime
import logging as pylogging
import threading
from typing import Dict, List, Any, Optional
import json
import os
import sys
import time
from datetime import timezone



# %%
pylogging.basicConfig(
    level=pylogging.DEBUG,
    format='[%(asctime)s] - %(process)s {%(pathname)s:%(lineno)d} - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        pylogging.FileHandler('logs/BloombergEventHandler.log',mode='w')
    ]
)
LOG = pylogging.getLogger(__name__)

# %%
class BloombergEventHandler(object):
    def __init__(self):
        super().__init__()
        self.lock = threading.Lock()
        self.msg_list = []
    
    def __call__(self, event: bp.Event, session: bp.Session) -> None:
        """Make the handler callable - this is the main event handling method"""
        if event.eventType() == bp.Event.SUBSCRIPTION_DATA:
            self.processSubscriptionDataEvent(event, session)
        else:
            LOG.info(f"Received event: {event.eventType()}")
            for msg in event:
                LOG.info(f"Received message: {msg}")

    def processSubscriptionDataEvent(self, event: bp.Event, session: bp.Session) -> None:
        for msg in event:
            try:
                msg_dict = msg.toPy()
                sec = msg.correlationId().value()
                msg_dict["TICKER"] = sec
                msg_dict['receive_time'] = msg.timeReceived(timezone.utc)

                LOG.info(f"Received message: {sec} - {msg}")
                with self.lock:
                    self.msg_list.append(msg_dict)
            except Exception as e:
                LOG.error(f"Error processing message: {str(e)}", exc_info=True)

# %%
with open("config/bpipe_config.local.json", 'r') as f:
    _config = json.load(f)
LOG.debug(f"Loaded Bloomberg configuration: {pformat(_config)}")

_sessionOptions = bp.SessionOptions()
for i, host in enumerate(_config['hosts']):
    _sessionOptions.setServerAddress(host['addr'], host['port'], i)

_sessionOptions.autoRestartOnDisconnection = True
_sessionOptions.numStartAttempts = 10

if 'appname' not in _config or not _config['appname']:
    raise ValueError("ApplicationName is required in the configuration")

authOpts = bp.AuthOptions.createWithApp(appName=_config['appname'])
_sessionOptions.setSessionIdentityOptions(authOpts)

_sessionOptions.setRecordSubscriptionDataReceiveTimes(True)

if "tlsInfo" in _config:
    tlsInfo = _config["tlsInfo"]
    pk12Blob = None
    pk7Blob = None
    with open(tlsInfo['pk12path'], 'rb') as pk12File:
        pk12Blob = pk12File.read()
    with open(tlsInfo['pk7path'], 'rb') as pk7File:
        pk7Blob = pk7File.read()

    _sessionOptions.setTlsOptions(bp.TlsOptions.createFromBlobs(pk12Blob, tlsInfo['password'], pk7Blob))


_sessionOptions.setMaxEventQueueSize(500000)

# %%


# %% [markdown]
# 

# %%
eventHandler = BloombergEventHandler()
eventDispatcher = bp.EventDispatcher(4)
eventDispatcher.start()

session = bp.Session(_sessionOptions,eventHandler)
session.start()

with open("securities-LN.txt", "r") as security_file:
    security_list = security_file.readlines()

with open("fields.txt", "r") as field_file:
    fields = field_file.readlines()

sublist = bp.SubscriptionList()
for ticker in security_list:
    sublist.add(ticker.strip(),fields,None,bp.CorrelationId(ticker.strip()))

session.subscribe(sublist)
input("Press any key to exit")

for i in range(0,sublist.size()):
    session.cancel(sublist.correlationIdAt(i))

session.stop()

# %%


# %%



