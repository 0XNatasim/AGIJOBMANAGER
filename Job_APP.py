import json
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

ALCHEMY_URL = os.getenv("ALCHEMY_URL", "").strip()
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

STATE_FILE = os.getenv("STATE_FILE", "state/state.json")
CONFIG_FILE = os.getenv("CONFIG_FILE", "radar_config.json")

START_BLOCK = int(os.getenv("START_BLOCK", "0"))
BLOCK_LOOKBACK_ON_FIRST_RUN = int(os.getenv("BLOCK_LOOKBACK_ON_FIRST_RUN", "500"))
BLOCK_CHUNK_SIZE = int(os.getenv("BLOCK_CHUNK_SIZE", "200"))  # safer for Alchemy

TOKEN_SYMBOL = os.getenv("TOKEN_SYMBOL", "AGIALPHA")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --------------------------------------------------
# Validate env
# --------------------------------------------------

missing = []
if not ALCHEMY_URL:
    missing.append("ALCHEMY_URL")
if not CONTRACT_ADDRESS:
    missing.append("CONTRACT_ADDRESS")
if not DISCORD_WEBHOOK_URL:
    missing.append("DISCORD_WEBHOOK_URL")

if missing:
    logging.error("Missing env vars: %s", ",".join(missing))
    sys.exit(1)

# --------------------------------------------------
# Helpers
# --------------------------------------------------


def ensure_dir(filepath):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)


def load_json(filepath, default):
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except:
        return default


def save_json(filepath, data):
    ensure_dir(filepath)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def wei_to_token(value: int):
    return f"{Decimal(value)/Decimal(10**18)} {TOKEN_SYMBOL}"


def ipfs_gateway(uri: str):
    if uri.startswith("ipfs://"):
        return "https://ipfs.io/ipfs/" + uri.replace("ipfs://", "")
    return uri


# --------------------------------------------------
# Load ABI
# --------------------------------------------------

with open("abi/AGIJobManager.json", "r") as f:
    ABI = json.load(f)

# --------------------------------------------------
# Web3
# --------------------------------------------------

web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

if not web3.is_connected():
    logging.error("Web3 connection failed")
    sys.exit(1)

contract = web3.eth.contract(
    address=Web3.to_checksum_address(CONTRACT_ADDRESS),
    abi=ABI
)

# --------------------------------------------------
# State
# --------------------------------------------------

state = load_json(STATE_FILE, {"last_checked_block": None})
last_checked = state.get("last_checked_block")

if last_checked is None:
    if START_BLOCK > 0:
        start_block = START_BLOCK
    else:
        start_block = max(latest_block - BLOCK_LOOKBACK_ON_FIRST_RUN, 0)
else:
    start_block = int(last_checked) + 1

end_block = latest_block

logging.info("START_BLOCK from env: %s", START_BLOCK)
logging.info("Scanning blocks %s -> %s", start_block, end_block)

# --------------------------------------------------
# Event list (important ones)
# --------------------------------------------------

EVENTS = [
    "JobCreated",
    "JobApplied",
    "JobCompletionRequested",
    "JobValidated",
    "JobDisputed",
    "DisputeResolvedWithCode",
    "JobCompleted",
    "JobExpired",
    "JobCancelled",
]

# --------------------------------------------------
# Discord
# --------------------------------------------------


def post(msg):

    payload = {"content": msg[:1900]}

    r = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if r.status_code >= 400:
        raise Exception(r.text)


# --------------------------------------------------
# Scanner
# --------------------------------------------------

all_events = []

chunk_start = start_block

while chunk_start <= end_block:

    chunk_end = min(chunk_start + BLOCK_CHUNK_SIZE - 1, end_block)

    logging.info("Scanning chunk %s -> %s", chunk_start, chunk_end)

    for event in EVENTS:

        try:

            event_obj = getattr(contract.events, event)

            logs = event_obj().get_logs(
                fromBlock=chunk_start,
                toBlock=chunk_end
            )

            if logs:
                logging.info("Found %s logs for %s", len(logs), event)

            for log in logs:

                all_events.append({
                    "event": event,
                    "args": dict(log["args"]),
                    "block": log["blockNumber"],
                    "tx": log["transactionHash"].hex(),
                    "index": log["logIndex"]
                })

        except Exception as e:

            logging.error(
                "Error reading %s in blocks %s-%s: %s",
                event,
                chunk_start,
                chunk_end,
                e
            )

    chunk_start = chunk_end + 1

# --------------------------------------------------
# Sort events
# --------------------------------------------------

all_events.sort(key=lambda x: (x["block"], x["index"]))

# --------------------------------------------------
# Post events
# --------------------------------------------------

posted = 0

for e in all_events:

    args = e["args"]
    name = e["event"]

    if name == "JobCreated":

        msg = f"""🚨 **JobCreated**

Job ID: `{args["jobId"]}`
Payout: `{wei_to_token(args["payout"])}`
Duration: `{args["duration"]}`
Spec: {args["jobSpecURI"]}
Gateway: {ipfs_gateway(args["jobSpecURI"])}

Details:
{args["details"]}

Block: `{e["block"]}`
Tx: `{e["tx"]}`
"""

    elif name == "JobApplied":

        msg = f"""🤖 **JobApplied**

Job ID: `{args["jobId"]}`
Agent: `{args["agent"]}`

Block: `{e["block"]}`
Tx: `{e["tx"]}`
"""

    elif name == "JobCompletionRequested":

        msg = f"""📦 **Completion Requested**

Job ID: `{args["jobId"]}`
Agent: `{args["agent"]}`

Completion:
{args["jobCompletionURI"]}

Block: `{e["block"]}`
Tx: `{e["tx"]}`
"""

    else:

        msg = f"""📡 **{name}**

Args:
{args}

Block: `{e["block"]}`
Tx: `{e["tx"]}`
"""

    try:

        post(msg)
        posted += 1

    except Exception as err:

        logging.error("Discord post failed: %s", err)

# --------------------------------------------------
# Save state
# --------------------------------------------------

save_json(STATE_FILE, {"last_checked_block": end_block})

logging.info("Posted %s messages", posted)
logging.info("Saved state block %s", end_block)

sys.exit(0)