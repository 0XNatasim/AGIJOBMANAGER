import json
import os
import sys
import logging
from web3 import Web3
from dotenv import load_dotenv
import requests

load_dotenv()

ALCHEMY_URL = os.getenv("ALCHEMY_URL")
CONTRACT_ADDRESS = Web3.to_checksum_address(os.getenv("CONTRACT_ADDRESS", "").strip())
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL").strip()

START_BLOCK = int(os.getenv("START_BLOCK", "0"))
BLOCK_CHUNK_SIZE = int(os.getenv("BLOCK_CHUNK_SIZE", "500"))

STATE_FILE = "state/state.json"

logging.basicConfig(level=logging.INFO)

# -----------------------------
# Load ABI
# -----------------------------

with open("abi/AGIJobManager.json") as f:
    ABI = json.load(f)

# -----------------------------
# Web3
# -----------------------------

web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

if not web3.is_connected():
    print("RPC connection failed")
    sys.exit(1)

contract = web3.eth.contract(address=CONTRACT_ADDRESS, abi=ABI)

latest_block = web3.eth.block_number

logging.info(f"Latest block {latest_block}")

# -----------------------------
# State
# -----------------------------

os.makedirs("state", exist_ok=True)

if os.path.exists(STATE_FILE):
    with open(STATE_FILE) as f:
        state = json.load(f)
        start_block = state["last_block"] + 1
else:
    start_block = START_BLOCK

end_block = latest_block

logging.info(f"Scanning {start_block} → {end_block}")

# -----------------------------
# Event topic map
# -----------------------------

topic_map = {}

for item in ABI:
    if item["type"] == "event":
        signature = f"{item['name']}({','.join(i['type'] for i in item['inputs'])})"
        topic = web3.keccak(text=signature).hex()
        topic_map[topic] = item["name"]

# -----------------------------
# Scan logs
# -----------------------------

all_events = []

block = start_block

while block <= end_block:

    chunk_end = min(block + BLOCK_CHUNK_SIZE, end_block)

    logging.info(f"Chunk {block} → {chunk_end}")

    logs = web3.eth.get_logs({
        "fromBlock": block,
        "toBlock": chunk_end,
        "address": CONTRACT_ADDRESS
    })

    for log in logs:

        topic0 = log["topics"][0].hex()

        if topic0 not in topic_map:
            continue

        event_name = topic_map[topic0]

        decoded = contract.events[event_name]().process_log(log)

        all_events.append({
            "event": event_name,
            "args": dict(decoded["args"]),
            "block": log["blockNumber"],
            "tx": log["transactionHash"].hex()
        })

    block = chunk_end + 1

# -----------------------------
# Send to Discord
# -----------------------------

for e in all_events:

    msg = f"""
🚨 **{e['event']}**

Block: {e['block']}
Tx: {e['tx']}

Args:
{json.dumps(e['args'], indent=2)}
"""

    requests.post(DISCORD_WEBHOOK_URL, json={"content": msg[:1800]})

# -----------------------------
# Save state
# -----------------------------

with open(STATE_FILE, "w") as f:
    json.dump({"last_block": end_block}, f)

logging.info(f"Processed {len(all_events)} events")