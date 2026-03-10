import json
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from web3 import Web3

# -----------------------------
# Load env
# -----------------------------
load_dotenv()

ALCHEMY_URL = os.getenv("ALCHEMY_URL", "").strip()
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
STATE_FILE = os.getenv("STATE_FILE", "state/state.json").strip()
CONFIG_FILE = os.getenv("CONFIG_FILE", "radar_config.json").strip()
START_BLOCK = int(os.getenv("START_BLOCK", "0").strip() or "0")
BLOCK_LOOKBACK_ON_FIRST_RUN = int(os.getenv("BLOCK_LOOKBACK_ON_FIRST_RUN", "500").strip() or "500")
BLOCK_CHUNK_SIZE = int(os.getenv("BLOCK_CHUNK_SIZE", "2000").strip() or "2000")
CHAIN_NAME = os.getenv("CHAIN_NAME", "Ethereum").strip()
TOKEN_SYMBOL = os.getenv("TOKEN_SYMBOL", "AGIALPHA").strip()

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Validate env
# -----------------------------
missing = []
if not ALCHEMY_URL:
    missing.append("ALCHEMY_URL")
if not CONTRACT_ADDRESS:
    missing.append("CONTRACT_ADDRESS")
if not DISCORD_WEBHOOK_URL:
    missing.append("DISCORD_WEBHOOK_URL")

if missing:
    logging.error("Missing required env vars: %s", ", ".join(missing))
    sys.exit(1)

# -----------------------------
# Event ABI (events only)
# -----------------------------
EVENT_ABI: List[Dict[str, Any]] = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "oldToken", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "newToken", "type": "address"},
        ],
        "name": "AGITokenAddressUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "nftAddress", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "payoutPercentage", "type": "uint256"},
        ],
        "name": "AGITypeUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "to", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "amount", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "remainingWithdrawable", "type": "uint256"},
        ],
        "name": "AGIWithdrawn",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "agent", "type": "address"},
            {"indexed": True, "internalType": "bool", "name": "status", "type": "bool"},
        ],
        "name": "AgentBlacklisted",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldMin", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newMin", "type": "uint256"},
        ],
        "name": "AgentBondMinUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldBps", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "oldMin", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "oldMax", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "newBps", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "newMin", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "newMax", "type": "uint256"},
        ],
        "name": "AgentBondParamsUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "owner", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "approved", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
        ],
        "name": "Approval",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "owner", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "operator", "type": "address"},
            {"indexed": False, "internalType": "bool", "name": "approved", "type": "bool"},
        ],
        "name": "ApprovalForAll",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldPeriod", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newPeriod", "type": "uint256"},
        ],
        "name": "ChallengePeriodAfterApprovalUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldPeriod", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newPeriod", "type": "uint256"},
        ],
        "name": "CompletionReviewPeriodUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "resolver", "type": "address"},
            {"indexed": True, "internalType": "uint8", "name": "resolutionCode", "type": "uint8"},
            {"indexed": False, "internalType": "string", "name": "reason", "type": "string"},
        ],
        "name": "DisputeResolvedWithCode",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldPeriod", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newPeriod", "type": "uint256"},
        ],
        "name": "DisputeReviewPeriodUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint8", "name": "hook", "type": "uint8"},
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "target", "type": "address"},
            {"indexed": False, "internalType": "bool", "name": "success", "type": "bool"},
        ],
        "name": "EnsHookAttempted",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "oldEnsJobPages", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "newEnsJobPages", "type": "address"},
        ],
        "name": "EnsJobPagesUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "address", "name": "newEnsRegistry", "type": "address"},
        ],
        "name": "EnsRegistryUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "locker", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "atTimestamp", "type": "uint256"},
        ],
        "name": "IdentityConfigurationLocked",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "agent", "type": "address"},
        ],
        "name": "JobApplied",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
        ],
        "name": "JobCancelled",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "agent", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "reputationPoints", "type": "uint256"},
        ],
        "name": "JobCompleted",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "agent", "type": "address"},
            {"indexed": False, "internalType": "string", "name": "jobCompletionURI", "type": "string"},
        ],
        "name": "JobCompletionRequested",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": False, "internalType": "string", "name": "jobSpecURI", "type": "string"},
            {"indexed": True, "internalType": "uint256", "name": "payout", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "duration", "type": "uint256"},
            {"indexed": False, "internalType": "string", "name": "details", "type": "string"},
        ],
        "name": "JobCreated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "validator", "type": "address"},
        ],
        "name": "JobDisapproved",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "disputant", "type": "address"},
        ],
        "name": "JobDisputed",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "employer", "type": "address"},
            {"indexed": False, "internalType": "address", "name": "agent", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "payout", "type": "uint256"},
        ],
        "name": "JobExpired",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "validator", "type": "address"},
        ],
        "name": "JobValidated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "bytes32", "name": "validatorMerkleRoot", "type": "bytes32"},
            {"indexed": False, "internalType": "bytes32", "name": "agentMerkleRoot", "type": "bytes32"},
        ],
        "name": "MerkleRootsUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
            {"indexed": True, "internalType": "address", "name": "employer", "type": "address"},
            {"indexed": False, "internalType": "string", "name": "tokenURI", "type": "string"},
        ],
        "name": "NFTIssued",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "address", "name": "newNameWrapper", "type": "address"},
        ],
        "name": "NameWrapperUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "previousOwner", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "newOwner", "type": "address"},
        ],
        "name": "OwnershipTransferred",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "address", "name": "account", "type": "address"},
        ],
        "name": "Paused",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "jobId", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "amount", "type": "uint256"},
        ],
        "name": "PlatformRevenueAccrued",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "address", "name": "user", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "newReputation", "type": "uint256"},
        ],
        "name": "ReputationUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldApprovals", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newApprovals", "type": "uint256"},
        ],
        "name": "RequiredValidatorApprovalsUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldDisapprovals", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newDisapprovals", "type": "uint256"},
        ],
        "name": "RequiredValidatorDisapprovalsUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "bytes32", "name": "clubRootNode", "type": "bytes32"},
            {"indexed": True, "internalType": "bytes32", "name": "agentRootNode", "type": "bytes32"},
            {"indexed": True, "internalType": "bytes32", "name": "alphaClubRootNode", "type": "bytes32"},
            {"indexed": False, "internalType": "bytes32", "name": "alphaAgentRootNode", "type": "bytes32"},
        ],
        "name": "RootNodesUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "setter", "type": "address"},
            {"indexed": True, "internalType": "bool", "name": "paused", "type": "bool"},
        ],
        "name": "SettlementPauseSet",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "from", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "to", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "address", "name": "account", "type": "address"},
        ],
        "name": "Unpaused",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldPercentage", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newPercentage", "type": "uint256"},
        ],
        "name": "ValidationRewardPercentageUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "validator", "type": "address"},
            {"indexed": True, "internalType": "bool", "name": "status", "type": "bool"},
        ],
        "name": "ValidatorBlacklisted",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "bps", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "min", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "max", "type": "uint256"},
        ],
        "name": "ValidatorBondParamsUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldBps", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newBps", "type": "uint256"},
        ],
        "name": "ValidatorSlashBpsUpdated",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "uint256", "name": "oldQuorum", "type": "uint256"},
            {"indexed": True, "internalType": "uint256", "name": "newQuorum", "type": "uint256"},
        ],
        "name": "VoteQuorumUpdated",
        "type": "event",
    },
]

# -----------------------------
# Default config
# -----------------------------
DEFAULT_CONFIG: Dict[str, Any] = {
    "mute_events": [
        "Approval",
        "ApprovalForAll",
        "Transfer"
    ],
    "mention_everyone_for": [
        "JobCreated",
        "JobCompletionRequested",
        "JobDisputed",
        "DisputeResolvedWithCode",
        "JobCompleted",
        "JobExpired",
        "JobCancelled"
    ],
    "max_events_per_run": 200,
    "discord_username": "AGIJobManager Radar",
    "discord_avatar_url": "",
    "include_tx_hash": True,
    "include_block_number": True
}

# -----------------------------
# Helpers
# -----------------------------
def ensure_dir_for_file(filepath: str) -> None:
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

def load_json_file(filepath: str, default: Any) -> Any:
    path = Path(filepath)
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logging.warning("Could not read %s: %s", filepath, exc)
        return default

def save_json_file(filepath: str, data: Any) -> None:
    ensure_dir_for_file(filepath)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def short_addr(addr: Optional[str]) -> str:
    if not addr:
        return "N/A"
    addr = str(addr)
    if len(addr) < 12:
        return addr
    return f"{addr[:6]}...{addr[-4:]}"

def normalize_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, (list, tuple)):
        return [normalize_value(v) for v in value]
    if hasattr(value, "hex") and not isinstance(value, str):
        try:
            return value.hex()
        except Exception:
            pass
    return value

def wei_to_token_str(value: int) -> str:
    try:
        dec = Decimal(value) / Decimal(10**18)
        return f"{dec.normalize()} {TOKEN_SYMBOL}"
    except Exception:
        return f"{value} raw"

def build_ipfs_gateway_url(uri: str) -> str:
    if uri.startswith("ipfs://"):
        return "https://ipfs.io/ipfs/" + uri.replace("ipfs://", "", 1)
    return uri

def load_config() -> Dict[str, Any]:
    cfg = load_json_file(CONFIG_FILE, DEFAULT_CONFIG)
    if not isinstance(cfg, dict):
        cfg = DEFAULT_CONFIG.copy()

    merged = DEFAULT_CONFIG.copy()
    merged.update(cfg)
    merged["mute_events"] = set(merged.get("mute_events", []))
    merged["mention_everyone_for"] = set(merged.get("mention_everyone_for", []))
    return merged

def load_state() -> Dict[str, Any]:
    return load_json_file(STATE_FILE, {"last_checked_block": None})

def save_state(last_checked_block: int) -> None:
    save_json_file(STATE_FILE, {"last_checked_block": last_checked_block})

def post_discord_message(webhook_url: str, content: str, username: str = "", avatar_url: str = "") -> None:
    payload: Dict[str, Any] = {"content": content[:1900]}
    if username:
        payload["username"] = username
    if avatar_url:
        payload["avatar_url"] = avatar_url

    resp = requests.post(webhook_url, json=payload, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Discord webhook failed: {resp.status_code} {resp.text}")

def get_contract(web3: Web3):
    checksum_address = Web3.to_checksum_address(CONTRACT_ADDRESS)
    return web3.eth.contract(address=checksum_address, abi=EVENT_ABI)

def all_event_names() -> List[str]:
    return [item["name"] for item in EVENT_ABI if item.get("type") == "event"]

def format_event_message(
    event_name: str,
    args: Dict[str, Any],
    tx_hash: str,
    block_number: int,
    config: Dict[str, Any]
) -> str:
    prefix = "@everyone " if event_name in config["mention_everyone_for"] else ""

    if event_name == "JobCreated":
        payout_text = wei_to_token_str(int(args["payout"]))
        spec_uri = str(args["jobSpecURI"])
        spec_link = build_ipfs_gateway_url(spec_uri)
        lines = [
            f"{prefix}🚨 **JobCreated**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Payout:** `{payout_text}`",
            f"**Duration:** `{args['duration']}` sec",
            f"**Job Spec URI:** {spec_uri}",
            f"**Gateway:** {spec_link}",
            f"**Details:** {args['details']}",
        ]
    elif event_name == "JobApplied":
        lines = [
            f"{prefix}🤖 **JobApplied**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Agent:** `{args['agent']}`",
        ]
    elif event_name == "JobCompletionRequested":
        completion_uri = str(args["jobCompletionURI"])
        completion_link = build_ipfs_gateway_url(completion_uri)
        lines = [
            f"{prefix}📦 **JobCompletionRequested**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Agent:** `{args['agent']}`",
            f"**Completion URI:** {completion_uri}",
            f"**Gateway:** {completion_link}",
        ]
    elif event_name == "JobValidated":
        lines = [
            f"{prefix}✅ **JobValidated**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Validator:** `{args['validator']}`",
        ]
    elif event_name == "JobDisapproved":
        lines = [
            f"{prefix}⛔ **JobDisapproved**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Validator:** `{args['validator']}`",
        ]
    elif event_name == "JobDisputed":
        lines = [
            f"{prefix}⚠️ **JobDisputed**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Disputant:** `{args['disputant']}`",
        ]
    elif event_name == "DisputeResolvedWithCode":
        lines = [
            f"{prefix}🧑‍⚖️ **DisputeResolvedWithCode**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Resolver:** `{args['resolver']}`",
            f"**Resolution Code:** `{args['resolutionCode']}`",
            f"**Reason:** {args['reason']}",
        ]
    elif event_name == "JobCompleted":
        lines = [
            f"{prefix}🏁 **JobCompleted**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Agent:** `{args['agent']}`",
            f"**Reputation Points:** `{args['reputationPoints']}`",
        ]
    elif event_name == "JobExpired":
        lines = [
            f"{prefix}⌛ **JobExpired**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Employer:** `{args['employer']}`",
            f"**Agent:** `{args['agent']}`",
            f"**Payout:** `{wei_to_token_str(int(args['payout']))}`",
        ]
    elif event_name == "JobCancelled":
        lines = [
            f"{prefix}🛑 **JobCancelled**",
            f"**Job ID:** `{args['jobId']}`",
        ]
    elif event_name == "PlatformRevenueAccrued":
        lines = [
            f"{prefix}💰 **PlatformRevenueAccrued**",
            f"**Job ID:** `{args['jobId']}`",
            f"**Amount:** `{wei_to_token_str(int(args['amount']))}`",
        ]
    elif event_name == "ReputationUpdated":
        lines = [
            f"{prefix}📈 **ReputationUpdated**",
            f"**User:** `{args['user']}`",
            f"**New Reputation:** `{args['newReputation']}`",
        ]
    elif event_name == "SettlementPauseSet":
        lines = [
            f"{prefix}🧯 **SettlementPauseSet**",
            f"**Setter:** `{args['setter']}`",
            f"**Paused:** `{args['paused']}`",
        ]
    elif event_name == "Paused":
        lines = [
            f"{prefix}⏸️ **Paused**",
            f"**Account:** `{args['account']}`",
        ]
    elif event_name == "Unpaused":
        lines = [
            f"{prefix}▶️ **Unpaused**",
            f"**Account:** `{args['account']}`",
        ]
    elif event_name == "ValidatorBlacklisted":
        lines = [
            f"{prefix}🚫 **ValidatorBlacklisted**",
            f"**Validator:** `{args['validator']}`",
            f"**Status:** `{args['status']}`",
        ]
    elif event_name == "AgentBlacklisted":
        lines = [
            f"{prefix}🚫 **AgentBlacklisted**",
            f"**Agent:** `{args['agent']}`",
            f"**Status:** `{args['status']}`",
        ]
    elif event_name == "NFTIssued":
        lines = [
            f"{prefix}🖼️ **NFTIssued**",
            f"**Token ID:** `{args['tokenId']}`",
            f"**Employer:** `{args['employer']}`",
            f"**Token URI:** {args['tokenURI']}",
        ]
    else:
        lines = [f"{prefix}📡 **{event_name}**"]
        for key, value in args.items():
            lines.append(f"**{key}:** `{value}`")

    if config.get("include_block_number", True):
        lines.append(f"**Block:** `{block_number}`")
    if config.get("include_tx_hash", True):
        lines.append(f"**Tx:** `{tx_hash}`")

    return "\n".join(lines)

def decode_and_collect_logs(
    contract,
    event_name: str,
    fromBlock: int,
    toBlock: int
) -> List[Dict[str, Any]]:
    event_obj = getattr(contract.events, event_name)
    logs = event_obj().get_logs(fromBlock=fromBlock, toBlock=toBlock)

    collected: List[Dict[str, Any]] = []
    for ev in logs:
        args = {k: normalize_value(v) for k, v in dict(ev["args"]).items()}
        collected.append(
            {
                "event_name": event_name,
                "args": args,
                "block_number": int(ev["blockNumber"]),
                "tx_hash": ev["transactionHash"].hex(),
                "log_index": int(ev["logIndex"]),
            }
        )
    return collected

def main() -> int:
    config = load_config()
    state = load_state()

    web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
    if not web3.is_connected():
        logging.error("Web3 connection failed.")
        return 1

    contract = get_contract(web3)
    latest_block = int(web3.eth.block_number)
    logging.info("Connected. Latest block: %s", latest_block)

    last_checked_block = state.get("last_checked_block")
    if last_checked_block is None:
        if START_BLOCK > 0:
            fromBlock = START_BLOCK
        else:
            fromBlock = max(latest_block - BLOCK_LOOKBACK_ON_FIRST_RUN, 0)
    else:
        fromBlock = int(last_checked_block) + 1

    toBlock = latest_block

    if fromBlock > toBlock:
        logging.info("No new blocks to scan. fromBlock=%s toBlock=%s", fromBlock, toBlock)
        return 0

    logging.info("Scanning blocks %s -> %s", fromBlock, toBlock)

    all_logs: List[Dict[str, Any]] = []
    chunk_start = fromBlock
    event_names = all_event_names()

    while chunk_start <= toBlock:
        chunk_end = min(chunk_start + BLOCK_CHUNK_SIZE - 1, toBlock)
        logging.info("Scanning chunk %s -> %s", chunk_start, chunk_end)

        for event_name in event_names:
            if event_name in config["mute_events"]:
                continue

            try:
                logs = decode_and_collect_logs(contract, event_name, chunk_start, chunk_end)
                if logs:
                    logging.info("Found %s logs for %s", len(logs), event_name)
                    all_logs.extend(logs)
            except Exception as exc:
                logging.error("Error reading %s in blocks %s-%s: %s", event_name, chunk_start, chunk_end, exc)

        chunk_start = chunk_end + 1

    all_logs.sort(key=lambda x: (x["block_number"], x["log_index"]))

    max_events_per_run = int(config.get("max_events_per_run", 200))
    if len(all_logs) > max_events_per_run:
        logging.warning(
            "Found %s events; truncating to last %s for Discord posting.",
            len(all_logs),
            max_events_per_run,
        )
        all_logs = all_logs[-max_events_per_run:]

    if not all_logs:
        logging.info("No matching events found.")
        save_state(toBlock)
        return 0

    posted = 0
    for item in all_logs:
        try:
            message = format_event_message(
                event_name=item["event_name"],
                args=item["args"],
                tx_hash=item["tx_hash"],
                block_number=item["block_number"],
                config=config,
            )
            post_discord_message(
                DISCORD_WEBHOOK_URL,
                message,
                username=config.get("discord_username", ""),
                avatar_url=config.get("discord_avatar_url", ""),
            )
            posted += 1
        except Exception as exc:
            logging.error("Failed posting Discord message for %s: %s", item["event_name"], exc)

    logging.info("Posted %s Discord messages.", posted)
    save_state(toBlock)
    logging.info("Saved state at block %s", toBlock)
    return 0
    logging.info(f"START_BLOCK from env: {START_BLOCK}")

if __name__ == "__main__":
    sys.exit(main())