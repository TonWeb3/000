"""
One-time on-chain allowance setup for live trading on Polymarket (Polygon).

The wallet (your private key / seed phrase) must approve the Polymarket exchange
contracts to move its USDC (ERC-20) and outcome tokens (ERC-1155 CTF) before the
CLOB will accept its orders. This module checks the current allowances and only
sends the missing approvals.

Requires the trading wallet to hold a little POL/MATIC for gas.
"""

from typing import Dict, Any, List
from web3 import AsyncWeb3
from .config import settings
from .chainlink import chainlink_fetcher  # reuse its ordered Polygon RPC list
from .wallet import resolve_private_key, derive_address

MAX_UINT256 = (2 ** 256) - 1
ALLOWANCE_THRESHOLD = 2 ** 255  # an existing allowance above this counts as "set"

ERC20_ABI = [
    {"constant": False, "inputs": [{"name": "spender", "type": "address"}, {"name": "value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"constant": True, "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
]

ERC1155_ABI = [
    {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}], "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}], "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "view", "type": "function"},
]


def _raw_tx(signed) -> bytes:
    # web3 v6.5+ uses raw_transaction; older uses rawTransaction
    return getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")


def _is_gas_error(msg: str) -> bool:
    m = (msg or "").lower()
    return ("insufficient funds" in m or "insufficient balance" in m
            or "gas required exceeds" in m or "intrinsic gas" in m)


async def _run_allowances(w3: AsyncWeb3) -> Dict[str, Any]:
    """Send only the MISSING approvals on `w3`. Checks the wallet's gas balance up
    front and returns a clear 'no_gas' result (rather than failing obscurely) if it
    can't pay for the approval transactions."""
    acct = w3.eth.account.from_key(resolve_private_key(settings.PRIVATE_KEY))
    owner = acct.address

    usdc = w3.eth.contract(address=AsyncWeb3.to_checksum_address(settings.USDC_ADDRESS), abi=ERC20_ABI)
    ctf = w3.eth.contract(address=AsyncWeb3.to_checksum_address(settings.CTF_ADDRESS), abi=ERC1155_ABI)

    spenders = [
        ("exchange", settings.CLOB_EXCHANGE_ADDRESS),
        ("neg_risk_exchange", settings.CLOB_NEG_RISK_EXCHANGE_ADDRESS),
        ("neg_risk_adapter", settings.CLOB_NEG_RISK_ADAPTER_ADDRESS),
    ]

    # 1) Which approvals are still missing? (read-only)
    missing: List = []  # (kind, spender_name, spender_addr)
    for name, raw_addr in spenders:
        if not raw_addr:
            continue
        spender = AsyncWeb3.to_checksum_address(raw_addr)
        if (await usdc.functions.allowance(owner, spender).call()) < ALLOWANCE_THRESHOLD:
            missing.append(("usdc", name, spender))
        if not (await ctf.functions.isApprovedForAll(owner, spender).call()):
            missing.append(("ctf", name, spender))

    if not missing:
        return {"ok": True, "owner": owner, "actions": [], "already_set": True}

    # 2) Gas check — make sure the wallet can pay before sending anything.
    try:
        gas_price = int(await w3.eth.gas_price)
    except Exception:
        gas_price = w3.to_wei(50, "gwei")
    gas_price = int(gas_price * 1.25)
    gas_per_tx = 120000
    needed_wei = len(missing) * gas_per_tx * gas_price
    balance_wei = await w3.eth.get_balance(owner)
    if balance_wei < needed_wei:
        return {
            "ok": False,
            "error": "no_gas",
            "owner": owner,
            "message": (f"Not enough gas: wallet {owner} holds {balance_wei / 1e18:.4f} POL but needs "
                        f"~{needed_wei / 1e18:.4f} POL to send {len(missing)} approval(s). "
                        f"Fund it with a little POL (MATIC) on Polygon and try again.")
        }

    # 3) Send the missing approvals. "pending" nonce so a retry can't reuse one.
    nonce = await w3.eth.get_transaction_count(owner, "pending")
    actions: List[Dict[str, Any]] = []

    async def _send(func) -> Dict[str, Any]:
        nonlocal nonce
        tx = await func.build_transaction({
            "from": owner, "nonce": nonce, "chainId": 137,
            "gas": gas_per_tx, "gasPrice": gas_price,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(_raw_tx(signed))
        nonce += 1
        receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        return {"tx": tx_hash.hex(), "status": int(receipt.get("status", 0))}

    for kind, name, spender in missing:
        if kind == "usdc":
            res = await _send(usdc.functions.approve(spender, MAX_UINT256))
            actions.append({"type": "usdc_approve", "spender": name, **res})
        else:
            res = await _send(ctf.functions.setApprovalForAll(spender, True))
            actions.append({"type": "ctf_approve", "spender": name, **res})

    return {"ok": True, "owner": owner, "actions": actions, "already_set": False}


async def ensure_allowances() -> Dict[str, Any]:
    if not settings.PRIVATE_KEY:
        return {"ok": False, "error": "missing_private_key"}

    rpcs = chainlink_fetcher.get_ordered_rpcs()
    if not rpcs:
        return {"ok": False, "error": "no_rpc_available"}

    # Find an RPC that actually answers a read (Alchemy first when configured). Only
    # CONNECTION failures fall through to the next RPC.
    w3 = None
    conn_err = None
    for rpc in rpcs:
        try:
            cand = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc, request_kwargs={"timeout": 20.0}))
            await cand.eth.get_block_number()  # a real read confirms it works
            w3 = cand
            break
        except Exception as e:
            conn_err = f"{rpc}: {type(e).__name__}: {e}"
            continue
    if w3 is None:
        return {"ok": False, "error": f"no_working_rpc ({conn_err})"}

    # Run the allowance logic. A gas/balance failure is reported clearly (not retried
    # on other RPCs — it's the same wallet, same result).
    try:
        return await _run_allowances(w3)
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        if _is_gas_error(msg):
            owner = derive_address(settings.PRIVATE_KEY)
            return {"ok": False, "error": "no_gas", "owner": owner,
                    "message": (f"Not enough gas: wallet {owner} has too little POL (MATIC) on Polygon "
                                f"to send the approval transactions. Fund it and try again.")}
        return {"ok": False, "error": msg}
