#!/usr/bin/python3

import argparse
import aria2p
import copy
import json
import logging
import os
import random
import re
import requests
import retry
import subprocess
import sys
import tenacity
import time
import urllib3

from typing import Any, Dict, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--miner-id",
        help="Storage Provider miner ID (ie. f0123456)",
        type=str,
        default=os.environ.get("MINER_ID"),
        required=not os.environ.get("MINER_ID"),
    )
    parser.add_argument(
        "--fil-spid-file-path",
        help="Full file path of the `fil-spid.bash` authorization script provided by Spade.",
        type=str,
        default=os.environ.get("FIL_SPID_FILE_PATH"),
        required=not os.environ.get("FIL_SPID_FILE_PATH"),
    )
    parser.add_argument(
        "--aria2c-url",
        help="URL of the aria2c process running in daemon mode (eg. 'http://localhost:6800'). Launch the daemon with `aria2c --enable-rpc`.",
        nargs="?",
        const="http://localhost:6800",
        type=str,
        default=os.environ.get("ARIA2C_URL", "http://localhost:6800"),
        required=False,
    )
    parser.add_argument(
        "--aria2c-connections-per-server",
        help="Configures the '-x' flag in aria2c. (eg. aria2c -x8 <uri>). Default: 10",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_CONNECTIONS_PER_SERVER", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-max-concurrent-downloads",
        help="Configures the '-j' flag in aria2c. (eg. aria2c -j10). Default: 10",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_MAX_CONCURRENT_DOWNLOADS", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-download-path",
        help="The directory into which aria2c should be configured to download files before being imported to Boost. Default: /mnt/data",
        nargs="?",
        const="/mnt/data",
        type=str,
        default=os.environ.get("ARIA2C_DOWNLOAD_PATH", "/mnt/data"),
        required=False,
    )
    parser.add_argument(
        "--boost-api-info",
        help="The Boost api string normally set as the BOOST_API_INFO environment variable (eg. 'eyJhbG...aCG:/ip4/10.0.0.10/tcp/1234/http')",
        type=str,
        default=os.environ.get("BOOST_API_INFO"),
        required=not os.environ.get("BOOST_API_INFO"),
    )
    parser.add_argument(
        "--boost-graphql-port",
        help="The port number where Boost's graphql is hosted (eg. 8080)",
        nargs="?",
        const=8080,
        type=int,
        default=os.environ.get("BOOST_GRAPHQL_PORT", 8080),
        required=not os.environ.get("BOOST_GRAPHQL_PORT"),
    )
    parser.add_argument(
        "--boost-delete-after-import",
        help="Whether or not to instruct Boost to delete the downloaded data after it is imported. Equivalent of 'boostd --delete-after-import'. Default: True",
        nargs="?",
        const=True,
        type=bool,
        default=os.environ.get("BOOST_DELETE_AFTER_IMPORT", True),
        required=False,
    )
    parser.add_argument(
        "--spade-deal-timeout",
        help="The time to wait between a deal appearing in Boost and appearing in Spade before considering the deal failed (or not a Spade deal) and ignoring it. Stated in seconds, with no units. Default: 3600",
        nargs="?",
        const=True,
        type=int,
        default=os.environ.get("SPADE_DEAL_TIMEOUT", 3600),
        required=False,
    )
    parser.add_argument(
        "--maximum-boost-deals-in-flight",
        help="The maximum number of deals in 'Awaiting Offline Data Import' state in Boost UI. Default: 10",
        nargs="?",
        const=10,
        type=int,
        default=os.environ.get("MAXIMUM_BOOST_DEALS_IN_FLIGHT", 10),
        required=False,
    )
    parser.add_argument(
        "--maximum-snap-sectors-in-flight",
        help="The maximum number of UpdateReplica and ProveReplicaUpdate tasks being processed by the miner. If above this limit, new Spade deals will not be requested until count is below this number again. Default: 0 (meaning no limit)",
        nargs="?",
        const=0,
        type=int,
        default=os.environ.get("MAXIMUM_SNAP_SECTORS_IN_FLIGHT", 0),
        required=False,
    )
    parser.add_argument(
        "--maximum-sectors-in-adding-piece-state",
        help="The maximum number of 'Adding to Sector' tasks being processed by the miner. If above this limit, new Spade deals will not be requested until count is below this number again. Default: 0 (meaning no limit)",
        nargs="?",
        const=0,
        type=int,
        default=os.environ.get("MAXIMUM_SECTORS_IN_ADDING_PIECE_STATE", 0),
        required=False,
    )
    parser.add_argument(
        "--complete-existing-deals-only",
        help="Setting this flag will prevent new deals from being requested but allow existing deals to complete. Useful for cleaning out the deals pipeline to debug, or otherwise. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("COMPLETE_EXISTING_DEALS_ONLY", False),
        required=False,
    )
    parser.add_argument(
        "--verbose",
        help="If enabled, logging will be greatly increased. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("VERBOSE", False),
        required=False,
    )
    parser.add_argument(
        "--debug",
        help="If enabled, logging will be thorough, enabling debugging of deep issues. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("DEBUG", False),
        required=False,
    )
    return parser.parse_args()


def get_logger(name: str, *, options: dict) -> logging.Logger:
    logger = logging.getLogger(name)
    stdout_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(f"[%(levelname)s] {name}: %(message)s")
    stdout_handler.setFormatter(formatter)
    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)
    return logger


options = parse_args()
json_rpc_id = 1
aria2 = None
log = get_logger("Ace", options=options)
log_retry = get_logger("RETRYING", options=options)
log_aria2 = get_logger("Aria2", options=options)
log_request = get_logger("Request", options=options)

ELIGIBLE_PIECES_ENDPOINT = "https://api.spade.storage/sp/eligible_pieces"
INVOKE_ENDPOINT = "https://api.spade.storage/sp/invoke"
PENDING_PROPOSALS_ENDPOINT = "https://api.spade.storage/sp/pending_proposals"


def eligible_pieces(*, options: dict) -> dict:
    log.debug("Querying for eligible pieces")
    response = request_handler(
        url=ELIGIBLE_PIECES_ENDPOINT,
        method="get",
        parameters={"timeout": 30, "allow_redirects": True},
        log_name="eligible_pieces",
        miner_auth_header={"add": True},
    )
    if response == None:
        return None
    return response


def invoke_deal(*, piece_cid: str, tenant_policy_cid: str, options: dict) -> dict:
    log.debug("Invoking a new deal")
    response = request_handler(
        url=INVOKE_ENDPOINT,
        method="post",
        parameters={"timeout": 30, "allow_redirects": True},
        log_name="invoke_deal",
        miner_auth_header={
            "add": True,
            "stdin": f"call=reserve_piece&piece_cid={piece_cid}&tenant_policy={tenant_policy_cid}",
        },
    )

    if response == None:
        return None

    response["piece_cid"] = re.findall(r"baga[a-zA-Z0-9]+", response["info_lines"][0])[0]
    log.debug(f"New deal requested: {response}")
    return response


def pending_proposals(*, options: dict) -> list:
    log.debug("Querying pending proposals")
    response = request_handler(
        url=PENDING_PROPOSALS_ENDPOINT,
        method="get",
        parameters={"timeout": 30, "allow_redirects": True},
        log_name="pending_proposals",
        miner_auth_header={"add": True},
    )
    if response == None:
        return None
    return response["response"]


def boost_import(*, options: dict, deal_uuid: str, file_path: str) -> bool:
    global json_rpc_id

    log.info(f"Importing deal to boost: UUID: {deal_uuid}, Path: {file_path}")
    boost_bearer_token = options.boost_api_info.split(":")[0]
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    boost_port = options.boost_api_info.split(":")[1].split("/")[4]
    headers = {"Authorization": f"Bearer {boost_bearer_token}", "content-type": "application/json"}
    payload = {
        "method": "Filecoin.BoostOfflineDealWithData",
        "params": [
            deal_uuid,
            file_path,
            options.boost_delete_after_import,
        ],
        "jsonrpc": "2.0",
        "id": json_rpc_id,
    }

    response = request_handler(
        url=f"http://{boost_url}:{boost_port}/rpc/v0",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload), "headers": headers},
        log_name="boost_import",
        miner_auth_header={"add": False},
    )
    json_rpc_id += 1
    if response == None:
        return None
    elif "error" in response:
        log.warning(f"Import to boost failed with error: {response['error']}")
        return None
    elif "result" in response and response["result"]["Accepted"]:
        log.info(f"Deal imported to boost: {deal_uuid}")
        return True
    else:
        log.error(f"Deal failed to be imported to boost: {response}")
        return None


def get_boost_deals(*, options: dict) -> Any:
    # ToDo: Filter out deals not managed by Spade by comparing against list of pending_proposals or filtering by the client address. Currently all deals are expected to be Spade deals.
    log.debug("Querying deals from boost")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query {deals(limit: 1000, filter: {IsOffline: true, Checkpoint: Accepted}) {deals {ID CreatedAt Checkpoint IsOffline Err PieceCid Message}totalCount}}"
    }

    response = request_handler(
        url=f"http://{boost_url}:{options.boost_graphql_port}/graphql/query",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload)},
        log_name="get_boost_deals",
        miner_auth_header={"add": False},
    )
    if response == None:
        return None
    else:
        deals = response["data"]["deals"]["deals"]
        return [d for d in deals if d["Message"] == "Awaiting Offline Data Import"]


def get_snap_sectors_count(*, options: dict) -> int:
    log.debug("Querying sector statuses from boost")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query {sealingpipeline { SectorStates { SnapDeals { Key, Value}}}}"
    }

    response = request_handler(
        url=f"http://{boost_url}:{options.boost_graphql_port}/graphql/query",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload)},
        log_name="get_snap_sectors_count",
        miner_auth_header={"add": False},
    )
    if response == None:
        return -1
    else:
        count = 0
        states = response.get("data", {})
        if states == None:
            log.error(f"Boost API returned an unexpected result. Nonetype instead of an object. Ignoring")
            return options.maximum_snap_sectors_in_flight

        snapdeals = states.get("sealingpipeline", {}).get("SectorStates", {}).get("SnapDeals", [])
        for i in snapdeals:
            if i['Key'] in ['UpdateReplica', 'ProveReplicaUpdate']:
                count += i['Value']
        log.debug(f"Found {count} sectors in RU/PRU states.")
        return count


def get_adding_to_sector_count(*, options: dict) -> int:
    log.debug("Querying count of deals in 'Adding to Sector' state")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query {deals(limit: 1000, filter: {IsOffline: true, Checkpoint: PublishConfirmed}) {totalCount}}"
    }

    response = request_handler(
        url=f"http://{boost_url}:{options.boost_graphql_port}/graphql/query",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload)},
        log_name="get_adding_to_sector_count",
        miner_auth_header={"add": False},
    )
    if response == None:
        return -1
    else:
        states = response.get("data", {})
        if states == None:
            log.error(f"Boost API returned an unexpected result. Nonetype instead of an object. Ignoring")
            return options.maximum_sectors_in_adding_piece_state

        count = states.get("deals", {}).get("totalCount", options.maximum_sectors_in_adding_piece_state)
        log.debug(f"Found {count} sectors in 'Adding to Sector' state.")
        return count


def request_handler(*, url: str, method: str, parameters: dict, log_name: str, miner_auth_header: dict) -> bool:
    try:
        return make_request(
            url=url, method=method, parameters=parameters, log_name=log_name, miner_auth_header=miner_auth_header
        )
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    after=tenacity.after.after_log(log_retry, logging.INFO),
)
def make_request(*, url: str, method: str, parameters: dict, log_name: str, miner_auth_header: dict) -> Any:
    try:
        if miner_auth_header["add"]:
            if "stdin" in miner_auth_header:
                auth_token = shell(
                    command=["bash", options.fil_spid_file_path, options.miner_id], stdin=miner_auth_header["stdin"]
                )
            else:
                auth_token = shell(command=["bash", options.fil_spid_file_path, options.miner_id])

            if "headers" not in parameters:
                parameters["headers"] = {"Authorization": auth_token}
            else:
                parameters["headers"]["Authorization"] = auth_token

        if method == "post":
            response = requests.post(url, **parameters)
        if method == "get":
            response = requests.get(url, **parameters)

        res = response.json()

        # Disable logging of noisy responses
        if url != ELIGIBLE_PIECES_ENDPOINT:
            log_request.debug(f"{log_name}, Response: {res}")
    except requests.exceptions.HTTPError as e:
        log_request.error(f"{log_name}, HTTPError: {e}")
        raise Exception(f"HTTPError: {e}")
    except requests.exceptions.ConnectionError as e:
        log_request.error(f"{log_name}, ConnectionError: {e}")
        raise Exception(f"ConnectionError: {e}")
    except (TimeoutError, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout) as e:
        log_request.error(f"{log_name}, Timeout: {e}")
        raise Exception(f"Timeout: {e}")
    except:
        log_request.error(f"{log_name}, Timeout: {e}")
        raise Exception(f"Timeout: {e}")

    if response.status_code == 401:
        if "error_lines" in res and "in the future" in "".join(res["error_lines"]):
            log_request.info(
                f'Known issue in Spade: the auth token generated by fil-spid.bash is "in the future" according to Spade. Retrying.'
            )
            raise Exception("Auth token is in the future.")
        else:
            log_request.error(f"{log_name}, received 401 Unauthorized: {res}")
            return None
    if response.status_code == 403:
        log_request.error(f"{log_name}, received 403 Forbidden: {res}")
        return None

    return res


def shell(*, command: list, stdin: Optional[str] = None) -> str:
    # This is gross and unfortunately necessary. fil-spid.bash is too complex to reimplement in this script.
    try:
        process = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, universal_newlines=True, input=stdin
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Bash command failed with exit code {e.returncode}",
            f"Error message: {e.stderr}",
        ) from e
    return process.stdout.rstrip()


def download(*, options: dict, source: str) -> bool:
    try:
        return download_files(options=options, source=source)
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    after=tenacity.after.after_log(log_retry, logging.WARN),
)
def download_files(*, options: dict, source: str) -> bool:
    if not source.startswith("http"):
        log_aria2.error(f"Error. Unknown file source: {source}")
        # ToDo handle this
    try:
        down = aria2.add_uris(
            [source],
            options={
                "max_connection_per_server": options.aria2c_connections_per_server,
                "auto_file_renaming": False,
                "dir": options.aria2c_download_path,
            },
        )
    except:
        log_aria2.error("Aria2c failed to start download of file.")
        raise Exception("Aria2c failed to start download of file.")

    # This is a hack. For some reason 'max_connection_per_server' is ignored by aria2.add_uris().
    # Since this is incredibly important for this use case, this workaround is required.
    down.options.max_connection_per_server = options.aria2c_connections_per_server
    log_aria2.info(f"Downloading file: {source}")
    return True


def get_downloads() -> dict:
    try:
        return get_aria_downloads()
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    after=tenacity.after.after_log(log_retry, logging.WARN),
)
def get_aria_downloads() -> dict:
    return aria2.get_downloads()


def download_error(e, i):
    try:
        downloads = e.get_downloads()
        for down in downloads:
            if down.gid == i:
                log.info(f"Retrying download due to aria2 error: {down.error_message}")
                response = e.retry_downloads([down], False)
                log.debug(f"Retry status: {response}")
    except:
        log.error(f"Failed to retry download of {i}")


def is_download_in_progress(*, url: str, downloads: list) -> bool:
    for d in downloads:
        for f in d.files:
            for u in f.uris:
                if u["uri"] == url:
                    return True
    return False


def request_deal(*, options: dict) -> Any:
    log.info("Requesting a new deal from Spade")
    # Select a deal
    pieces = eligible_pieces(options=options)
    if pieces == None:
        return None
    if len(pieces["response"]) < 1:
        log.error("Error. No deal pieces returned.")
        return None

    # Randomly select a deal from those returned
    deal_number = random.randint(0, len(pieces["response"]) - 1)
    deal = pieces["response"][deal_number]
    log.debug(f"Deal selected: {deal}")

    # Create a reservation
    return invoke_deal(piece_cid=deal["piece_cid"], tenant_policy_cid=deal["tenant_policy_cid"], options=options)


def setup_aria2p(*, options: dict) -> Any:
    global aria2

    try:
        aria2 = aria2p.API(
            aria2p.Client(
                host=":".join(options.aria2c_url.split(":")[:-1]), port=options.aria2c_url.split(":")[-1], secret=""
            )
        )
        aria2.get_global_options().max_concurrent_downloads = options.aria2c_max_concurrent_downloads
    except:
        log_aria2.error(f"Could not connect to an aria2 daemon running at '{options.aria2c_url}'")
        raise Exception(f"Could not connect to an aria2 daemon running at '{options.aria2c_url}'")

    try:
        aria2.listen_to_notifications(
            threaded=True,
            on_download_start=None,
            on_download_pause=None,
            on_download_stop=None,
            on_download_complete=None,
            on_download_error=download_error,
            on_bt_download_complete=None,
            timeout=5,
            handle_signals=True,
        )
    except:
        log_aria2.error(f"Could not start listening to notifications from aria2c API.")
        raise Exception(f"Could not start listening to notifications from aria2c API.")


def populate_startup_state(*, options: dict) -> dict:
    state = {}
    # Read in deals from Boost. This is the only source necessary on startup as
    # the main control loop will detect and update deal status dynamically.
    deals = get_boost_deals(options=options)
    if deals == None:
        os._exit(1)
    for d in deals:
        state[d["PieceCid"]] = {
            "deal_uuid": d["ID"],
            "files": {},
            "timestamp_in_boost": time.time(),
            "status": "available_in_boost",
        }
    log.info(f"Found {len(deals)} deals in Boost")
    return state


def startup_checks(*, options: dict) -> None:
    # Ensure the file-spid.bash script exists
    if not os.path.exists(options.fil_spid_file_path):
        log.error(f"Authorization script does not exist: {options.fil_spid_file_path}")
        os._exit(1)

    # Ensure the download directory exists
    if not os.path.exists(options.aria2c_download_path):
        log.error(f"Aria2c download directory does not exist: {options.aria2c_download_path}")
        os._exit(1)


def main() -> None:
    global options

    log.info('--== Ace of Spades ==--')
    log.info('Parameters: \n    ' + '\n    '.join(f'{k}={v}' for k, v in vars(options).items() if k != 'boost_api_info'))

    startup_checks(options=options)
    log.info("Connecting to Aria2c...")
    setup_aria2p(options=options)
    log.info("Starting Ace of Spades...")

    deals_in_error_state = []
    state = populate_startup_state(options=options)
    # Example: state = {
    #     '<piece_cid>': {
    #         'deal_uuid': '<deal_uuid>',
    #         'files': {
    #             'http://foo': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #             'http://foo2': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #         },
    #         'timestamp_in_boost': '1234567890',  # The timestamp at which Ace detected the deal in Boost. Used for weeding out an edge case of stale deals.
    #         'status': 'invoked'  #can be one of ['invoked','available_in_boost','downloading','downloaded']
    #     }
    # }

    # Control loop to take actions, verify outcomes, and otherwise manage Spade deals
    while True:
        if options.verbose:
            log.info(f"Loop start state: {json.dumps(state, indent=4)}")
        else:
            log.debug(f"Loop start state: {json.dumps(state, indent=4)}")

        # Request deals from Spade
        if not options.complete_existing_deals_only:
            if len(state) < options.maximum_boost_deals_in_flight:
                if options.maximum_snap_sectors_in_flight == 0 or options.maximum_snap_sectors_in_flight > get_snap_sectors_count(options=options):
                    if options.maximum_sectors_in_adding_piece_state == 0 or options.maximum_sectors_in_adding_piece_state > get_adding_to_sector_count(options=options):
                        for i in range(len(state), options.maximum_boost_deals_in_flight):
                            new_deal = request_deal(options=options)
                            if new_deal != None:
                                if new_deal["piece_cid"] not in deals_in_error_state:
                                    log.debug(f'adding {new_deal["piece_cid"]} to state')
                                    state[new_deal["piece_cid"]] = {
                                        "deal_uuid": "unknown",
                                        "files": {},
                                        "timestamp_in_boost": time.time(),
                                        "status": "invoked",
                                    }
                                    log.info(f'New deal found in Boost: {new_deal["piece_cid"]}')
                    else:
                        log.debug(f'Too many sectors found in "Adding to Sector" state, as count is greater than desired quantity ({options.maximum_sectors_in_adding_piece_state}). Not requesting a new deal.')
                        if len(state) == 0:
                            log.debug(f'No work for Ace to monitor. Sleeping for 2 minutes...')
                            time.sleep(120)
                else:
                    log.debug(f'Snap sector count is greater than desired quantity ({options.maximum_snap_sectors_in_flight}). Not requesting a new deal.')
                    if len(state) == 0:
                        log.debug(f'No work for Ace to monitor. Sleeping for 2 minutes...')
                        time.sleep(120)


        log.debug(f"request state: {json.dumps(state, indent=4)}")

        # Identify when deals are submitted to Boost
        deals = get_boost_deals(options=options)
        if deals != None:
            for d in deals:
                if d["PieceCid"] not in deals_in_error_state:
                    if d["PieceCid"] not in state:
                        # Fallback necessary during certain Ace restart scenarios
                        state[d["PieceCid"]] = {
                            "deal_uuid": d["ID"],
                            "files": {},
                            "timestamp_in_boost": time.time(),
                            "status": "available_in_boost",
                        }
                    if state[d["PieceCid"]]["status"] == "invoked":
                        state[d["PieceCid"]]["status"] = "available_in_boost"
                        state[d["PieceCid"]]["timestamp_in_boost"] = time.time()

        log.debug(f"identify state: {json.dumps(state, indent=4)}")

        # Edge case. If a deal shows up in Boost, but does not appear in Spade for more than 1 hour (3600 seconds),
        # consider the deal to be failed and remove it from consideration. Without this check, this failure scenario can
        # prevent Ace from maintaining its maximum_boost_deals_in_flight.
        for s in list(state):
            if state[s]["status"] == "available_in_boost":
                if state[s]["timestamp_in_boost"] < (time.time() - options.spade_deal_timeout):
                    log.warning(
                        f"Deal can be seen in Boost, but has not appeared in Spade for more than {options.spade_deal_timeout} seconds. Considering the deal to be either failed or not a Spade deal: {s}"
                    )
                    del state[s]
                    deals_in_error_state.append(s)

        proposals = pending_proposals(options=options)
        downloads = get_downloads()

        # Handle Spade errors in creating deals
        log.debug(f"deals_in_error_state: {deals_in_error_state}")
        if proposals != None:
            if "recent_failures" in proposals:
                for p in proposals["recent_failures"]:
                    if p["piece_cid"] in state:
                        log.warning(
                            f'Spade encountered an error with {p["piece_cid"]}: `{p["error"]}`. Ignoring deal and moving on.'
                        )
                        del state[p["piece_cid"]]
                        deals_in_error_state.append(p["piece_cid"])

        # Start downloading deal files
        if proposals != None and downloads != None:
            for p in proposals["pending_proposals"]:
                if p["piece_cid"] in state:
                    if state[p["piece_cid"]]["status"] == "available_in_boost":
                        state[p["piece_cid"]]["deal_uuid"] = p["deal_proposal_id"]

                        # Start download of all files in deal
                        running = 0
                        for source in p["data_sources"]:
                            # Notice and ingest preexisting downloads, whether by a previous Ace process or manual human intervention
                            if is_download_in_progress(url=source, downloads=downloads):
                                state[p["piece_cid"]]["files"][source] = "incomplete"
                                running += 1
                            else:
                                if download(source=source, options=options):
                                    running += 1
                                    state[p["piece_cid"]]["files"][source] = "incomplete"
                                else:
                                    log.error(f"Failed to start download of URL: {source}")
                        if running == len(p["data_sources"]):
                            state[p["piece_cid"]]["status"] = "downloading"

        log.debug(f"download state: {json.dumps(state, indent=4)}")

        # Check for completed downloads
        downloads = get_downloads()
        if downloads != None:
            for down in downloads:
                if down.is_complete == True:
                    for s in state.keys():
                        # Ensure files have been populated before proceeding
                        if bool(state[s]["files"]):
                            for source in state[s]["files"].keys():
                                # If the completed downloads's source matches this deal, change 'incomplete' to the file path
                                if source == down.files[0].uris[0]["uri"]:
                                    state[s]["files"][source] = str(down.files[0].path)
                                    break
                            if all([v != "incomplete" for v in state[s]["files"].values()]):
                                state[s]["status"] = "downloaded"
                                log.info(f"Download complete: {s}")
                    # Cleanup download from Aria2c
                    down.purge()

        log.debug(f"completed download state: {json.dumps(state, indent=4)}")

        # Import deals to Boost
        for s in list(state):
            outcome = []
            if state[s]["status"] == "downloaded":
                for f in state[s]["files"].keys():
                    out = boost_import(options=options, deal_uuid=state[s]["deal_uuid"], file_path=state[s]["files"][f])
                    if out != None:
                        outcome.append(out)

                # If all files for this deal have been imported, delete the deal from local state
                if all(outcome):
                    log.info(f"Deal complete: {s}")
                    del state[s]

        if options.verbose:
            log.info(f"Loop end state: {json.dumps(state, indent=4)}")
        else:
            log.debug(f"Loop end state: {json.dumps(state, indent=4)}")

        if options.complete_existing_deals_only:
            if len(state) == 0:
                log.info("No more deals in flight. Exiting due to --complete-existing-deals-only flag.")
                os._exit(0)

        time.sleep(15)


if __name__ == "__main__":
    main()
