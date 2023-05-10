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

from typing import Any, Dict, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--miner-id",
        help="Storage Provider miner ID (ie. f0123456)",
        default=os.environ.get("MINER_ID"),
        required=not os.environ.get("MINER_ID"),
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
        help="Configures the '-x' flag in aria2c. (eg. aria2c -x8 <uri>)",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_CONNECTIONS_PER_SERVER", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-max-concurrent-downloads",
        help="Configures the '-j' flag in aria2c. (eg. aria2c -j10)",
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
        help="The Boost api string normally set as the BOOST_API_INFO environment variable (eg. 'eyJhbG...aCG:/ip4/192.168.10.10/tcp/3051/http')",
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
        "--maximum-boost-deals-in-flight",
        help="The maximum number of deals in 'Awaiting Offline Data Import' state in Boost UI. Default: 10",
        nargs="?",
        const=10,
        type=int,
        default=os.environ.get("MAXIMUM_BOOST_DEALS_IN_FLIGHT", 10),
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
    parser.add_argument(
        "--complete-existing-deals-only",
        help="Setting this flag will prevent new deals from being requested but allow existing deals to complete. Useful for cleaning out the deals pipeline to debug, or otherwise. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("COMPLETE_EXISTING_DEALS_ONLY", False),
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
log_boost = get_logger("Boost", options=options)

ELIGIBLE_PIECES_ENDPOINT = "https://api.spade.storage/sp/eligible_pieces"
INVOKE_ENDPOINT = "https://api.spade.storage/sp/invoke"
PENDING_PROPOSALS_ENDPOINT = "https://api.spade.storage/sp/pending_proposals"


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=15, multiplier=2), after=tenacity.after.after_log(log_retry, logging.INFO)
)
def eligible_pieces(*, options: dict) -> dict:
    log.debug("Querying for eligible pieces")
    headers = {"Authorization": shell(command=["bash", "fil-spid.bash", options.miner_id])}
    response = requests.get(ELIGIBLE_PIECES_ENDPOINT, timeout=30, headers=headers, allow_redirects=True)
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError:
        log.error(f"Failed to get eligible_pieces. {response.text}")
        raise Exception(f"Failed to get eligible_pieces. {response.text}, {response.status_code}, {response.headers}")


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=3, multiplier=2), after=tenacity.after.after_log(log_retry, logging.INFO)
)
def invoke_deal(*, piece_cid: str, tenant_policy_cid: str, options: dict) -> dict:
    log.debug("Invoking a new deal")
    stdin = f"call=reserve_piece&piece_cid={piece_cid}&tenant_policy={tenant_policy_cid}"
    auth_token = shell(command=["bash", "fil-spid.bash", options.miner_id], stdin=stdin)

    headers = {"Authorization": auth_token}
    response = requests.post(INVOKE_ENDPOINT, timeout=30, headers=headers, allow_redirects=True)
    try:
        response.raise_for_status()
        res = response.json()
        res["piece_cid"] = re.findall(r"baga[a-zA-Z0-9]+", res["info_lines"][0])[0]
        log.debug(f"New deal requested: {res}")
        return res
    except requests.exceptions.HTTPError:
        log.error(f"Failed to invoke deal. {response.text}")
        raise Exception(f"Failed to invoke deal. {response.text}, {response.status_code}, {response.headers}")


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=15, multiplier=2), after=tenacity.after.after_log(log_retry, logging.INFO)
)
def pending_proposals(*, options: dict) -> list:
    headers = {"Authorization": shell(command=["bash", "fil-spid.bash", options.miner_id])}
    response = requests.get(PENDING_PROPOSALS_ENDPOINT, timeout=30, headers=headers, allow_redirects=True)

    try:
        response.raise_for_status()
        return response.json()["response"]["pending_proposals"]
    except requests.exceptions.HTTPError:
        log.error(f"Failed to get pending proposals. {response.text}")
        raise Exception(
            f"Querying pending_proposals resulted in an HTTPError. {response.text}, {response.status_code}, {response.headers}"
        )


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

    response = requests.post(f"http://{boost_url}:{boost_port}/rpc/v0", data=json.dumps(payload), headers=headers)
    json_rpc_id += 1
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        log_boost.error("Error talking to Boost")
        raise Exception("Error talking to Boost")

    if response.json()["result"]["Accepted"]:
        log.debug(f"Deal imported to boost: {response.json()}")
        log.info(f"Deal imported to boost: {deal_uuid}")
        return True
    else:
        log.error(f"Deal failed to be imported to boost: {response.json()}")
        return False


def get_boost_deals(*, options: dict) -> Any:
    # ToDo: Filter out deals not managed by Spade by comparing against list of pending_proposals or filtering by the client address. Currently all deals are expected to be Spade deals.
    log.debug("Querying deals from boost")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query {deals(limit: 1000, filter: {IsOffline: true, Checkpoint: Accepted}) {deals {ID CreatedAt Checkpoint IsOffline Err PieceCid Message}totalCount}}"
    }

    response = requests.post(f"http://{boost_url}:{options.boost_graphql_port}/graphql/query", data=json.dumps(payload))
    try:
        response.raise_for_status()
        log.debug(f'Deal data from boost: {response.json()["data"]["deals"]}')
        deals = response.json()["data"]["deals"]["deals"]
        return [d for d in deals if d["Message"] == "Awaiting Offline Data Import"]
    except requests.exceptions.HTTPError:
        log_boost.error("Error talking to Boost")
        raise Exception("Error talking to Boost")


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


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=3, multiplier=2), after=tenacity.after.after_log(log_retry, logging.WARN)
)
def download(*, options: dict, source: str) -> bool:
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


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=15, multiplier=2), after=tenacity.after.after_log(log_retry, logging.WARN)
)
def get_downloads() -> dict:
    return aria2.get_downloads()


def download_error(e, i):
    log.error(f"Download error: {e}, {i}")
    breakpoint()


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
    if len(pieces["response"]) < 1:
        log.error("Error. No deal pieces returned.")

    # Randomly select a deal from those returned
    deal_number = random.randint(0, len(pieces["response"]) - 1)
    deal = pieces["response"][deal_number]
    log.debug(f"Deal selected: {deal}")

    # Create a reservation
    reservation = invoke_deal(piece_cid=deal["piece_cid"], tenant_policy_cid=deal["tenant_policy_cid"], options=options)
    return reservation


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
    for d in deals:
        state[d["PieceCid"]] = {
            "deal_uuid": d["ID"],
            "files": {},
            "status": "available_in_boost",
        }
    return state


def main() -> None:
    global options
    log.info("Connecting to Aria2c...")
    setup_aria2p(options=options)
    log.info("Starting Ace of Spades...")

    state = populate_startup_state(options=options)
    # Example: state = {
    #     '<piece_cid>': {
    #         'deal_uuid': '<deal_uuid>',
    #         'files': {
    #             'http://foo': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #             'http://foo2': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #         },
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
                for i in range(len(state), options.maximum_boost_deals_in_flight):
                    new_deal = request_deal(options=options)
                    log.debug(f'adding {new_deal["piece_cid"]} to state')
                    state[new_deal["piece_cid"]] = {
                        "deal_uuid": "unknown",
                        "files": {},
                        "status": "invoked",
                    }
                    log.info(f'New deal found in Boost: {new_deal["piece_cid"]}')

        log.debug(f"request state: {json.dumps(state, indent=4)}")

        # Identify when deals are submitted to Boost
        deals = get_boost_deals(options=options)
        log.debug(f"Deals: {deals}")
        for d in deals:
            if d["PieceCid"] not in state:
                # Fallback necessary during certain Ace restart scenarios
                state[d["PieceCid"]] = {
                    "deal_uuid": d["ID"],
                    "files": {},
                    "status": "available_in_boost",
                }
            if state[d["PieceCid"]]["status"] == "invoked":
                state[d["PieceCid"]]["status"] = "available_in_boost"

        log.debug(f"identify state: {json.dumps(state, indent=4)}")

        # Start downloading deal files
        proposals = pending_proposals(options=options)
        downloads = get_downloads()
        log.debug(f"Proposals: {proposals}")
        for p in proposals:
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
        for down in get_downloads():
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
                    outcome.append(
                        boost_import(options=options, deal_uuid=state[s]["deal_uuid"], file_path=state[s]["files"][f])
                    )

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

    # rate limit based on number of RU/PR2 jobs currently running


if __name__ == "__main__":
    main()
