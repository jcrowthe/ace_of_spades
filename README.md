# Ace of Spades

_Ace of Spades_ ("Ace") is a project complimentary to Spade, a Fil+ dealmaking component in the Filecoin community. _Ace of Spades_ orchestrates automated deal creation through Spade, data download, and deal import to Boost.

_Ace of Spades_ has been built to be stateless. It accomplishes this by dynamically rebuilding its internal state based on the contents of the APIs it pulls and pushes data to (ie. Spade, Aria2c, and Boost). This makes Ace reasonably resilient to failures, allowing for unintentional restarts due to hardware or software failure, or intentional shutdowns such as for maintenance and upgrades. When Spade is started up again, it will pick up from where it last left off. _Ace_ requires no database in order to function and the code is contained in a single Python file.

_Ace of Spades_ requires a minimum of Boost v1.7.x to function.

## Installation and Requirements
There are three components that _Ace_ relies upon to function: [Boost](https://boost.filecoin.io/), [Spade](https://github.com/ribasushi/spade), and [Aria2c](https://aria2.github.io/). From the perspective of the operator of _Ace_, Spade is a service provided to you, Boost is your dealmaking component for your SP that you run, and Aria2c is a new daemon component you will need to run in order to use _Ace_. All downloading of .CAR files has been offloaded to Aria2c since it is already a robust and performant download manager.

To install dependencies:
```
pip3 install -r requirements.txt
```

You will also need a copy of `fil-spid.bash`. Refer to the Spade slack channel and repo for a copy of this.

## Usage

As stated above, it is the operator's responsibilty to run two components: Aria2c and _Ace of Spades_. Aria2c manages all downloads while _Ace_ manages the flow 

### Aria2c

To run this, install [aria2c](https://aria2.github.io/) and start it with the JSON-RPC server:
```
aria2c --enable-rpc
```

### Starting Ace

Example usage:

```
python3 ace_of_spades.py \
    --miner-id f0123456789 \
    --fil-spid-file-path /path/to/fil-spid.bash \
    --aria2c-download-path /mnt/localssd/incoming/
```

A token to talk to boost is also required, and can either be specified on the command line with --boost-api-info or using the BOOST_API_INFO environment variable, as is standard in other Filecoin projects. This token should be specified in the standard Boost notation (ie. 'export BOOST_API_INFO=eyJhbG...aCG:/ip4/10.1.1.10/tcp/1234/http`). Note that since Boost has two APIs (GraphQL and JSON-RPC), and that only the JSON-RPC endpoint is found in the BOOST_API_INFO environment variable, the GraphQL port must also be specified.

All parameters to _Ace_ can be specified either via command line flag or environment variable. See `--help` for both CLI flag names and env var names.
```
usage: ace_of_spades.py [-h] [--miner-id MINER_ID] [--fil-spid-file-path FIL_SPID_FILE_PATH] [--aria2c-url [ARIA2C_URL]] [--aria2c-connections-per-server [ARIA2C_CONNECTIONS_PER_SERVER]] [--aria2c-max-concurrent-downloads [ARIA2C_MAX_CONCURRENT_DOWNLOADS]] [--aria2c-download-path [ARIA2C_DOWNLOAD_PATH]]
                        [--boost-api-info BOOST_API_INFO] [--boost-graphql-port [BOOST_GRAPHQL_PORT]] [--boost-delete-after-import [BOOST_DELETE_AFTER_IMPORT]] [--spade-deal-timeout [SPADE_DEAL_TIMEOUT]] [--maximum-boost-deals-in-flight [MAXIMUM_BOOST_DEALS_IN_FLIGHT]]
                        [--maximum-snap-sectors-in-flight [MAXIMUM_SNAP_SECTORS_IN_FLIGHT]] [--complete-existing-deals-only [COMPLETE_EXISTING_DEALS_ONLY]] [--verbose [VERBOSE]] [--debug [DEBUG]]

options:
  -h, --help            show this help message and exit
  --miner-id MINER_ID   Storage Provider miner ID (ie. f0123456)
  --fil-spid-file-path FIL_SPID_FILE_PATH
                        Full file path of the `fil-spid.bash` authorization script provided by Spade.
  --aria2c-url [ARIA2C_URL]
                        URL of the aria2c process running in daemon mode (eg. 'http://localhost:6800'). Launch the daemon with `aria2c --enable-rpc`.
  --aria2c-connections-per-server [ARIA2C_CONNECTIONS_PER_SERVER]
                        Configures the '-x' flag in aria2c. (eg. aria2c -x8 <uri>). Default: 10
  --aria2c-max-concurrent-downloads [ARIA2C_MAX_CONCURRENT_DOWNLOADS]
                        Configures the '-j' flag in aria2c. (eg. aria2c -j10). Default: 10
  --aria2c-download-path [ARIA2C_DOWNLOAD_PATH]
                        The directory into which aria2c should be configured to download files before being imported to Boost. Default: /mnt/data
  --boost-api-info BOOST_API_INFO
                        The Boost api string normally set as the BOOST_API_INFO environment variable (eg. 'eyJhbG...aCG:/ip4/10.0.0.10/tcp/1234/http')
  --boost-graphql-port [BOOST_GRAPHQL_PORT]
                        The port number where Boost's graphql is hosted (eg. 8080)
  --boost-delete-after-import [BOOST_DELETE_AFTER_IMPORT]
                        Whether or not to instruct Boost to delete the downloaded data after it is imported. Equivalent of 'boostd --delete-after-import'. Default: True
  --spade-deal-timeout [SPADE_DEAL_TIMEOUT]
                        The time to wait between a deal appearing in Boost and appearing in Spade before considering the deal failed (or not a Spade deal) and ignoring it. Stated in seconds, with no units. Default: 3600
  --maximum-boost-deals-in-flight [MAXIMUM_BOOST_DEALS_IN_FLIGHT]
                        The maximum number of deals in 'Awaiting Offline Data Import' state in Boost UI. Default: 10
  --maximum-snap-sectors-in-flight [MAXIMUM_SNAP_SECTORS_IN_FLIGHT]
                        The maximum number of UpdateReplica and ProveReplicaUpdate tasks being processed by the miner. If above this limit, new Spade deals will not be requested until count is below this number again. Default: 0 (meaning no limit)
  --complete-existing-deals-only [COMPLETE_EXISTING_DEALS_ONLY]
                        Setting this flag will prevent new deals from being requested but allow existing deals to complete. Useful for cleaning out the deals pipeline to debug, or otherwise. Default: False
  --verbose [VERBOSE]   If enabled, logging will be greatly increased. Default: False
  --debug [DEBUG]       If enabled, logging will be thorough, enabling debugging of deep issues. Default: False
```

### Support

Consider this code to be provided without support. Feel free to open tickets, but responses will be best-effort.

### License

MIT license. Feel free to use _Ace of Spades_ however you see fit.

### Contributions

Pull requests are welcome! This is the quickest way to getting features or fixes that you need. When someone opens an issue _and_ provides a PR to fix the issue, you can expect a response.

To enforce code style, a Makefile with `black` has been provided. Please run `make format` prior to opening PRs to ensure consistent style.

### Donate

Like what you see? Buy me lunch! Send donations to `f1ykyg3jzptvjbvf4leuy6ya3xdssrc7khpp6b6gq` and say hello to me at `@Jacob Crowther` on Filecoin slack.