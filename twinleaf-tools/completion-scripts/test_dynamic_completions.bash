#!/usr/bin/env bash
# Requires bash-completion installed in /usr/share (typical on Linux).
# Do not use `set -e` here: bash-completion uses `((expr))` which returns 1 when false.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source /usr/share/bash-completion/bash_completion || exit 1
source "$SCRIPT_DIR/test_helpers.sh" || exit 1
source "$SCRIPT_DIR/tio_completions_dynamic.bash" || exit 1

if pgrep ^tio$ >/dev/null; then
	killall tio
fi

### Offline tests (no real rpc completions)

EXPECTED=( ${TIO_RPC_OPTS[@]} list dump $RPC_LIST_FAILED )
bcomptest tio rpc ""

# After an rpc name: rpcname subcmd — options + [ARG], no more rpc names
EXPECTED=( ${TIO_RPC_OPTS[@]} [ARG])
bcomptest tio rpc rpc.name ""

EXPECTED=( ${TIO_RPC_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio rpc -s /0 ""

EXPECTED=( ${TIO_RPC_TYPES[@]} )
bcomptest tio rpc -t ""

# cur=-* gates _tio__helper__append_rpcs: options only, no rpc list call
EXPECTED=( ${TIO_RPC_OPTS[@]} )
bcomptest tio rpc -

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio capture ""

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio capture -s /0 ""

# Name already supplied: options only
EXPECTED=( ${TIO_CAPTURE_OPTS[@]} )
bcomptest tio capture rpc.name ""

EXPECTED=( ${TIO_RPC_LIST_OPTS[@]} )
bcomptest tio rpc list ""

EXPECTED=( ${TIO_RPC_LIST_OPTS[@]} )
bcomptest tio rpc list -s /0 -

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio rpc dump ""

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio rpc dump -s /0 ""

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} )
bcomptest tio rpc dump rpc.name ""

### Live simulate: root/sensor forwarding
# --root= / --root / -r / stuck -rURL / -sPATH all forward into tio rpc list

_tio_sim
EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc --root $TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc -r $TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc -r$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc -s/0 -r$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_RPC_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio rpc -s/fake -r$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc dump --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

# list never appends rpc names
_tio_sim
EXPECTED=( ${TIO_RPC_LIST_OPTS[@]} )
bcomptest tio rpc list --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

# capture lists with --capture-only
_tio_sim
EXPECTED=( ${TIO_CAPTURE_OPTS[@]} test.capture )
bcomptest tio capture --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

### Connect proxy, so we don't need to use --root every time

_tio_sim
setsid tio proxy $TIO_SIM_ADDRESS:$PORT_NUM </dev/null &>/dev/null &

EXPECTED=( ${TIO_RPC_OPTS[@]} list dump ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc ""

# Prefix filter on COMPREPLY
EXPECTED=( ${TIO_SIM_DEV_RPCS[@]} )
bcomptest tio rpc dev

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc dump ""

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} test.capture )
bcomptest tio capture ""

EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc -s/0 ""

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc dump -s/0 ""

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} test.capture )
bcomptest tio capture -s/0 ""

EXPECTED=( ${TIO_RPC_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio rpc -s /fake ""

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio capture -s /fake ""

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} $RPC_LIST_FAILED )
bcomptest tio rpc dump -s /fake ""

# Flags/values must not be parsed as the rpc name (rpc_opt / $next)
EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc -d ""

EXPECTED=( ${TIO_RPC_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc -s /0 -t u8 ""

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} ${TIO_SIM_RPCS[@]} )
bcomptest tio rpc dump -s /0 --capture ""

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} test.capture )
bcomptest tio capture -s /0 --timeout 1 ""

killall tio &>/dev/null || true

if (( $FAILS > 0 )); then
	echo "$FAILS test(s) failed" >&2
	exit 1
fi
echo "All dynamic completion tests passed"
exit 0
