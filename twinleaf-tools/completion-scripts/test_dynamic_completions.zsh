#!/usr/bin/env zsh
# Zsh dynamic completion tests. Structured around zsh-specific completer
# branches (see completions.rs / tio_completions_dynamic.zsh), not as a
# mirror of the bash suite.

SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"

if ! command -v tio >/dev/null; then
	echo "tio not found on PATH (required inside zsh -f pty)" >&2
	exit 1
fi

autoload -Uz compinit
compinit

zmodload zsh/zpty

source "$SCRIPT_DIR/test_helpers.sh" || exit 1
source "$SCRIPT_DIR/tio_completions_dynamic.zsh" || exit 1

if pgrep ^tio$ >/dev/null; then
	killall tio
fi

### Offline tests (no real rpc completions)

# else-branch: list/dump come from _tio__subcmd__rpc_commands
EXPECTED=( list dump $RPC_LIST_FAILED )
zcomptest tio rpc ""

# words[2]==-*: options only — no list/dump/rpc names
EXPECTED=( ${TIO_RPC_OPTS[@]} )
zcomptest tio rpc -

# Still on -* after a flag: rpc names, never list/dump
EXPECTED=( $RPC_LIST_FAILED )
zcomptest tio rpc -d ""

# _describe prefix matching
EXPECTED=( list )
zcomptest tio rpc li

EXPECTED=( dump )
zcomptest tio rpc du

# [^-]* after an rpc name: arg slot, not more rpc names
EXPECTED=( )
zcomptest tio rpc rpc.name ""

# Same arm with -: rpc opts including -h/--help
EXPECTED=( ${TIO_RPC_OPTS[@]} )
zcomptest tio rpc rpc.name -

EXPECTED=( ${TIO_RPC_TYPES[@]} )
zcomptest tio rpc -t ""

EXPECTED=( ${TIO_RPC_TYPES[@]} )
zcomptest tio rpc --req-type ""

EXPECTED=( ${TIO_RPC_TYPES[@]} )
zcomptest tio rpc -T ""

EXPECTED=( ${TIO_RPC_TYPES[@]} )
zcomptest tio rpc --rep-type ""

EXPECTED=( ${TIO_RPC_LIST_OPTS[@]} )
zcomptest tio rpc list ""

# Already-used options are omitted from further option completion
used=( --name-only )
EXPECTED=( ${(@)TIO_RPC_LIST_OPTS:|used} )
zcomptest tio rpc list --name-only -

# Regression: shared -s/-r/-h with parent `rpc` must not garble list opts
# (zsh omits the short -s once used, but still offers --sensor)
used=( -s )
EXPECTED=( ${(@)TIO_RPC_LIST_OPTS:|used} )
zcomptest tio rpc list -s /0 -

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} )
zcomptest tio rpc dump -

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} )
zcomptest tio capture -

EXPECTED=( ${TIO_RPC_DUMP_OPTS[@]} )
zcomptest tio rpc dump rpc.name ""

EXPECTED=( ${TIO_CAPTURE_OPTS[@]} )
zcomptest tio capture rpc.name ""

EXPECTED=( $RPC_LIST_FAILED )
zcomptest tio capture ""

EXPECTED=( $RPC_LIST_FAILED )
zcomptest tio rpc dump ""

### Live simulate: root/sensor forwarding

# --root= / --root / -r / stuck -rURL / -sPATH / --sensor= all forward into tio rpc list
_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc --root $TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -r $TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -r$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -s/0 -r$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc --sensor=/0 --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -d -r$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( $RPC_LIST_FAILED )
zcomptest tio rpc -s/fake -r$TIO_SIM_ADDRESS:$PORT_NUM ""

# dump saves pre-subcommand opts in _line and slices [3,-2] into rpc list
_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc dump --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

_tio_sim
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc dump -r$TIO_SIM_ADDRESS:$PORT_NUM ""

# list never offers rpc names; --root already on the line is dropped from opts
_tio_sim
used=( --root )
EXPECTED=( ${(@)TIO_RPC_LIST_OPTS:|used} )
zcomptest tio rpc list --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

# capture lists with --capture-only
_tio_sim
EXPECTED=( test.capture )
zcomptest tio capture --root=$TIO_SIM_ADDRESS:$PORT_NUM ""

# words[2]==-*: "dump" is an rpc-name prefix, not the dump subcommand
_tio_sim
EXPECTED=( )
zcomptest tio rpc --root=$TIO_SIM_ADDRESS:$PORT_NUM dump

### Connect proxy, so we don't need to use --root every time

_tio_sim
setsid tio proxy $TIO_SIM_ADDRESS:$PORT_NUM </dev/null &>/dev/null &
sleep 0.15

EXPECTED=( list dump ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc ""

EXPECTED=( ${TIO_SIM_DEV_RPCS[@]} )
zcomptest tio rpc dev

EXPECTED=( list )
zcomptest tio rpc li

EXPECTED=( dump )
zcomptest tio rpc du

EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc dump ""

EXPECTED=( test.capture )
zcomptest tio capture ""

EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -s/0 ""

EXPECTED=( $RPC_LIST_FAILED )
zcomptest tio rpc -s /fake ""

# Flags/values must not be parsed as the rpc name
EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -d ""

EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc --debug ""

EXPECTED=( ${TIO_SIM_RPCS[@]} )
zcomptest tio rpc -s /0 -t u8 ""

killall tio &>/dev/null || true

if (( $FAILS > 0 )); then
	echo "$FAILS test(s) failed" >&2
	exit 1
fi
echo "All dynamic completion tests passed"
exit 0
