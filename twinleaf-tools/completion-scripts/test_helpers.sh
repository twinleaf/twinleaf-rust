# Defines constants and functions for bash/zsh completion tests
# Mostly uses cross-shell syntax; some zsh-only syntax at the bottom
# It's fine for bash to source it though since it's all in functions
RPC_LIST_FAILED="[RPC_LIST_FAILED]"
TIO_SIM_ADDRESS="udp4://127.0.0.1"
PORT_NUM=7855
FAILS=0

TIO_OPTS=( -r -s -h --root --sensor --help )
TIO_RPC_OPTS=( ${TIO_OPTS[@]} -t -T -d --req-type --rep-type --debug )
TIO_RPC_LIST_OPTS=( ${TIO_OPTS[@]} --name-only --capture-only )
TIO_RPC_DUMP_OPTS=( ${TIO_OPTS[@]} --capture )
TIO_CAPTURE_OPTS=( ${TIO_OPTS[@]} --timeout )
TIO_RPC_TYPES=( u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string )

TIO_SIM_RPCS=( dev.desc dev.firmware.upgrade dev.firmware.upload dev.metadata dev.name dev.stop rpc.hash rpc.id rpc.info rpc.list rpc.listinfo rpc.name test.amplitude test.capture test.enable test.frequency test.go test.noise test.status )
TIO_SIM_DEV_RPCS=( dev.desc dev.firmware.upgrade dev.firmware.upload dev.metadata dev.name dev.stop )

_tio_sim() {
	killall tio &>/dev/null
	PORT_NUM=$(( $PORT_NUM + 1 ))
	setsid tio simulate --port $PORT_NUM </dev/null &>/dev/null &
	# Allow the socket to bind before completion runs `tio rpc list`
	sleep 0.15
}

_fail_test() {
	echo "" >&2
	echo "FAILED: $*" >&2
	echo "Expected: ${EXPECTED[*]}" >&2
	echo "Actual:   ${ACTUAL[*]}" >&2
	echo "" >&2
	FAILS=$(( $FAILS + 1 ))
}

_test_compare() {
	if [[ ${#ACTUAL[@]} != ${#EXPECTED[@]} ]]; then
		_fail_test $*
		return 1
	else
		local i
		for ((i = 0; i < ${#ACTUAL[@]}; i++)); do
			if [[ "${ACTUAL[i]}" != "${EXPECTED[i]}" ]]; then
				_fail_test $*
				return 1
			fi
		done
	fi

	# Passed
	echo "Passed: $*"
	return 0
}

# Bash test driver
# Completes the current word as if you typed $@<TAB>
# Usage: set EXPECTED to an array of expected comp options
# (order doesn't matter), then call bcomptest tio rpc ...
# Note: Last argument is completed, include empty last
# argument "" if you want to complete next word
bcomptest() {
	local cmd="$1" func cur prev
	local -a ACTUAL

	# Lookup registered completer for this cmd and save to func
	if [[ $(complete -p "$cmd") =~ -F[[:space:]]+([^[:space:]]+) ]]; then
		func="${BASH_REMATCH[1]}"
	else
		echo "FAILED: $* (no -F completion for $cmd)" >&2
		return 1
	fi

	# Load command line to environment / args
	COMP_WORDS=( "$@" )
	COMP_CWORD=$((${#COMP_WORDS[@]} - 1))
	COMP_LINE="$*"
	COMP_POINT=${#COMP_LINE}
	cur="${COMP_WORDS[COMP_CWORD]}"
	prev=""
	if (( $COMP_CWORD > 0 )); then
		prev="${COMP_WORDS[COMP_CWORD - 1]}"
	fi
	COMPREPLY=()

	# Call our completion function
	"$func" "$cmd" "$cur" "$prev"

	# Sort arrays
	mapfile -t ACTUAL < <(printf '%s\n' "${COMPREPLY[@]}" | sort)
	mapfile -t EXPECTED < <(printf '%s\n' "${EXPECTED[@]}" | sort)

	_test_compare $*
}

_TMPFILE="/tmp/tio_zsh_comp_test_tmp"
# Override zsh's compadd builtin with ours to capture completions
# We write results to _TMPFILE to be read from outside the pty
# We define again because this will be sent to pty as text
compadd() {
	_TMPFILE="/tmp/tio_zsh_comp_test_tmp"
	touch "$_TMPFILE"
	local -a args=( "$@" )

	# -D: filter the named array in place; do not register matches.
	# Find -D by index so clustered flags don't break parsing.
	local di=${args[(i)-D]}
	if (( di <= $#args )); then
		local d=$args[di+1]
		local -a completions=()
		# Completions follow -/-- after the -D array name
		local -a rest=( "${(@)args[di+2,-1]}" )
		if [[ $rest[1] == -- || $rest[1] == - ]]; then
			completions=( "${(@)rest[2,-1]}" )
		else
			completions=( "${(@)rest}" )
		fi
		local -a src=( "${(@P)d}" )
		local -a kept=()
		local i
		for (( i = 1; i <= $#completions; i++ )); do
			if [[ ${completions[i]} == ${PREFIX}* ]]; then
				kept+=( "${src[i]}" )
			fi
		done
		eval "$d"'=( "${kept[@]}" )'
		return $(( ${#kept} == 0 ))
	fi

	# -a: find array names after positional args
	local ai=${args[(i)-a]}
	if (( ai <= $#args )); then
		# Skip one-shot --opt=value completions when the user is on
		# a new empty word.
		if [[ -n $IPREFIX && -z ${words[CURRENT]} ]]; then
			return 1
		fi
		local -a arrnames=()
		local ei=$(( $#args + 1 )) i
		# Find last -/--
		for (( i = $#args; i >= 1; i-- )); do
			if [[ $args[i] == - || $args[i] == -- ]]; then
				ei=$i
				break
			fi
		done
		if (( ei <= $#args && ei > ai )); then
			arrnames=( "${(@)args[ei+1,-1]}" )
		else
			arrnames=( "${args[-1]}" )
		fi
		local arrname m
		for arrname in "${arrnames[@]}"; do
			local -a MATCHES=( "${(@P)arrname}" )
			for m in "${MATCHES[@]}"; do
				echo "$m" >> "$_TMPFILE"
			done
		done
	fi
}

# We test in zsh by using a zpty to simulate completion,
# but defining a fake compadd function (called by _arguments)
# to write our output to a convenient array, which we can then
# read out. Otherwise, this can be called the same as bcomptest
zcomptest() {
	setopt local_options extendedglob
	# Remove _TMPFILE before each test
	rm -f "$_TMPFILE"

	# zsh -f -i: interactive, but skip .zshrc
	# We use $_TMPFILE.setup to load completion and _tio
	local helpers_dir=${functions_source[zcomptest]:A:h}
	local setup="$_TMPFILE.setup"
	# pty will write this when it's sourced setup
	local ready="$_TMPFILE.ready"
	rm -f "$ready"
	{
		print -r -- 'autoload -Uz compinit; compinit -C'
		print -r -- "source ${(q)helpers_dir}/tio_completions_dynamic.zsh"
		which compadd
	} > $setup

	zpty -d _tio_test 2>/dev/null
	zpty _tio_test zsh -f -i
	# zsh -f keeps the environment, but be explicit so `tio rpc list` works
	zpty -w _tio_test "export PATH=${(q)PATH}"$'\n'
	zpty -w _tio_test "source ${(q)setup}; : > ${(q)ready}"$'\n'
	# Poll for ready file
	local -F _t=0
	while [[ ! -f $ready ]]; do
		sleep 0.01
		_t=$(( _t + 0.01 ))
		(( _t > 5 )) && break
	done
	# Brief settle so the shell is back at the prompt before we type
	sleep 0.02

	# Finally send completion input. Matcher retries can keep appending for a
	# while; wait until the capture file stops growing (or timeout).
	zpty -w _tio_test "$@"$'\t'
	local -F _t=0
	local prev=-1 cur=0 stable=0
	while (( _t < 3 )); do
		sleep 0.03
		_t=$(( _t + 0.03 ))
		if [[ -f $_TMPFILE ]]; then
			cur=$(wc -c < "$_TMPFILE")
		else
			cur=0
		fi
		if (( cur == prev && cur > 0 )); then
			(( stable++ ))
			(( stable >= 3 )) && break
		else
			stable=0
		fi
		prev=$cur
	done

	# Now read _TMPFILE into MATCHES
	local MATCHES=()
	if [[ -f $_TMPFILE ]]; then
		for line in "${(@f)"$(<$_TMPFILE)"}"; do
			MATCHES+=( "$line" )
		done
	fi

	# Deduplicate MATCHES to account for multiple compadds
	MATCHES=( ${(u)MATCHES} )

	# Sort arrays and compare
	ACTUAL=( ${(o)MATCHES} )
	EXPECTED=( ${(o)EXPECTED} )
	_test_compare $*
}
