#compdef tio

autoload -U is-at-least

_tio() {
    typeset -A opt_args
    typeset -a _arguments_options
    local ret=1

    if is-at-least 5.2; then
        _arguments_options=(-s -S -C)
    else
        _arguments_options=(-s -C)
    fi

    local context curcontext="$curcontext" state line
    _arguments "${_arguments_options[@]}" : \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
":: :_tio_commands" \
"*::: :->tio" \
&& ret=0
    case $state in
    (tio)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tio-command-$line[1]:"
        case $line[1] in
            (list)
_arguments "${_arguments_options[@]}" : \
'-a[Include serial ports with unknown VID/PID]' \
'--all[Include serial ports with unknown VID/PID]' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
(monitor)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'--fps=[]:FPS:_default' \
'-c+[]:COLORS:_default' \
'--colors=[]:COLORS:_default' \
'--depth=[Routing depth limit (default\: unlimited)]:DEPTH:_default' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
&& ret=0
;;
(health)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'--jitter-window=[Seconds for jitter calculation window (>= 1)]:SECONDS:_default' \
'--ppm-warn=[Warning threshold in parts per million (>= 0)]:PPM:_default' \
'--ppm-err=[Error threshold in parts per million (>= 0)]:PPM:_default' \
'*--streams=[Comma-separated stream IDs to monitor (e.g., 0,1,5)]:IDS:_default' \
'--fps=[UI refresh rate for heartbeat animation and stale detection (1-60)]:FPS:_default' \
'--stale-ms=[Mark streams as stale after this many milliseconds without data (>= 1)]:MS:_default' \
'-n+[Maximum number of events to keep in history (>= 1)]:N:_default' \
'--event-log-size=[Maximum number of events to keep in history (>= 1)]:N:_default' \
'--event-display-lines=[Number of event lines to show (>= 3)]:LINES:_default' \
'-q[Suppress the footer help text]' \
'--quiet[Suppress the footer help text]' \
'-w[Only show warning and error events in the log]' \
'--warnings-only[Only show warning and error events in the log]' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
&& ret=0
;;
(dump)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'--depth=[Routing depth limit (default\: unlimited)]:DEPTH:_default' \
'--duration=[Stop after this wall-clock duration (e.g. 30s, 5m, 2h)]:DURATION:_default' \
'-d[Show parsed data samples]' \
'--data[Show parsed data samples]' \
'-m[Show metadata on boundaries]' \
'--meta[Show metadata on boundaries]' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
(log)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'-f+[Output log file path]:FILE:_default' \
'--depth=[Routing depth (only used in --raw mode)]:DEPTH:_default' \
'--duration=[Stop after this wall-clock duration (e.g. 30s, 5m, 2h)]:DURATION:_default' \
'-u[Unbuffered output (flush every packet)]' \
'--raw[Raw mode\: skip metadata request and dump all packets]' \
'-h[Print help]' \
'--help[Print help]' \
":: :_tio__subcmd__log_commands" \
"*::: :->log" \
&& ret=0

    case $state in
    (log)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tio-log-command-$line[1]:"
        case $line[1] in
            (meta)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'-f+[Output metadata file path]:FILE:_default' \
'-h[Print help]' \
'--help[Print help]' \
":: :_tio__subcmd__log__subcmd__meta_commands" \
"*::: :->meta" \
&& ret=0

    case $state in
    (meta)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tio-log-meta-command-$line[1]:"
        case $line[1] in
            (reroute)
_arguments "${_arguments_options[@]}" : \
'-s+[New device route (e.g., /0/1)]:ROUTE:_default' \
'--sensor=[New device route (e.g., /0/1)]:ROUTE:_default' \
'-o+[Output metadata file path (defaults to <input>_rerouted.tio)]:OUTPUT:_default' \
'--output=[Output metadata file path (defaults to <input>_rerouted.tio)]:OUTPUT:_default' \
'-h[Print help]' \
'--help[Print help]' \
':input -- Input metadata file path:_files' \
&& ret=0
;;
        esac
    ;;
esac
;;
(dump)
_arguments "${_arguments_options[@]}" : \
'-s+[Sensor path in the sensor tree (e.g., /, /0, /0/1)]:SENSOR:_default' \
'--sensor=[Sensor path in the sensor tree (e.g., /, /0, /0/1)]:SENSOR:_default' \
'--depth=[Routing depth limit (default\: unlimited)]:DEPTH:_default' \
'-d[Show parsed data samples]' \
'--data[Show parsed data samples]' \
'-m[Show metadata on boundaries]' \
'--meta[Show metadata on boundaries]' \
'-h[Print help]' \
'--help[Print help]' \
'*::files -- Input log file(s):_files' \
&& ret=0
;;
(inspect)
_arguments "${_arguments_options[@]}" : \
'-h[Print help]' \
'--help[Print help]' \
'*::files -- Input log file(s):_files' \
&& ret=0
;;
(csv)
_arguments "${_arguments_options[@]}" : \
'-s+[Sensor route in the device tree (default\: /); overridden by a route prefix in the selector]:SENSOR:_default' \
'-o+[Output filename prefix]:OUTPUT:_default' \
'-f[Overwrite the output file if it already exists]' \
'--force[Overwrite the output file if it already exists]' \
'-h[Print help]' \
'--help[Print help]' \
'*::args -- Stream selector (name or id, optionally with a route prefix like /0/field) and input .tio files (order-independent):_files' \
&& ret=0
;;
(hdf)
_arguments "${_arguments_options[@]}" : \
'-o+[Output file path (defaults to input filename with .h5 extension)]:OUTPUT:_default' \
'-g+[Filter streams using a glob pattern (e.g. "/*/vector")]:FILTER:_default' \
'--glob=[Filter streams using a glob pattern (e.g. "/*/vector")]:FILTER:_default' \
'-l+[How to organize runs in the output (none=flat, stream=per-stream, device=per-device, global=all-shared)]:SPLIT_LEVEL:((none\:"No run splitting - one table per stream\: /{route}/{stream}"
stream\:"Each stream has independent run counter (separate table per run)"
device\:"All streams on a device share run counter"
global\:"All streams globally share run counter"))' \
'--split=[How to organize runs in the output (none=flat, stream=per-stream, device=per-device, global=all-shared)]:SPLIT_LEVEL:((none\:"No run splitting - one table per stream\: /{route}/{stream}"
stream\:"Each stream has independent run counter (separate table per run)"
device\:"All streams on a device share run counter"
global\:"All streams globally share run counter"))' \
'-p+[When to detect discontinuities (continuous=any gap, monotonic=only time backward)]:SPLIT_POLICY:((continuous\:"Split on any discontinuity (gaps, rate changes, etc.)"
monotonic\:"Only split when time goes backward (allows gaps)"))' \
'--policy=[When to detect discontinuities (continuous=any gap, monotonic=only time backward)]:SPLIT_POLICY:((continuous\:"Split on any discontinuity (gaps, rate changes, etc.)"
monotonic\:"Only split when time goes backward (allows gaps)"))' \
'-c[Enable deflate compression (saves space, slows down write significantly)]' \
'--compress[Enable deflate compression (saves space, slows down write significantly)]' \
'-d[Enable debug output for glob matching]' \
'--debug[Enable debug output for glob matching]' \
'-h[Print help (see more with '\''--help'\'')]' \
'--help[Print help (see more with '\''--help'\'')]' \
'*::files -- Input log file(s):_files' \
&& ret=0
;;
        esac
    ;;
esac
;;
(rpc)
if [[ "$words[2]" == -* ]]; then
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'-t+[RPC request type]:REQ_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'--req-type=[RPC request type]:REQ_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'-T+[RPC reply type]:REP_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'--rep-type=[RPC reply type]:REP_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'-d[Enable debug output]' \
'--debug[Enable debug output]' \
'-h[Print help]' \
'--help[Print help]' \
":: :_tio__subcmd__rpc_names ${line[2,-2]} $(( ${#line[2,-2]} + 1 ))" \
':rpc_arg -- RPC argument value:' \
&& ret=0
else
# Save line to _line, which will come in handy later
local _line=( "${line[@]}" )
_arguments "${_arguments_options[@]}" : \
":: :_tio__subcmd__rpc_commands ${line[2,-2]} $(( ${#line[2,-2]} + 1 ))" \
"*::: :->rpc" \
&& ret=0

    case $state in
    (rpc)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tio-rpc-command-$line[1]:"
        case $line[1] in
            (list)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'--name-only[List names without permissions and types]' \
'--capture-only[List only rpcs that are \`tio capture\`able]' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
(dump)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'--capture[Trigger a capture before dumping]' \
'-h[Print help]' \
'--help[Print help]' \
":: :_tio__subcmd__rpc_names ${_line[3,-2]} $(( ${#_line[3,-2]} + 1 ))" \
&& ret=0
;;
(rpc)
;;
([^-]*)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'-t+[RPC request type]:REQ_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'--req-type=[RPC request type]:REQ_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'-T+[RPC reply type]:REP_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'--rep-type=[RPC reply type]:REP_TYPE:(u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string)' \
'-d[Enable debug output]' \
'--debug[Enable debug output]' \
'-h[Print help]' \
'--help[Print help]' \
':rpc_arg -- RPC argument value:' \
&& ret=0
;;
        esac
    ;;
esac
fi
;;
(capture)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'--timeout=[Maximum time to wait for capture data]:TIMEOUT:_default' \
'-h[Print help]' \
'--help[Print help]' \
":: :_tio__subcmd__capture_rpc_names ${line[2,-2]} $(( ${#line[2,-2]} + 1 ))" \
&& ret=0
;;
(upgrade)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'()--downgrade[List all published firmware for the connected sensor and interactively pick one to install (allows installing an older release)]' \
'-y[Skip confirmation prompt]' \
'--yes[Skip confirmation prompt]' \
'-h[Print help]' \
'--help[Print help]' \
'::firmware_path -- Input firmware image path. Omit to download the latest published firmware for the connected sensor'\''s name and hardware revision:_files' \
&& ret=0
;;
(proxy)
_arguments "${_arguments_options[@]}" : \
'*--mount=[Mount a sensor at a route prefix to multiplex multiple devices (repeatable)]:LOCATOR=/N:_default' \
'-p+[TCP port to listen on for clients]:PORT:_default' \
'--port=[TCP port to listen on for clients]:PORT:_default' \
'(--mount)-s+[Sensor subtree to look at]:SUBTREE:_default' \
'(--mount)--subtree=[Sensor subtree to look at]:SUBTREE:_default' \
'-t+[Deprecated; timestamps are now emitted by the logger (set RUST_LOG to control verbosity)]:TIMESTAMP_FORMAT:_default' \
'--timestamp=[Deprecated; timestamps are now emitted by the logger (set RUST_LOG to control verbosity)]:TIMESTAMP_FORMAT:_default' \
'-T+[Time limit for sensor reconnection attempts (seconds)]:RECONNECT_TIMEOUT:_default' \
'--timeout=[Time limit for sensor reconnection attempts (seconds)]:RECONNECT_TIMEOUT:_default' \
'-k[Kick off slow clients instead of dropping traffic]' \
'--kick-slow[Kick off slow clients instead of dropping traffic]' \
'-v[Verbose output]' \
'--verbose[Verbose output]' \
'-d[Debugging output]' \
'--debug[Debugging output]' \
'--dump[Dump packet traffic except sample data/metadata or heartbeats]' \
'--dump-data[Dump sample data traffic]' \
'--dump-meta[Dump sample metadata traffic]' \
'--dump-hb[Dump heartbeat traffic]' \
'-a[Deprecated; running without -s <url> now auto-detects by default]' \
'--auto[Deprecated; running without -s <url> now auto-detects by default]' \
'-e[Deprecated; use \`tio list\` instead]' \
'--enumerate[Deprecated; use \`tio list\` instead]' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
'::sensor_url -- Sensor URL (e.g., tcp\://localhost, serial\:///dev/ttyUSB0); defaults to auto-detecting a single connected device:_urls' \
":: :_tio__subcmd__proxy_commands" \
"*::: :->proxy" \
&& ret=0

    case $state in
    (proxy)
        words=($line[2] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tio-proxy-command-$line[2]:"
        case $line[2] in
            (nmea)
_arguments "${_arguments_options[@]}" : \
'-r+[Sensor root address]:ROOT:_urls' \
'--root=[Sensor root address]:ROOT:_urls' \
'-s+[Sensor path in the sensor tree]:ROUTE:_default' \
'--sensor=[Sensor path in the sensor tree]:ROUTE:_default' \
'-p+[TCP port to listen on]:TCP_PORT:_default' \
'--port=[TCP port to listen on]:TCP_PORT:_default' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
        esac
    ;;
esac
;;
(simulate)
_arguments "${_arguments_options[@]}" : \
'--samplerate=[Sample rate in Hz]:SAMPLERATE:_default' \
'--frequency=[Initial sine wave frequency in Hz]:FREQUENCY:_default' \
'--amplitude=[Initial sine wave amplitude in V]:AMPLITUDE:_default' \
'--noise=[Initial white noise level in V/sqrt(Hz)]:NOISE:_default' \
'--segment-seconds=[Segment duration in seconds]:SEGMENT_SECONDS:_default' \
'--port=[UDP port to listen on]:PORT:_default' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
&& ret=0
;;
(test)
_arguments "${_arguments_options[@]}" : \
'--samplerate=[Sample rate in Hz]:SAMPLERATE:_default' \
'--frequency=[Initial sine wave frequency in Hz]:FREQUENCY:_default' \
'--amplitude=[Initial sine wave amplitude in V]:AMPLITUDE:_default' \
'--noise=[Initial white noise level in V/sqrt(Hz)]:NOISE:_default' \
'--segment-seconds=[Segment duration in seconds]:SEGMENT_SECONDS:_default' \
'--port=[UDP port to listen on]:PORT:_default' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
&& ret=0
;;
(completions)
_arguments "${_arguments_options[@]}" : \
'-s[Generate static instead of dynamic completions]' \
'--static[Generate static instead of dynamic completions]' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
':shell -- Shell to output completions for:(bash elvish fish powershell zsh)' \
&& ret=0
;;
        esac
    ;;
esac
}

(( $+functions[_tio_commands] )) ||
_tio_commands() {
    local commands; commands=(
'list:List connected devices' \
'monitor:Live sensor data display' \
'health:Live timing and rate diagnostics' \
'dump:Dump raw packets from a device' \
'log:Log samples to a file' \
'rpc:Execute a device RPC' \
'capture:Trigger and read a capture RPC' \
'upgrade:Upgrade device firmware' \
'proxy:Multiplex a sensor over TCP' \
'simulate:Run a simulated sine wave Twinleaf device over UDP' \
'test:(deprecated, use \`simulate\`) Run a simulated sine wave Twinleaf device over UDP' \
'completions:Generate shell completions for tio' \
    )
    _describe -t commands 'tio commands' commands "$@"
}
(( $+functions[_tio__subcmd__capture_commands] )) ||
_tio__subcmd__capture_commands() {
    local commands; commands=()
    _describe -t commands 'tio capture commands' commands "$@"
}
(( $+functions[_tio__subcmd__completions_commands] )) ||
_tio__subcmd__completions_commands() {
    local commands; commands=()
    _describe -t commands 'tio completions commands' commands "$@"
}
(( $+functions[_tio__subcmd__dump_commands] )) ||
_tio__subcmd__dump_commands() {
    local commands; commands=()
    _describe -t commands 'tio dump commands' commands "$@"
}
(( $+functions[_tio__subcmd__health_commands] )) ||
_tio__subcmd__health_commands() {
    local commands; commands=()
    _describe -t commands 'tio health commands' commands "$@"
}
(( $+functions[_tio__subcmd__list_commands] )) ||
_tio__subcmd__list_commands() {
    local commands; commands=()
    _describe -t commands 'tio list commands' commands "$@"
}
(( $+functions[_tio__subcmd__log_commands] )) ||
_tio__subcmd__log_commands() {
    local commands; commands=(
'meta:Log metadata to a file. See "tio log meta --help" for more options' \
'dump:Dump data from binary log file(s)' \
'inspect:Summarize the contents of binary log file(s)' \
'csv:Convert binary log data to CSV' \
'hdf:Convert binary log files to HDF5 format' \
    )
    _describe -t commands 'tio log commands' commands "$@"
}
(( $+functions[_tio__subcmd__log__subcmd__csv_commands] )) ||
_tio__subcmd__log__subcmd__csv_commands() {
    local commands; commands=()
    _describe -t commands 'tio log csv commands' commands "$@"
}
(( $+functions[_tio__subcmd__log__subcmd__dump_commands] )) ||
_tio__subcmd__log__subcmd__dump_commands() {
    local commands; commands=()
    _describe -t commands 'tio log dump commands' commands "$@"
}
(( $+functions[_tio__subcmd__log__subcmd__hdf_commands] )) ||
_tio__subcmd__log__subcmd__hdf_commands() {
    local commands; commands=()
    _describe -t commands 'tio log hdf commands' commands "$@"
}
(( $+functions[_tio__subcmd__log__subcmd__inspect_commands] )) ||
_tio__subcmd__log__subcmd__inspect_commands() {
    local commands; commands=()
    _describe -t commands 'tio log inspect commands' commands "$@"
}
(( $+functions[_tio__subcmd__log__subcmd__meta_commands] )) ||
_tio__subcmd__log__subcmd__meta_commands() {
    local commands; commands=(
'reroute:Reroute metadata packets in a metadata file' \
    )
    _describe -t commands 'tio log meta commands' commands "$@"
}
(( $+functions[_tio__subcmd__log__subcmd__meta__subcmd__reroute_commands] )) ||
_tio__subcmd__log__subcmd__meta__subcmd__reroute_commands() {
    local commands; commands=()
    _describe -t commands 'tio log meta reroute commands' commands "$@"
}
(( $+functions[_tio__subcmd__monitor_commands] )) ||
_tio__subcmd__monitor_commands() {
    local commands; commands=()
    _describe -t commands 'tio monitor commands' commands "$@"
}
(( $+functions[_tio__subcmd__proxy_commands] )) ||
_tio__subcmd__proxy_commands() {
    local commands; commands=(
'nmea:Bridge Twinleaf sensor data to NMEA TCP stream' \
    )
    _describe -t commands 'tio proxy commands' commands "$@"
}
(( $+functions[_tio__subcmd__proxy__subcmd__nmea_commands] )) ||
_tio__subcmd__proxy__subcmd__nmea_commands() {
    local commands; commands=()
    _describe -t commands 'tio proxy nmea commands' commands "$@"
}
(( $+functions[_tio__helper__list_rpcs] )) ||
_tio__helper__list_rpcs() {
    local opts=()
    local next=false;
    for item in $@; do
        if $next; then
            opts+=( "$item" )
            next=false;
        elif [[ "$item" =~ "^(--name-only|--capture-only|--root=.+|--sensor=.+|-s.+|-r.+)$" ]]; then
            opts+=( "$item" )
        elif [[ "$item" =~ "^(-r|-s|--root|--sensor)$" ]]; then
            next=true;
            opts+=( "$item" )
        fi
    done
    IFS=$'\n' reply=($(tio rpc list $opts 2>/dev/null || echo '[RPC_LIST_FAILED]'))
}
(( $+functions[_tio__subcmd__rpc_names] )) ||
_tio__subcmd__rpc_names() {
    # We've been passed an argument array of zsh stuff and then what we added
    # The last element is the length of what we added, we use that to get the rest
    # We use set -- to slice this off of $@ so that the zsh stuff can do its job
    local len=${@[-1]}
    local opts=( ${@[-$len,-2]} )
    set -- ${@:1:-$len}

    local commands
    _tio__helper__list_rpcs ${opts[@]} --name-only
    commands=( "${reply[@]}" )
    _describe -t commands 'rpc names' commands "$@"
}
(( $+functions[_tio__subcmd__capture_rpc_names] )) ||
_tio__subcmd__capture_rpc_names() {
    local len=${@[-1]}
    local opts=( ${@[-$len,-2]} )
    set -- ${@:1:-$len}

    local commands
    _tio__helper__list_rpcs ${opts[@]} --name-only --capture-only
    commands=( "${reply[@]}" )
    _describe -t commands 'capture rpc names' commands "$@"
}
(( $+functions[_tio__subcmd__rpc_commands] )) ||
_tio__subcmd__rpc_commands() {
    local len=${@[-1]}
    local opts=( ${@[-$len,-2]} )
    set -- ${@:1:-$len}

    local commands
    _tio__helper__list_rpcs ${opts[@]} --name-only
    commands=( "${reply[@]}" )
    commands=( "${commands[@]}"
'list:List available RPCs on the device' \
'dump:Dump RPC data from the device' \
    )
    _describe -t commands 'tio rpc commands' commands "$@"
}
(( $+functions[_tio__subcmd__rpc__subcmd__dump_commands] )) ||
_tio__subcmd__rpc__subcmd__dump_commands() {
    local commands; commands=()
    _describe -t commands 'tio rpc dump commands' commands "$@"
}
(( $+functions[_tio__subcmd__rpc__subcmd__list_commands] )) ||
_tio__subcmd__rpc__subcmd__list_commands() {
    local commands; commands=()
    _describe -t commands 'tio rpc list commands' commands "$@"
}
(( $+functions[_tio__subcmd__simulate_commands] )) ||
_tio__subcmd__simulate_commands() {
    local commands; commands=()
    _describe -t commands 'tio simulate commands' commands "$@"
}
(( $+functions[_tio__subcmd__test_commands] )) ||
_tio__subcmd__test_commands() {
    local commands; commands=()
    _describe -t commands 'tio test commands' commands "$@"
}
(( $+functions[_tio__subcmd__upgrade_commands] )) ||
_tio__subcmd__upgrade_commands() {
    local commands; commands=()
    _describe -t commands 'tio upgrade commands' commands "$@"
}

if [ "$funcstack[1]" = "_tio" ]; then
    _tio "$@"
else
    compdef _tio tio
fi
