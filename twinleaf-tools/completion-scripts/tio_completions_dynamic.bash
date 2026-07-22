_tio() {
    local i cur prev opts cmd words next rpc_opt
    COMPREPLY=()
    # $COMP_WORDS breaks on : & = which is bad for roots and flags;
    # This function fills $words with a version that doesn't
    _get_comp_words_by_ref -n := words
    if [[ "${BASH_VERSINFO[0]}" -ge 4 ]]; then
        cur="$2"
    else
        cur="${words[COMP_CWORD]}"
    fi
    prev="$3"
    cmd=""
    opts=""
    next=false
    rpc_opt=false

    for i in "${words[@]:0:COMP_CWORD}"
    do
        case "${cmd},${i}" in
            ",$1")
                cmd="tio"
                ;;
            tio,capture)
                cmd="tio__subcmd__capture"
                ;;
            tio,completions)
                cmd="tio__subcmd__completions"
                ;;
            tio,dump)
                cmd="tio__subcmd__dump"
                ;;
            tio,health)
                cmd="tio__subcmd__health"
                ;;
            tio,list)
                cmd="tio__subcmd__list"
                ;;
            tio,log)
                cmd="tio__subcmd__log"
                ;;
            tio,monitor)
                cmd="tio__subcmd__monitor"
                ;;
            tio,proxy)
                cmd="tio__subcmd__proxy"
                ;;
            tio,rpc)
                cmd="tio__subcmd__rpc"
                ;;
            tio,simulate)
                cmd="tio__subcmd__simulate"
                ;;
            tio,test)
                cmd="tio__subcmd__test"
                ;;
            tio,upgrade)
                cmd="tio__subcmd__upgrade"
                ;;
            tio__subcmd__log,csv)
                cmd="tio__subcmd__log__subcmd__csv"
                ;;
            tio__subcmd__log,dump)
                cmd="tio__subcmd__log__subcmd__dump"
                ;;
            tio__subcmd__log,hdf)
                cmd="tio__subcmd__log__subcmd__hdf"
                ;;
            tio__subcmd__log,inspect)
                cmd="tio__subcmd__log__subcmd__inspect"
                ;;
            tio__subcmd__log,meta)
                cmd="tio__subcmd__log__subcmd__meta"
                ;;
            tio__subcmd__log__subcmd__meta,reroute)
                cmd="tio__subcmd__log__subcmd__meta__subcmd__reroute"
                ;;
            tio__subcmd__proxy,nmea)
                cmd="tio__subcmd__proxy__subcmd__nmea"
                ;;
            tio__subcmd__rpc,dump)
                cmd="tio__subcmd__rpc__subcmd__dump"
                ;;
            tio__subcmd__rpc,list)
                cmd="tio__subcmd__rpc__subcmd__list"
                ;;
            tio__subcmd__rpc,*)
                if $next; then
                    next=false
                elif [[ "$i" =~ ^(-r|-s|-t|-T|--root|--sensor|--rep-type|--req-type)$ ]]; then
                    next=true
                    rpc_opt=true
                elif [[ "$i" == -* ]]; then
                    rpc_opt=true
                else
                    cmd="tio__subcmd__rpc__subcmd__rpcname"
                fi
                ;;
            tio__subcmd__rpc__subcmd__dump,*)
                if $next; then
                    next=false
                elif [[ "$i" =~ ^(-r|-s|--root|--sensor)$ ]]; then
                    next=true
                elif [[ "$i" != -* ]]; then
                    cmd="tio__subcmd__rpc__subcmd__dump__subcmd__rpcname"
                fi
                ;;
            tio__subcmd__capture,*)
                if $next; then
                    next=false
                elif [[ "$i" =~ ^(-r|-s|--root|--sensor|--timeout)$ ]]; then
                    next=true
                elif [[ "$i" != -* ]]; then
                    cmd="tio__subcmd__capture__subcmd__rpcname"
                fi
                ;;
            *)
                ;;
        esac
    done

    case "${cmd}" in
        tio)
            opts="-h -V --help --version list monitor health dump log rpc capture upgrade proxy simulate test completions"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 1 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__capture)
            opts="-r -s -h --root --sensor --timeout --help $(_tio__helper__append_rpcs --name-only --capture-only)"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --timeout)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__completions)
            opts="-s -h -V --static --help --version bash elvish fish powershell zsh"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__dump)
            opts="-r -s -d -m -h --root --sensor --data --meta --depth --duration --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --depth)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --duration)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__health)
            opts="-r -s -q -n -w -h -V --root --sensor --jitter-window --ppm-warn --ppm-err --streams --quiet --fps --stale-ms --event-log-size --event-display-lines --warnings-only --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --jitter-window)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --ppm-warn)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --ppm-err)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --streams)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --fps)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --stale-ms)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --event-log-size)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -n)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --event-display-lines)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__list)
            opts="-a -h --all --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log)
            opts="-r -s -f -u -h --root --sensor --raw --depth --duration --help meta dump inspect csv hdf"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -f)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --depth)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --duration)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log__subcmd__csv)
            opts="-s -o -f -h --force --help [ARGS]..."
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -o)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log__subcmd__dump)
            opts="-d -m -s -h --data --meta --sensor --depth --help <FILES>..."
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --depth)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log__subcmd__hdf)
            opts="-o -g -c -d -l -p -h --glob --compress --debug --split --policy --help <FILES>..."
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                -o)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --glob)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -g)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --split)
                    COMPREPLY=($(compgen -W "none stream device global" -- "${cur}"))
                    return 0
                    ;;
                -l)
                    COMPREPLY=($(compgen -W "none stream device global" -- "${cur}"))
                    return 0
                    ;;
                --policy)
                    COMPREPLY=($(compgen -W "continuous monotonic" -- "${cur}"))
                    return 0
                    ;;
                -p)
                    COMPREPLY=($(compgen -W "continuous monotonic" -- "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log__subcmd__inspect)
            opts="-h --help <FILES>..."
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log__subcmd__meta)
            opts="-r -s -f -h --root --sensor --help reroute"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -f)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__log__subcmd__meta__subcmd__reroute)
            opts="-s -o -h --sensor --output --help <INPUT>"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --output)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -o)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__monitor)
            opts="-r -s -c -h -V --root --sensor --fps --colors --depth --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --fps)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --colors)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -c)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --depth)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__proxy)
            opts="-p -k -s -v -d -t -T -a -e -h -V --mount --port --kick-slow --subtree --verbose --debug --timestamp --timeout --dump --dump-data --dump-meta --dump-hb --auto --enumerate --help --version [SENSOR_URL] nmea"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --mount)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --port)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -p)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --subtree)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --timestamp)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -t)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --timeout)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -T)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__proxy__subcmd__nmea)
            opts="-r -s -p -h --root --sensor --port --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --port)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -p)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__rpc)
            opts="-r -s -t -T -d -h --root --sensor --req-type --rep-type --debug --help "
            if ! $rpc_opt; then
                opts="$opts list dump"
            fi
            opts="$opts $(_tio__helper__append_rpcs --name-only)"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --req-type)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                -t)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                --rep-type)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                -T)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__rpc__subcmd__rpcname)
            opts="-r -s -t -T -d -h --root --sensor --req-type --rep-type --debug --help [ARG]"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --req-type)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                -t)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                --rep-type)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                -T)
                    COMPREPLY=($(compgen -W "u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 string" -- "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;

        tio__subcmd__rpc__subcmd__dump)
            opts="-r -s -h --root --sensor --capture --help $(_tio__helper__append_rpcs --name-only)"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__rpc__subcmd__dump__subcmd__rpcname)
            opts="-r -s -h --root --sensor --capture --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;

        tio__subcmd__capture__subcmd__rpcname)
            opts="-r -s -h --root --sensor --timeout --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --timeout)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__rpc__subcmd__list)
            opts="-r -s -h --root --sensor --name-only --capture-only --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__simulate)
            opts="-h -V --samplerate --frequency --amplitude --noise --segment-seconds --port --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --samplerate)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --frequency)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --amplitude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --noise)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-seconds)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --port)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__test)
            opts="-h -V --samplerate --frequency --amplitude --noise --segment-seconds --port --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --samplerate)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --frequency)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --amplitude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --noise)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-seconds)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --port)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        tio__subcmd__upgrade)
            opts="-r -s -y -h --root --sensor --downgrade --yes --help [FIRMWARE_PATH]"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --root)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -r)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --sensor)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                -s)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
    esac
}

_tio__helper__append_rpcs() {
    # Only fetch when completing a positional (RPC name), not a flag/value
    if [[ ${cur} == -* ]]; then
        return
    fi
    case "${prev}" in
        -r|--root|-s|--sensor|-t|--req-type|-T|--rep-type|--timeout)
            return
            ;;
    esac
    local rpcs
    rpcs="$(_tio__helper__list_rpcs "$@")"
    rpcs="${rpcs//$'\n'/ }" # replace newlines with spaces
    rpcs="${rpcs% }"     # remove trailing whitespace
    echo "$rpcs"
}
_tio__helper__list_rpcs() {
    local opts=()
    local next=false
    local item
    # Scan completed words; forward -r/-s/--root/--sensor
    for item in "${words[@]}"; do
        if $next; then
            opts+=( "$item" )
            next=false
        elif [[ "$item" =~ ^(--name-only|--capture-only|--root=.+|--sensor=.+|-s.+|-r.+)$ ]]; then
            opts+=( "$item" )
        elif [[ "$item" =~ ^(-r|-s|--root|--sensor)$ ]]; then
            next=true
            opts+=( "$item" )
        fi
    done
    opts+=( "$@" )
    tio rpc list "${opts[@]}" 2>/dev/null || echo '[RPC_LIST_FAILED]'
}

if [[ "${BASH_VERSINFO[0]}" -eq 4 && "${BASH_VERSINFO[1]}" -ge 4 || "${BASH_VERSINFO[0]}" -gt 4 ]]; then
    complete -F _tio -o nosort -o bashdefault -o default tio
else
    complete -F _tio -o bashdefault -o default tio
fi
