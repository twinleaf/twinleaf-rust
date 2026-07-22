#!/usr/bin/env bash
# This should be run after any changes to completions or CLI
# It will make sure the new completion scripts generate properly, and update the repo with them
# You can use --skip-initial-build if there are no changes to the CLI itself

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TIO="$SCRIPT_DIR/../target/debug/tio"
COMP="$SCRIPT_DIR/completion-scripts/tio_completions"
TESTS="$SCRIPT_DIR/completion-scripts/test_dynamic_completions"

# Ensure the just-built tio is preferred over any other on PATH (tests invoke `tio`)
export PATH="$(cd "$(dirname "$TIO")" && pwd):$PATH"

# Overkill option handler
commit=false skip_initial_build=false bad_opt=false
for i in "$@"; do
	case $i in
		-c|--commit)
			commit=true
			;;
		-s|--skip-initial-build)
			skip_initial_build=true
			;;
		-*|--*)
			bad_opt=true
			echo "Unknown option $i" >&2
			;;
		*)
			;;
	esac
done

if $bad_opt; then
	exit 1
fi

if ! $skip_initial_build; then
	echo "Initial build..."
	cargo build
fi

# generate static scripts
echo "Generating static scripts"
"$TIO" completions --static bash > "$COMP"_static.bash
"$TIO" completions --static zsh > "$COMP"_static.zsh
echo "Re-build..."
cargo build # build again to embed static scripts

# generate dynamic scripts
echo "Generating dynamic scripts"
"$TIO" completions bash > "$COMP"_dynamic.bash
"$TIO" completions zsh > "$COMP"_dynamic.zsh

# ensure generated scripts work in bash / zsh
echo "Testing bash scripts"
bash -n "$COMP"_dynamic.bash
bash "$TESTS".bash >/dev/null && echo "Passed!" # fails go to stderr

echo "Testing zsh scripts"
zsh -n "$COMP"_dynamic.zsh
zsh "$TESTS".zsh >/dev/null && echo "Passed!" # fails go to stderr

# optional autocommit
if $commit; then
	echo "Creating git commit"
	git add "$COMP"*
	git commit -m "Chore: Regenerate completion scripts"
fi
