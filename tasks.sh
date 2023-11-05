#!/bin/bash
set -e -u
ENV="${ENV:-dev}"

_load_env() {
    source env/${ENV}
}

_activate_venv() {
    _load_env
    source .venv/bin/activate
}

install() {
    if [ ! -d .venv ]; then
        python3.12 -m venv .venv
    fi

    _activate_venv
    pip install -e ".[$ENV]" "$@"
}

cli() {
    _activate_venv
    python -m dbk.cli "$@"
}

tui() {
    _activate_venv
    python -m dbk.tui "$@"
}

test() {
    _activate_venv
    pytest "$@" tests
}

"$@"
