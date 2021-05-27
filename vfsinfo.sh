#! /usr/bin/env bash
set -u
tool_dir="$(dirname "$(realpath "$0")")"

# enter python virtual environment
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
export PIPENV_VENV_IN_PROJECT=1
PYTHONPATH="$(dirname "$(realpath "$tool_dir")")"${PYTHONPATH:+":$PYTHONPATH"}
export PYTHONPATH

# run scripts
cd "$tool_dir" || exit

pipenv run python vfsinfo.py "$@" \
   --setting ~/lib/Computer/Languages/Python/site-packages/my_settings/settings.json ||
   { echo "execution failed."; exit; }
