#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# Excellent uncrustify script from the apache cordova-ios project
#
SCRIPT_PATH=$(dirname $0)
CONFIG="$SCRIPT_PATH/uncrustify/uncrustify.cfg"

function Usage() {
    echo "Formats code using format_src.sh."
    echo "Usage: scripts/format_src.sh --changed         # Runs on changed (staged or not) files"
    echo "       scripts/format_src.sh --staged          # Runs on staged files"
    echo "       scripts/format_src.sh --all             # Runs on all source files under the current directory"
    echo "       scripts/format_src.sh --check-file file # Returns 1 if the given file requires changes, 0 otherwise."
    echo "       scripts/format_src.sh files             # Runs on the given files"
    exit 1
}

function VerifyEnv() {
    if ! which uncrustify > /dev/null; then
        echo "uncrustify binary not found. Please ensure that it is in your PATH."
        exit 1
    fi
}

function FilterFileList() {
    for f in "$@"; do
        # Filter out deleted files.
        [[ ! -e "$f" ]] && continue

        # Filter out third-party sources.
        [[ "$f" == *thirdparty* ]] && continue

        # Only use cc and h files
        [[ "$f" == *.cc ]] && echo $f
        [[ "$f" == *.h ]] && echo $f
    done
}

function FilterAndRun() {
    files=$(FilterFileList "$@")

    if [[ -z "$files" ]]; then
        echo No files to uncrustify.
        exit 2
    else
        echo "$files" | xargs uncrustify -l CPP --no-backup -c "$CONFIG"
    fi
}

if [[ "$1" = "--changed" ]]; then
    VerifyEnv
    files=$(git status --porcelain | sed s:...::)
    FilterAndRun $files
elif [[ "$1" = "--staged" ]]; then
    VerifyEnv
    files=$(git diff --cached --name-only)
    FilterAndRun $files
elif [[ "$1" = "--all" ]]; then
    VerifyEnv
    files=$(find .)
    FilterAndRun $files
elif [[ "$1" = "--check-file" ]]; then
    uncrustify -q -l CPP -c "$CONFIG" -f "$2" | cmp --quiet - "$2"
elif [[ "$1" = "--filter" ]]; then
    FilterFileList "$@"
elif [[ "$1" = -* ]]; then
    Usage
else
    VerifyEnv
    FilterAndRun "$@"
fi
