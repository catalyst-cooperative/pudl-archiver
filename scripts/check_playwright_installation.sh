#!/bin/bash
# Checks if playwright browser is installed. If not, installs it.

# `playwright install --dry-run` prints out a block of config information
# that looks like this:
#
# $ playwright install webkit --dry-run
# browser: webkit version 18.4
#   Install location:    /[path/to]/ms-playwright/webkit-2158
#   Download url:        https://cdn.playwright.dev/dbazure/down[...]ac-15-arm64.zip
#   Download fallback 1: https://playwright.download.prss.micros[...]ac-15-arm64.zip
#   Download fallback 2: https://cdn.playwright.dev/builds/webki[...]ac-15-arm64.zip
#
# There doesn't seem to be another way to get the path where playwright expects
# the browser to be. So, we make do:
#   1. Grab the output
#   2. Throw away everything but the "Install location" line
#   3. Strip out everything but the path
#
# It's not clear whether the path is always absolute or may sometimes be relative,
# so we stay laser-focused on just the "Install location" label and the whitespace
# around it.

install_path=`playwright install webkit --dry-run |grep "Install location" |sed 's/^[ ]*Install location:[ ]*//'`

# Check if the install path is a regular file. If so, we're done.
if [ -f "$install_path" ]; then exit 0; fi

# If not, install.

playwright install webkit

# If playwright archivers (e.g. ferc2, ferc714) still won't run on your machine, try
# playwright install --with-deps webkit
