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

# Our archivers use both webkit and chromium, so make sure each is installed.
# A browser may have several install locations (e.g. chromium also needs ffmpeg and
# a headless shell), and each is a directory, so install if any of them is missing.
for browser in webkit chromium; do
    install_paths=`playwright install $browser --dry-run |grep "Install location" |sed 's/^[ ]*Install location:[ ]*//'`

    missing=0
    while IFS= read -r install_path; do
        if [ ! -d "$install_path" ]; then missing=1; fi
    done <<< "$install_paths"

    if [ "$missing" -eq 1 ]; then playwright install $browser; fi
done

# If playwright archivers (e.g. ferc2, ferc714) still won't run on your machine, try
# playwright install --with-deps webkit chromium
