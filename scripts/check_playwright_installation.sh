#!/bin/bash

install_path=`playwright install webkit --dry-run |grep "Install location" |sed 's/^[ ]*Install location:[ ]*//'`
if [ -f "$install_path" ]; then exit 0; fi
playwright install --with-deps webkit
