#!/usr/bin/env python3
"""
Suitable for parsing output of an `strace` invocation, such as
`strace -ff -x -s 65536 -e trace=read,write -o /tmp/out`
"""


import re
import sys

files = {}

for line in sys.stdin:
    # read(3, "\x00\x00\x00\x33", 4)          = 4
    name = None
    body = None
    m = re.search(r'^read\((\d+), "([^"]+)"', line)
    if m:
        name = f'/tmp/r_{m.group(1)}.hex'
        body = m.group(2)
    m = re.search(r'^write\((\d+), "([^"]+)"', line)
    if m:
        name = f'/tmp/w_{m.group(1)}.hex'
        body = m.group(2)
    if name is not None:
        body = body.replace('\\x', '')
        if name not in files:
            files[name] = open(name, "w")
        files[name].write(body)
        files[name].write("\n")
