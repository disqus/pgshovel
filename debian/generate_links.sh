#!/bin/bash
cd ../
python setup.py install -n | sed 's,^Installing \(pgshovel-.*\) script.*,/usr/share/python/pgshovel/bin/\1 /usr/bin/\1,;tx;d;:x' | sort > debian/links
