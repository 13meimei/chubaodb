#!/bin/bash

echo "how to use: docker run -v ds.conf:/ds.conf ansj/chubaodb:[tagname]"

case $1 in
	ds)
	/chubaodb/bin/data-server -c /ds.conf
	;;
	ms)
	/chubaodb/bin/master-server  -conf /config.toml
	;;
	proxy)
	echo "not implement"
	;;
	*)
	echo "you must set type to start [ds] [ms] or [proxy]" 
	;;
esac
