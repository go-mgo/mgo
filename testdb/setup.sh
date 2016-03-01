#!/bin/sh -e

LINE="---------------"

start() {
    mkdir _testdb
    cd _testdb
    echo keyfile > keyfile
    chmod 600 keyfile
    cp ../testdb/server.pem server.pem
    cp -a ../testdb/daemons .
    if ! mongod --help | grep -q -- --ssl; then
        rm -rf daemons/db3
    fi
    COUNT=$(ls daemons | wc -l)
    echo "Running daemons..."
    svscan daemons &
    SVSCANPID=$!
    echo $SVSCANPID > svscan.pid
    if ! kill -0 $SVSCANPID; then
        echo "Cannot execute svscan."
        exit 1
    fi
    echo "Starting $COUNT processes..."
    for i in $(seq 30); do
        UP=$(svstat daemons/* | grep ' up ' | grep -v ' [0-3] seconds' | wc -l)
        echo "$UP processes up..."
        if [ x$COUNT = x$UP ]; then
            echo "Running setup.js with mongo..."
            mongo --nodb ../testdb/init.js
            exit 0
        fi
        sleep 1
    done
    echo "Failed to start processes. svstat _testdb/daemons/* output:"
    echo $LINE
    svstat daemons/*
    echo $LINE
    for DAEMON in daemons/*; do
        if $(svstat $DAEMON | grep ' up ' | grep ' [0-3] seconds' > /dev/null); then
            echo "Logs for _testdb/$DAEMON:"
            echo $LINE
            cat $DAEMON/log/log.txt
            echo $LINE
        fi
    done
    exit 1
}

stop() {
    if [ -d _testdb ]; then
        cd _testdb
        if [ -f svscan.pid ]; then
            kill -9 $(cat svscan.pid) 2> /dev/null || true
            svc -dx daemons/* daemons/*/log > /dev/null 2>&1 || true
            COUNT=$(ls daemons | wc -l)
            echo "Shutting down $COUNT processes..."
            while true; do
                DOWN=$(svstat daemons/* | grep 'supervise not running' | wc -l)
                echo "$DOWN processes down..."
                if [ x$DOWN = x$COUNT ]; then
                    break
                fi
                sleep 1
            done
            rm svscan.pid
            echo "Done."
        fi
        cd ..
        rm -rf _testdb
    fi
}


if [ ! -f suite_test.go ]; then
    echo "This script must be run from within the source directory."
    exit 1
fi

case "$1" in

    start)
        start $2
        ;;

    stop)
        stop $2
        ;;

esac

# vim:ts=4:sw=4:et
