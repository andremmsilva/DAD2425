#!/bin/bash

# File where the PIDs are stored
PID_FILE="server_pids.txt"

if [ -f $PID_FILE ]; then
    # Read the PIDs from the file and kill each process
    while IFS= read -r PID
    do
        echo "Stopping server with PID $PID"
        kill $PID
    done < "$PID_FILE"
    
    # Optionally, remove the PID file after stopping all processes
    rm $PID_FILE
else
    echo "No PID file found. Are the servers running?"
fi
