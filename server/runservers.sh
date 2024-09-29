#!/bin/bash

# File to store the PIDs of the running servers
PID_FILE="server_pids.txt"
# Clear the file before starting new servers
> $PID_FILE

# Starting 5 server instances with ports ranging from 8080 to 8084
for i in {0..4}
do
    PORT=$((8080))
    ID=$i
    LOGFILE="server_${i}.log"
    
    # Running the server and saving the PID
    mvn compile exec:java -Dexec.args="$PORT $ID" > "$LOGFILE" 2>&1 &
    
    # Get the PID of the last command (the server)
    echo $! >> $PID_FILE
    
    echo "Started server with PORT=$PORT and ID=$ID, logging to $LOGFILE"
done
