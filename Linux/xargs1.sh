#!/bin/bash

# --- VARIABLES ---
# Name of the archive file on the remote machine
ARCHIVE_NAME="ayush_yogesh_backup.tgz "

# Replace with the username of the remote machine
REMOTE_USER="acts"

# Replace with the IP address or hostname of the remote machine
REMOTE_HOST="192.168.82.91"

# Path on the remote machine where the archive is located
REMOTE_PATH="/home/acts/downloads"

# Path on the local machine where the retrieved file will be saved
LOCAL_RETRIEVE_PATH="/home/acts"

# 1. Retrieve the compressed archive from the remote machine
echo "Retrieving $ARCHIVE_NAME from $REMOTE_HOST:$REMOTE_PATH..."
scp "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/$ARCHIVE_NAME" "$LOCAL_RETRIEVE_PATH/"

# 2. Decompress and extract the retrieved archive
echo "Decompressing and extracting the archive..."
tar -xzvf "$LOCAL_RETRIEVE_PATH/$ARCHIVE_NAME" -C "$LOCAL_RETRIEVE_PATH"

echo "Retrieval and extraction complete."
