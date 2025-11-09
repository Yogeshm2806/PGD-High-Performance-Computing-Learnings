#!/bin/bash
read -p "Enter a path: " user_path
if [ -f "$user_path" ]; then
    echo "File"
    permissions=$(stat -c "%a" "$user_path" 2>/dev/null)
    if [ -n "$permissions" ]; then
        echo "Permissions: $permissions"
    else
        echo "Could not retrieve permissions."
    fi
elif [ -d "$user_path" ]; then
    echo "Directory"
    permissions=$(stat -c "%a" "$user_path" 2>/dev/null)
    if [ -n "$permissions" ]; then
        echo "Permissions: $permissions"
    else
        echo "Could not retrieve permissions."
    fi
else
    echo "Does not exist"
fi
