#!/bin/bash

task_arr=('sleep 5' 'sleep 5' 'sleep 5' 'sleep 5' 'sleep 10')
pid_arr=()

for cmd in "${task_arr[@]}"; do
    # Execute the command directly in background
    $cmd &
    eval pid_arr+=("$!")
done

# Show jobs while they are running
echo "Currently running jobs:"
jobs -l

wait "${pid_arr[@]}"

echo "All jobs finished. PIDs were: ${pid_arr[@]}"
