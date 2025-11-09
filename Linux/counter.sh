#!/bin/bash


read -p "Enter a positive integer: " user_number


if ! [[ "$user_number" =~ ^[0-9]+$ ]] || [ "$user_number" -le 0 ]; then
    echo "Error: Please enter a positive integer."
    exit 1
fi


for (( i=1; i<=user_number; i++ )); do
    echo "$i"
done


echo "Loop Finished"
