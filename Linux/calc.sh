#!/bin/bash


if [ $# -ne 3 ]; then
    echo "Usage: $0 <number1> <operator> <number2>"
    exit 1
fi


num1=$1
operator=$2
num2=$3


if [ "$operator" != "+" ] && [ "$operator" != "-" ]; then
    echo "Error: Invalid operator. Use + or -"
    exit 1
fi


if ! [[ "$num1" =~ ^-?[0-9]+$ ]]; then
    echo "Error: First number is not an integer."
    exit 1
fi


if ! [[ "$num2" =~ ^-?[0-9]+$ ]]; then
    echo "Error: Second number is not an integer."
    exit 1
fi


if [ "$operator" == "+" ]; then
    result=$((num1 + num2))
else
    result=$((num1 - num2))
fi

echo "Result: $result"
