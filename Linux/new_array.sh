#!/bin/bash



declare -A array='([a2]="2" [a1]="1" [zz]="Hello World" [b1]="bbb" [f50]="abcd" )'

### To print keys

echo "${!array[@]}"

echo "${array[@]}"

for i in ${!array[@]};do
	echo "Key: ${i}: ${array[${i}]}"
done	
