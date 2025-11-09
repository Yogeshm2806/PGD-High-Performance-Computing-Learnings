#!/bin/bash

read -p "Enter a Allocated Cores: " all_core
read -p "Enter a Actual Cores: " act_core

if [ ${all_core} -le ${act_core} ];then
	echo "Job used resources within Allocation"
elif [ ${all_core} -gt ${act_core} ];then
	sub=$(($all_core - $act_core))
	echo "$sub"
	echo "Job exceeded core Allcation ${sub} cores"
	
fi
