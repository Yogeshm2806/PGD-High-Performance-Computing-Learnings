#!/bin/bash


case ${1} in
	"-d")
		domain=${2}
		if [[ -z ${domain} ]]
		then
			echo"domain is req"
			exit
		fi

	;;

	"-w")
		sub_dom=${3}
		if [[ -z ${sub_dom} ]]
		then
			echo "sub is req"
			exit
			fi
		

		while read sub; do
			if host "${sub}.${domain}" &> /dev/null; then
				echo "${sub}.${domain}"
			fi
		done
		;;

	

	"-h" | "--help")
		echo "This is help"

		;;

	*)
		echo "Invalid Args"

		;;

esac
