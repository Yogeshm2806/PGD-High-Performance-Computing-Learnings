#!/bin/bash
case ${1} in
        "-d")
                domain=${2}
		if [[ -z ${2} ]]; then
			echo "Error : Please Provide Domain."
		exit 
		fi
		;;

	"-w")
		sub=${3}
		if [[ -z ${3} ]]; then
                        echo "Error : Please Provide Wordlist."
                exit
		fi

                while read sub;do
                        if host "${sub}.${domain}" &> /dev/null;then
                                echo "${sub}.${domain}"
                        fi
		done
                ;;
        "-h"|"--help")
                echo "SubDomain Identifier"
                echo "--------------------"
                echo "-d DOMAIN : Domain is required"
                echo "-h/--help : Print help"
                ;;
        *)
                echo "Error invalid argument: ${1}, Use -h/--help"
                exit 127
                ;;
esac
