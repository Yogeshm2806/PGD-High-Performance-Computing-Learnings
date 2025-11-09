#!/bin/bash
counter=0
while [[ ${counter} -le 1 ]];do
case ${1} in
        "-d")
                domain=${2}
                if [[ -z ${domain} ]];then
                        echo "Error: Domain is required, use -h/--help"
                        exit 128
                fi
                counter=$((counter + 1))
                ;;
        "-w")
                wordlist=${2}
                if [[ -z ${wordlist} ]];then
                        echo "Error: Wordlist is required, use -h/--help"
                        exit 128
                fi
                counter=$((counter + 1))
                ;;
        "-h"|"--help")
                echo "SubDomain Identifier"
                echo "--------------------"
                echo "-d DOMAIN : Domain is required"
                echo "-h/--help : Print help"
                exit 0
                ;;
        *)
                echo "Error invalid argument: ${1}, Use -h/--help"
                exit 127
                ;;
esac
shift 2
done
echo "working......"
