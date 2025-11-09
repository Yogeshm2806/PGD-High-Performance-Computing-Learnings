#!/bin/bash
echo "
1. Make a file. 
2. Display contents
3. Copy the file
4. Rename the file
5. Delete the file
6. Exit"

read -p "Enter your option: " men_opt

if [[ ${men_opt} == 1 ]]
then
        read -p "Enter the File name: " f_name
        if [[ -e ${f_name} ]]
                then
                        echo " ${f_name} File already exists"
        else touch ${f_name}
                echo "Done"
        fi


elif [[ ${men_opt} == 2 ]]
then
        read -p "Enter the File name: " f_name
        if [[ -e ${f_name} ]]
        then
                cat ${f_name}
        else
                echo "File Does Not Exist"
        fi

elif [[ ${men_opt}  == 3 ]]
then
        read -p "Enter the Source File name: " sor_name
        if [[ -e ${sor_name} ]] && [[ -r ${sor_name} ]]
                then
                        read -p "Enter the Target File name: " tar_name
                        if [[ ! -e ${tar_name} ]]
                        then
                                cp ${sor_name} ${tar_name}
                                echo "The Target file content is"

                        else
                                echo "The Target File Already Exists."

                        fi
        else
                echo "File Does not exist."
        fi

elif [[ ${men_opt}  == 4 ]]
then
        read -p "Enter the Source File name: " sor_name
        if [[ -e ${sor_name} ]] && [[ -r ${sor_name} ]]
                then
                        read -p "Enter the Target File name: " tar_name
                        if [[ ! -e ${tar_name} ]]
                        then
                                mv ${sor_name} ${tar_name}
                                echo "The Target file content is"
                                cat ${tar_name}
                        else
                                echo "The Target File Already Exists."
                                cat ${tar_name}
                        fi
        else
                echo "File Does not exist."
        fi



elif [[ ${men_opt}  == 5 ]]
then
        read -p "Enter the File name You Want to Delete: " del_name
        if [[ -e ${del_name} ]]
                then
                        if [[ -w ${del_name} ]]
                        then
                                read -p "Are you Sure you want to delete this file: (Y/n) " user_perm

                                if [[ ${user_perm} == "Y" ]]
                                then
                                        rm ${del_name}
                                else
                                        echo "File is not Deleted"

                                fi
                        fi

        else
                echo "File Does not Exist..."


        fi

elif [[ ${men_opt} == 6 ]]
then
        echo "EXITED SUCCESSFULLY"

        exit


else
        echo "Option is Invalid"

fi

