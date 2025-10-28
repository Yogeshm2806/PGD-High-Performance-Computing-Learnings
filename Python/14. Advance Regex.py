import re

# Class Assignment 1

# text="Quick Brown fox jumps over lazy dog\nLAzy Brown Fox Jumps over Lazy Dog\nQuick Brown Fox Jumps over Lazy Horse\nSpeedy Brown fox jumps over lazy zebra\nAgile Brown Fox jumps over Quick Cat\nQuick Brown cat jumps over Lazy Dog"
# list1=(text.split("\n"))
# print(list1)
# pattern = r'^Quick.*'
# for ch in list1:
#     result=re.search(pattern,ch)
#     if result:
#         filename = "/home/acts/Desktop/simple.txt"
#         with open(filename, 'a') as file:
            
#             file.write(f'Result: {ch}')
#             file.write(f'\n')
#         print(f'Match Found{ch}')
#     else:
#         print("No Match")
    
# Class Assignment 2

text = "I wish to work with:Google123,apple,123meta##,Amazon,X,Microsoft..,Tesla,Nasa(),ISRO"
list1=(text.split(":"))
list2=(list1[1].split(","))
print(list2)
pattern = r'^[A-Za-z]*[A-Za-z]$'
for i in list2:
    res = re.search(pattern,i)
    if res:
        print(f'results: {i}')
    else:
        print("None")
