import re

''' Using Search() '''
# def search_regex():
#     print("\n#####Using search()#####")
#     text = "Hello World!"
#     pattern = r'World!'
#     print(f'\nOrignal text: "{text}"')
#     print(f'Pattern: {pattern}')
#     bRes = re.search(pattern,text)

#     if bRes:
#         print(f"Match found with Search(): '{bRes.group()}'")
#     else:
#         print("No Match Found")

# search_regex()

# def match_regex():
#     print("\n#####Using Match()#####")
#     text = "World! Hello"
#     pattern = r'Hello'
#     print(f'\nOrignal text: "{text}"')
#     print(f'Pattern: {pattern}')
#     match_at_begining = re.match(pattern,text)

#     if match_at_begining:
#         print(f"Match found with Match(): '{match_at_begining.group()}'")
#     else:
#         print("No Match Found")

# match_regex()


# def findall_regex():
#     print("\n#####Using Findall()#####")
#     text = "Hello World! Hello"
#     pattern = r'Hello'
#     print(f'\nOrignal text: "{text}"')
#     print(f'Pattern: {pattern}')
#     list_findall = re.findall(pattern,text)

#     if list_findall:
#         print(f"Match found with Finadall(): '{list_findall}'")
#     else:
#         print("No Match Found")

# findall_regex()


# def substitue_regex():
#     print("\n#####Using Sub()#####")
#     text = "Hello World! here"
#     print(f'\nOrignal text: "{text}"')
#     pattern = r'here'
#     replacement = "there"
#     sub_replace = re.sub(pattern, replacement, text)
#     print(f"Replaced text with Sub(): '{sub_replace}'")


# substitue_regex()













# def split_regex():
#     print("\n#####Using Split()#####")
#     text = "Hello! World Hello World! Hey there! I am Batman"
#     pattern = r'!'
#     print(f'\nOrignal text: "{text}"')
#     print(f'Pattern: {pattern}')
#     list_split = re.split(pattern,text)
#     print(f"Replaced Text with Split(): '{list_split}'")


# split_regex()





# str = input("Enter a String: ")

# def search():
#     print (f'The Inputed String is: {str}')
#     pattern = r'fox'
#     bRes = re.search(pattern, str)

#     if bRes:
#         print(f'The Patter {pattern} was found in the String {str} at {bRes.span()}')
#     else:
#         print("No Match Found")

# search()


# def findall():
#     print (f'The Inputed String is: {str}')
#     pattern = r'fox'
#     bRes = re.findall(pattern, str)

#     if bRes:
#         print(f'The Patter {pattern} was found in the String at: {bRes}')
#     else:
#         print("No Match Found")

# findall()


# # Class Example
# text = "I have three apples and two oranges"
# pattern= r'apples'
# replacement="oranges"
# sub_replace=re.sub(pattern, replacement, text)
# print(sub_replace)



# 19/09/2025

# Anchors ^(start) $(end)

# text = "Quick Brown fox jumps over the lazy dog"
# pattern=r'^Quick'
# obj_search = re.search(pattern,text)
# if obj_search:
#     print(f'strings begins with: {obj_search.group()}')
# else:
#     print("No match FOund")

# pattern = r'dog$'
# obj_search= re.search(pattern,text)
# if obj_search:
#     print(f'The string ends with: {obj_search.group()}')
# else:
#     print(f'No match found')


# Combining Patterns - Anchors

# text = "Start of text and end of line"
# pattern= r'Start.*line$'
# obj_search = re.search(pattern,text)
# if obj_search:
#     print(f'Match found: {obj_search.group()}')
# else:
#     print("No match found")

# Character Classes 

# def demonstrate_regex(regex,text):
#     print(f'\n Regular expression:{regex}')
#     print(f'Text: {text}')
#     lst_matches = re.findall(regex,text)
#     if lst_matches:
#         print(f'Match Found: {lst_matches}')
#     else:
#         print("Match Not Found!")

# text = "Hello World 123 !@# Python_3.9\nLine2"

# # 1.[a-z]:
# print("Matches any Lowercase letter: ")
# demonstrate_regex(r'[a-z]', text)

# # 2. [A-Z]:
# print("Matches any Upper case letter: ")
# demonstrate_regex(r'[A-Z]', text)

# # 3. [0-9]:
# print("Matches any digit: ")
# demonstrate_regex(r'[0-9]', text)

# # 4. \d:
# print("Matches any digit equivalent to [0-9]: ")
# demonstrate_regex(r'\d', text)

# # 5.\D:
# print("Matches any non digit characher: ")
# demonstrate_regex(r'\D',text)

# # 6.\w:
# print("Matches any word character(alphanumeric + underscore): ")
# demonstrate_regex(r'\w', text)

# # 7.\W:
# print("Matches any non-word character : ")
# demonstrate_regex(r'\W', text)

# # 8. \s:
# print("Matches any whitespace character : ")
# demonstrate_regex(r'\s', text)

# # 9. \S:
# print("Matches any non-whitespace character : ")
# demonstrate_regex(r'\S', text)






# Character Classes and Quantifiers
# text="123 abc 456"
# pattern=r'\d{3}'
# matches= re.findall(pattern,text)
# print(f'matches found{matches}')




# Practice Example substitution: 

# text = "The price is 1000 dollars and 200 dollars"
# pattern= r'\d{3}'
# replacement = "XXX"
# sub_replace= re.sub(pattern,replacement,text)
# print(f' Replaced Text: {sub_replace}')


# Qunatifires
# text = "aa aaa aaaa aaaaaa"
# pattern = r'a{2,4}' 
# lst_matches = re.findall(pattern,text)
# print(f'Matches Found: {lst_matches}')



