strvar = "Hello World!"
#print (strvar)


# for iterrating through string (Values)
# for charvar in strvar:
#     print (charvar)


# for iterarting throuh string (index)
# for iIndex in range (0, len(strvar)):
#     print (strvar[iIndex])

# # for reversing the string: 
# for charvar in range (len(strvar)-1, -1, -1):
#     print (strvar[charvar])


# 
# str1 = " Quick                 Brown Fox"
# str2 = " Jumps Over the lazy dog"

# result = str1 +str2
# print (result)
#print (result *3)

#escape_seq = "Hi weather is \'good\'!\t have a good \n day!!"

# Strip()
# str1 = "   ---***Hello World***---   "
# stripped_string = str1.strip()
# print(str1.count("H"))
# print (stripped_string)
# # Left Strip
# left_strip=str1.lstrip("-*")
# print("The Left Strip : ", left_strip)

# # Right Strip
# right_strip=str1.rstrip("-*")
# print("The Right Strip : ", right_strip)

# # Both Strip
# both_strip=str1.strip("-*")
# print("The Both Strip : ", both_strip)



# # Find and Replace
# result = strvar + "abc" + "abc" + "abc"
# print ("\nOriginal String : ", result)
# position = result.find("abc")
# print("Position of string 'abc' : ", position)
# new_string = result.replace("abc","ABC")
# print("String with replaced value", new_string)


# # String Membership
# result = strvar
# print ("Original String",result)
# if ("World" in result):
#     print ("String Found")


# # String Comparison 
# str1= "Hello World"
# str2= "Hello World!"
# if (str1==str2):
#     print("Both strings are same")
# else:
#     print("Strings are not same")



# # Case Conversion
# result = "ABCabc"
# print ("Original String: ", result)
# print ("Converted to lower case: ",result.lower())
# print ("Converted to upper case: ",result.upper())
# print ("Converted to title case: ",result.title())



# #
# result = "ABCDEFGH"
# print("Original String: ",result)
# # Indexing
# firstchar=result[0]
# print(firstchar)

# lastchar=result[-1]
# print(lastchar)

# secondchar=result[1]
# print(secondchar)

# # Slicing 
# result = "ABCDEFGH"
# print("Original String: ",result)
# substring = result[0:4]
# print(substring)
# substring = result[:4]
# print(substring)
# substring = result[4:]
# print(substring)
# substring = result[::-1]
# print(substring)


# # Splitting
# resConcatenate = "abcd" + ',' + "efgh"
# print ("Conacateneted Strings: ",resConcatenate)
# print (type(resConcatenate))

# resSplit = resConcatenate.split(',')
# print(type(resSplit))
# print("After Splitting",resSplit)


# # Sort Strings
# str = "start"
# str = sorted(str)
# print(str)


# # Joining
# fruits =["apple","Banana","Cherry","Date"]
# result = "😀".join(fruits)
# print(result)


# # String Formatting 
# str1 = "Sham and ram are good friends"
# words= str1.split(" ")
# print(words[0],words[-1])