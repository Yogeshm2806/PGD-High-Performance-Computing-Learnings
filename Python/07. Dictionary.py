my_dict = {
    'integer_val' : 42,
    'string_val'  : "This is a String",
    'tuple_val'   : (1,2,3),
    'list_val'    : ["a", "list", "of", "strings"],
    'nested_dict' : {"key1": "value1", "key2": 2}
}

# print("Orignal Dictionary: ", my_dict)
# print("Element for key 'List_Value': ", my_dict['list_val'])


# Removing Entries  by POP (It returns a values which has been removed.)
# a = my_dict.pop('integer_val')
# print(a)
# print(my_dict)


# Delete Enteries by Del (It directly deleted the value.)
# del my_dict['tuple_val']
# print(my_dict)


# Extracting the Keys
# for key in my_dict:
#     print(key)


# Extracting the Values
# for values in my_dict.values():
#     print(values)


# print(my_dict.keys())
# print(my_dict.values())


# Printing Both Keys and Values using (dict.items())
# for key, value in my_dict.items():
#     print(f'{key}:{value}')
    
    
# Adding Elements in a Dictionary

# my_dict['a'] = 69
# print(my_dict)


''''
WAP to hold Key as Roll no & values as a Name.

'''

# hpcsa = {
#     '001' : 'Bhand',
#     '002' : 'Zine',
#     '003' : 'Hongekar',
#     '004' : 'Dewangan',
#     '011' : 'Mahajan',
#     '012' : 'Pawaskar',
#     '021' : 'Bhamare'
# }

# print ('Roll no    Names')
# print("-------------------")
# for key, value in hpcsa.items():
#     print(f' {key}      {value}')

values=input("Please Enter Values")
keys=input("Please Enter Keys")

hpcsa={keys:values}
print(hpcsa)


'''
Create a Dictionary for an inventory shop with key value pair as,
product and its respective prices and calculate the total cost of Dictionary
'''
# inventory_shop = {
#     "Pen" : 100,
#     "Pencil": 50,
#     "Books": 150,
#     "Compass": 120,
#     "Erazer" : 60,
#     "Sharpner" : 30,
#     "Scale": 20}

# sum=0
# for values in inventory_shop.values():
#     sum+=values
# print (f'The total sum of products in the dictionary is : {sum} ')

