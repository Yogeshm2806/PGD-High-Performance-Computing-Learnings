# Defining a Function
# def describe_pet(animal_type):
#     print(f'I have a {animal_type}. ')

# describe_pet("cat")




################ Using Named Arguements ######################
# def describe_pet(animal_type, pet_name):
#     print(f'I have a {animal_type} named {pet_name}')

# describe_pet("cat", "baji")


# Calculate area of rectangle using function
# def area (len, wid):
#     area= len * wid
#     print(f'Area of Rectangle is : {area:.9f}')
#     print(f'Area of Rectangle is : {type(area)}')
# area(9,3.7)



# Multiple Return Values

def calculate_area_and_perimeter (len, wid):
    area= len * wid
    perimeter= 2*(len+wid)
    return area, perimeter

# ret_val= calculate_area_and_perimeter(5,3)
# print(ret_val)
# print(f'Return Type: {type(ret_val)}')


# # Unpacking tuple of returned values
# ret_val= calculate_area_and_perimeter(5,3)
# iArea, iPerimeter = ret_val
# print(f' Area After Unpacking: {iArea}')
# print(f' Perimeter After Unpacking: {iPerimeter}')



# # No return value
# def noRetvalues():
#     print ("In func noRetVal")
# val = noRetvalues()
# print ("When no valueis explictly returned a ", type(val))
# print(type(noRetvalues()))

#################################### ARGUMENTS ###########################################################
# *args
# def add_numbers(*args):
#     print("Type of Args :", type(args))
#     print("First element of args tuple: ", args[0])
#     return sum(args) 
# Positional Arguments - With a varaible no of arguments
# print(add_numbers(1,2,3))
# print(add_numbers(4,5,6,7))


# **kwargs with key value pair argument
# def display_info(**keywords):
#     print(type(keywords))
#     print(keywords)
#     for key, value in keywords.items():
#           print(f'{key}:{value}')


# # **kwargs - with key value pair arguments
# display_info(animal="Cat", name="Baji")
# display_info(country="India", characterstics="Unity")


'''

Write a Function that takes 10 args and print the max value.

'''

def max_val(*args):
    print(f'The max value in all of them is {max(args)}')

max_val(1,2,3,4,5,6,7,8,9,0)

