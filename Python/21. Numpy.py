import numpy as np

# Creating a 1D Array
# array_1d = np.array([1, 2, 3, 4])
# print(f'1D Array: {array_1d}')


# # Creating an Array with values from 0 to 9 
# range_array = np.arange(0,11)
# print(f'Array with Range: {range_array}')


# # Creating a 2D Array
array_2d = np.arange(1,13).reshape(3,4)
print(f"Orignal 2D Array:\n{array_2d}")
print(f'Shape of Orignal Array: {array_2d.shape}')


# Transposing the Array using (.T)
transposed_array = array_2d.T
print(f"Transposed Array (Using .T) :\n{transposed_array}")
print(f'Shape of Transposed Array: {transposed_array.shape}')


# Transposing the Array using transpose()
array_2d_2 = np.arange(20,28).reshape(4,2)
print(f'Original 2D Array:\n{array_2d_2}\n')


transposed_array_2= np.transpose(array_2d_2)
print(f'Transposed Array (using transpose()):\n{transposed_array_2}\n')

print(f'Tanspose using .T:\n{array_2d_2.T}\n')





