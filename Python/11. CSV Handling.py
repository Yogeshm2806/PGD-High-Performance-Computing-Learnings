# import csv

# # Reading data from a csv a file:
# file = r"/home/acts/Python/sample.csv"
# with open(file, 'r') as f:
#     strData = csv.reader(f)
#     print(strData)
#     data=list(strData)


# # Print the Data
# print(data)
# print(data[0])


# # Modify the Data
# data.append(['Satyam','Prince','Rushi'])
# print(data)

# # Write the modified data back to the CSV file
# with open(file, 'w', newline='') as f:
#     writer = csv.writer(f)
#     writer.writerows(data)




# Example using the 'pyfiglet' library
# First, install it if you haven't: pip install pyfiglet
# import pyfiglet

# text = "Hello Python!"
# ascii_art = pyfiglet.figlet_format(text)
# print(ascii_art)
