import csv

string = input("Enter the string: ")
list1 = list(string.split(" "))

data = list1

filename = r"/home/acts/Python/txt and csv files/user input.csv"
with open(filename, 'w') as file:
    writer = csv.writer(file)
    writer.writerows(data)
    print(data)