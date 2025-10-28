# Reading a File

filename = r"text.txt"
with open(filename, 'r') as text:
    contents = text.read()
print("Content of the txt file is :\n", contents)



# # Appending the text in a file

with open(filename,'a') as txt:
    txt.write("\n This is appended Line")


# Reading a appended file and Splitting the text
with open(filename, 'r') as txtfile:
    contents=txtfile.read()
    split_text=contents.split(" ")
print("Contents of txt file after updation: ", contents)
print("Contents of txt file after spliting: ", split_text)
