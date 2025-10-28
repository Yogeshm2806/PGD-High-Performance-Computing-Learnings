import socket

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(('localhost', 12396))

# Step 1: Send Filename
filename = input("Enter the Filename: ")
client_socket.send(filename.encode())

response = client_socket.recv(4096).decode()

print(response)
client_socket.close()