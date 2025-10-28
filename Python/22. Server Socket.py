import socket

# Step 1: Create a Socket (IPv4 + TCP)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Step 2: Bind to localhost and port 12345
server_socket.bind(('localhost', 12345))

# Step 3: Put the socket into listening mode
server_socket.listen(1)
print("Server is listening on port 12345....")

# Step 4: Accept a connection
client_socket, client_address = server_socket.accept()
print(f'Connected to client at {client_address}')

# Step 5: Send a Welcome message to the Clien
welcome_message = "Welcome to the Server😀😀😀"
client_socket.send(welcome_message.encode()) #Encoding string to bytes

# Step 6: Close Sockets
client_socket.close()
server_socket.close()