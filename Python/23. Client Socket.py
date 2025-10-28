import socket

# Step 1: Create a Socket (Ipv4 + TCP)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Step 2: Connect to the Server
client_socket.connect(('192.168.82.169', 12355))

# Step 3: Receive data from the Server
message = client_socket.recv(1024) # Buffer size 1024 bytes
print("Message from Server: ", message.decode()) # Decode bytes to string

# Step 4: Close the Socket
client_socket.close()