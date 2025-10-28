import socket

# Step 1: Create a Socket (IPv4 + TCP)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Step 2: Bind to localhost and port 12345
server_socket.bind(('localhost', 12396))

# Step 3: Put the socket into listening mode
server_socket.listen(1)
print("Server is listening on port 12345....")

# Step 4: Accept a connection
client_socket, client_address = server_socket.accept()
print(f'Connected to client at {client_address}')

# Step 5: Send a Welcome message to the Clien
filename = client_socket.recv(1024).decode()
print(f'Requested file: {filename}') #Encoding string to bytes

try:
    with open(filename, 'r') as file:
        response = file.read()
except FileNotFoundError:
    response = "Error: File Not Found."
except Exception as e:
    response = f"ERROR: {str(e)}"


client_socket.send(response.encode())
client_socket.close()
server_socket.close()