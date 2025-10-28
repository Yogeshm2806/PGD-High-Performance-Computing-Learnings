ip = str(input("Enter the IP: "))
cidr = int(input("Enter the CIDR value: "))

ip = ip.split(".")

IP = []

for i in range(0, len(ip)):
    IP.append(int(ip[i]))

def check_for_class_A():
    if 0 <= IP[0] <= 127:
        if cidr > 8: #11
            nw = cidr - 8 # 11 - 8 = 3
            network = 2 ** nw #2^3 = 8
            hst = 8 - nw #2^5 = 32
            host = (2 ** hst) * (2 ** 8) * (2 ** 8)
            subnet = 256 - (2**hst)
            print(f'N/W = {network}')
            print(f'Host = {host}')
            print(f'Subnet: {subnet}')
            print("Class A")
            start = 0
            stop = network
            for r in range(1, 4):
                            print(f'Range {r}')
                            for i in range(start, stop):
                                if i > 255:
                                    # Prevent invalid octet beyond 255
                                    break
                                for j in range(0, 256):
                                    for k in range(0, 256):
                                        if i == start and j == 0 and k == 0:
                                            print(f'Network : {IP[0]}.{i}.{j}.{k}')  
                                        elif i == start and j == 0 and k == 1:
                                            print(f'Gateway : {IP[0]}.{i}.{j}.{k}')
                                        elif i == stop - 1 and j == 255 and k == 255:
                                            print(f'Broadcast : {IP[0]}.{i}.{j}.{k}')
                            start, stop = stop, stop * 2
                            if start > 255:
                                # Prevent further ranges with invalid octets
                                break

def check_for_class_B():
    if cidr > 16: 
        nw = cidr - 16 
        nw
        network = 2 ** nw 
        network
        hst = 8 - nw 
        hst
        host = hst + 8  
        hosts = 2 **host
        subnet = (256 - ((2 ** hst) - 1) - 1)
        print(f'N/W = {network}')
        print(f'Host = {hosts}') 
        print(f'Sub: {subnet}')
        start = 0
        stop = (2 ** hst)
        for r in range(1, 4):
            print(f'Range {r}')
            for i in range(start, stop):
                for j in range(0, 256):
                    if i == start and j == 0:
                        print(f'Network : {IP[0]}.{IP[1]}.{i}.{j}')
                    elif i == start and j == 1:
                        print(f'Gateway : {IP[0]}.{IP[1]}.{i}.{j}')
                    elif i == stop - 1 and j == 255:
                        print(f'Broadcast : {IP[0]}.{IP[1]}.{i}.{j}')
            start, stop = stop, stop * 2



def check_for_class_C():
    if cidr > 24:
                nw = cidr - 24
                hst = 8 - nw
                network = 2 ** nw
                host = 2 ** hst
                print(f'N/W = {network}')
                print(f'Host = {host}') 
                start = 0
                stop = host
                subnet = 256 - host
                print(f'255.255.255.{subnet}')
                for r in range(1, 4):
                    print(f'Range {r}')
                    for i in range(start, stop+1):
                        if i == start:
                            print(f'Network Address : {IP[0]}.{IP[1]}.{IP[2]}.{i}')
                        if i == start+1:
                            print(f'Gateway Address : {IP[0]}.{IP[1]}.{IP[2]}.{i}')
                        if i == stop:
                            print(f'Broadcast Address : {IP[0]}.{IP[1]}.{IP[2]}.{i-1}')
                    start, stop = stop, stop + host







def check_class():
    if 0 <= IP[0] <= 127:
        check_for_class_A()
    elif 128 <= IP[0] <= 191:
        check_for_class_B()
    elif 192 <= IP[0] <= 223:
         check_for_class_C()
    elif 224 <= IP[0] <= 239:
            print("Class D")
    elif 240 <= IP[0] <= 255:
        print("Class E")
    elif 0 > IP[0] > 255:
        print("INVALID IP....")


check_class()