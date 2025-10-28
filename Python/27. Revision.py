# class Employee:
#     def __init__(self):
#         self.name=input("Enter the Name of the Employee: ")
#         self.id=int(input(f"Enter the ID of {self.name}: "))
#         self.basic=int(input(f"Enter the Basic Pay of {self.name}: "))
    
#     def calc_sal(self):
#         self.DA=self.basic * 0.10
#         self.HRA=self.basic * 0.05
#         self.Total=self.basic+self.DA+self.HRA

#     def display_Sal(self):
#         print(f'''
# Employee Details 
# ---------------------------
# Name      : {self.name}
# ID        : {self.id}
# Basic     : {self.basic}
# DA (%10)  : {self.DA}
# HRA (%5)  : {self.HRA}
# Total Sal : {self.Total}''')


# E1 = Employee()
# E1.calc_sal()
# E1.display_Sal()



# IP = "192.168.82.91"
# IP = input("Enter the IP: ")
# list = IP.split(".")
# n_list = []
# for i in range(0, len(list)):
#     num = list[i]
#     new_num = int(num)
#     n_list.append(new_num)

# count = 0
# for ip in range(0, len(n_list)):
#     if n_list[ip]  >= 0 and n_list[ip] <= 256:
#         count += 1
    
# if count >= 4:
#     print(f'The IP {IP} is a Valid IP Address')
# else: 
#     print(f'The IP {IP} is a NOT a Valid IP Address')



stud_dict = {
    'Soham' : 87,
    'Mohit'  : 64,
    'Yogesh' : 85
}

name = input("Enter a Name: ")
marks = float(input("Enter the Marks: "))

stud_dict[name] = marks
print(stud_dict)

del_name = input("Enter a Name you want to delete: ")

del stud_dict[del_name] 
print(stud_dict)

sum = 0
for value in stud_dict.values():
    sum += value

print(sum)

average = sum / len(stud_dict)

print(f' The Average is {average}')