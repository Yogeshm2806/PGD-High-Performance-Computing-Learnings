# from abc import ABC, abstractmethod

# class Animal(ABC):
#     @abstractmethod
#     def sound(self):
#         pass

# class Dog(Animal):
#     def sound(self):
#         return "Woof!"
    
# class Cat(Animal):
#     def sound(self):
#         return "Meow!"
    
# objDog = Dog()
# print(objDog.sound())




class Students:
    Rollno = 0
    Name = "" 
    Marks = []
    percentage = 0


    def __init__(self, Rollno, Name, Marks):
        self.rollno = Rollno
        self.name = Name
        self.marks = Marks

    def calc_perc(self):
        for i in range(3):
            m = int(input("Enter the Marks: "))
            self.marks.append(m)


        print(self.marks)
        sum = 0
        for i in range(0, len(self.marks)):
            sum = sum + self.marks[i]
        

        self.percentage = (sum / 300) * 100

        # print(percent)


    
    def get_details(self):
        print(f'''
Students Information
---------------------
Name       : {self.name}
PRN        : {self.rollno}
Percentage : {self.percentage}%
\n''')
    



if __name__ == '__main__': 
    std1 = Students(250840127021,"Soham",)


    std1.calc_perc()
    std1.get_details()


# marks = []

# for i in range(3):
#     m = int(input("Enter the Marks: "))
#     marks.append(m)


# print(marks)



