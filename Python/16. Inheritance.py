# class Animal:
#     def __init__ (self,name):
#         print("in init of Animal")
#         self.name = name
#     def speak(self):
#         return f"{self.name} makes a sound"
#     def print_name(self):
#         print("Name is: ", self.name)

# # Derived Class (subclass)
# class Dog(Animal):
#     def __init__ (self,name):
#         print("In init of Dog")
#         self.name=name
#     def speak(self):
#         return f"{self.name} says Woof!"
    

# class Cat(Animal):
#     def speak(self):
#         return f"{self.name} says Meow!"
    


# objDog = Dog("Bruno")
# objCat= Cat("Lucy")

# objDog.print_name()
# objCat.print_name()

# print(objDog.speak())
# print(objCat.speak())




# Class Assignment:
# class Org:
#     center = "Pune"
#     course = "DAI"
#     def __init__(self,center,course):
#         print(f'Calling base class init')
#         self.center=center
#         self.course=course




# class Student(Org):
#     def __init__(self,name, rno, marks, center, course):
#         print(f'Calling dervied class init')
#         self.center=center
#         self.course=course
#         self.roll_no=rno
#         self.stud_name=name
#         self.stud_marks=marks


#     def display_info(self):
        
#         print(f'''
# ______________________
# | Student Information |
# ``````````````````````
# Center  : {self.center}
# Course  : {self.course}
# Name    : {self.stud_name}
# Roll no : {self.roll_no}
# Marks   : {self.stud_marks}%
# ''')

# S1 = Student("Soham",21,85,"ACTS","HPCSA")
# S1.display_info()

# class Animal:
#     def __init__(self, habitat):
#         self.habitat = habitat

#     def print_habitat(self):
#         print(self.habitat)

#     def sound(self):
#         print("Some Animal Sound")


# class Dog(Animal):
#     def __init__(self):
#         super().__init__("Kennel")

#     def sound(self):
#         print("Woof woof!")


# x = Dog()
# x.print_habitat()
# x.sound()

class Teacher:
    def teacher_action(self):
        print("I Can Teach")

class Master_Teacher:
    def Master_action(self):
        print("I Can Code")

class Youtuber:
    def Youtuber_action(self):
        print("I Can Teach, Code and Make Videos")


class Human(Teacher, Master_Teacher, Youtuber):
    pass


H1 = Human()
H1.teacher_action()
H1.Master_action()
H1.Youtuber_action()