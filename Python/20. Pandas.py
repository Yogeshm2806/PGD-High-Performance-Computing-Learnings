import pandas as pd



data ={
    "Name" : ['Soham','Mohit','Yogesh','Ayush','Ajinkya'],
    "Age"  : [22,23,25,23,27],
    "City" : ['Pune','Lonavla','Jalgaon','MP','Buldhana'],
    "Marks": [83,98,99,78,90],

}
obj_df=pd.DataFrame(data)
print(f'{obj_df}\n')

# View First 2 rows
print (f'First Two Rows:\n{obj_df.head(2)}\n')

# View Last 2 rows
print (f'Last Two Rows:\n{obj_df.tail(2)}\n')

# View with condition
print (f'First Two Rows where age is above than 24:\n{obj_df[obj_df['Age']>24].head(2)}\n')

# View descriptive Statistics
print (f'Descriptive Statistics:\n{obj_df.describe()}\n')
print (f'Descriptive Statistics of all columns:\n{obj_df.describe(include="all")}\n')


