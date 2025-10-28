filename= "/home/acts/Desktop/simplee.txt"

try:
    with open(filename, "r") as file:
        response = file.read()
        print(response)
except FileNotFoundError:
        response = "Error: fILE nOT fOUND."
        print (response)
except Exception as e:
    response = f"ERROR: {str(e)}"
    print (response)
finally:
    print("In finally")