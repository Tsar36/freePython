print("Hello from Python")
print("This is the first python script, just for check.")

list(range(1, 10, 2))
#--------------------------------------------------------
i = 0
if i == 45:
    print('i is 45')
elif i == 35:
    print('i is 35')
elif i > 10:
    print('i is greater than 10')
elif i%3 == 0:
    print('i is a multiple of 3')
else:
    print("I don't know much about i...")

#--------------------------------------------------------
'''
"for loop"
“We repeat this code 10 times, each time assigning the variable i to the next number in the sequence of 
integers from 0–9. for loops can be used to iterate through any of the Python sequence types.”
'''

for i in range(10):
    x = i * 2
    print(x)
# -------------------------------------------------------
for i in range(6):
    if i == 3:
        continue    # “The continue statement skips a step in a loop, jumping to the next item in the sequence:”
    print(i)