import matplotlib.pyplot as plt
import numpy as np
import ast
import re

x = np.arange(10)
file = open("output1.csv", "r+")
xar = []
yar1 = []
yar2 = []
yar3 = []
yar4 = []
yar5 = []
firstLine = next(file)
print(firstLine)
x = firstLine.split(",")
label1 = x[1]
label2 = x[2]
label3 = x[3]
label4 = x[4]
label5 = x[5]
for line in file:
    values = line.strip().split(",")
    # print(values)
    xar.append(values[0])

    try:
        yar1.append(int(values[1]))
    except ValueError:
        yar1.append(0)
        pass
    
    try:
        yar2.append(int(values[2]))
    except ValueError:
        yar2.append(0)
        pass
    
    try:
        yar3.append(int(values[3]))
    except ValueError:
        yar3.append(0)
        pass
    
    try:
        yar4.append(int(values[4]))
    except ValueError:
        yar4.append(0)
        pass
    
    try:
        yar5.append(int(values[5]))
    except ValueError:
        yar5.append(0)
        pass


print(xar)
print(yar1)

plt.plot(xar, yar1)
plt.plot(xar, yar2)
plt.plot(xar, yar3)
plt.plot(xar, yar4)
plt.plot(xar, yar5)
# end for

plt.legend([label1, label2, label3, label4, label5], loc='upper left')
plt.show()

