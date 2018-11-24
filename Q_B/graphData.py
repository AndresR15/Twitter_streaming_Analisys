import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from datetime import timedelta
import time
import ast
import re

x = np.arange(10)
file = open("output.csv", "r+")
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
startTime = None
FMT = '%H:%M:%S'
for i, line in enumerate(file):

    values = line.strip().split(",")

    controltime = datetime.now()

    if startTime == None:
        startDate = datetime.strptime(values[0], FMT)
        startTime = timedelta(hours=startDate.hour,
                                minutes=startDate.minute,
                                seconds=startDate.second).total_seconds()
        xar.append(0)
    else:
        newDate = datetime.strptime(values[0], FMT)
        newTime =  timedelta(hours=newDate.hour,
                                minutes=newDate.minute,
                                seconds=newDate.second).total_seconds()
        tDelta = newTime - startTime
        xar.append(int(tDelta))

    try:
        yar1.append(float(values[1]))
    except ValueError:
        yar1.append(0)
        pass

    try:
        yar2.append(float(values[2]))
    except ValueError:
        yar2.append(0)
        pass

    try:
        yar3.append(float(values[3]))
    except ValueError:
        yar3.append(0)
        pass

    try:
        yar4.append(float(values[4]))
    except ValueError:
        yar4.append(0)
        pass

    try:
        yar5.append(float(values[5]))
    except ValueError:
        yar5.append(0)
        pass


plt.plot(xar, yar1)
plt.plot(xar, yar2)
plt.plot(xar, yar3)
plt.plot(xar, yar4)
plt.plot(xar, yar5)
# end for

plt.legend([label1, label2, label3, label4, label5], loc='upper left')
plt.show()

