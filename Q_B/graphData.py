import matplotlib.pyplot as plt
import numpy as np
import ast
import re

x = np.arange(10)
file = open("ChartData.txt", "r+")

for line in file:
    time = line.split(" ")[1]
    time = time[:8]
    print(time)
    temp = re.search(r'\[(.*?)\]', line)
    print(temp)
    list = ast.literal_eval(temp)
    print(list)
    # plt.plot(time, )
# plt.plot(x, x)
# plt.plot(x, 2 * x)
# plt.plot(x, 3 * x)
# plt.plot(x, 4 * x)
#
# plt.legend(['y = x', 'y = 2x', 'y = 3x', 'y = 4x'], loc='upper left')
# plt.show()


