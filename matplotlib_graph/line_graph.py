from matplotlib import pyplot as plt
import numpy as np

x = np.linspace(0,10,10)
y = [5,8,6,6,10,20,30,4,1,5]

plt.title('Graph')
plt.legend()
plt.grid(True)
plt.plot(x,y, label='blue line')
plt.show()


