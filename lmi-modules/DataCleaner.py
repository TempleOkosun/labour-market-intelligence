from functions import InputDataLoader
import matplotlib.pyplot as plt
plt.style.use('seaborn')

print('Test')

my = InputDataLoader.data_loader_function()
print('thanks:')
print(my.head(100))
my.plot()
plt.show()
