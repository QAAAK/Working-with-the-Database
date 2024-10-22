from matplotlib import pyplot as plt

movies = ['Anny Hall', 'Ben-Gur', 'Casablanka','Gandi', 'Westside Story']
count_oscar = [5,11,3,8,10]

plt.bar(movies, count_oscar, color = 'green')
plt.title('Movies')
plt.ylabel('Oscar')

plt.show()
