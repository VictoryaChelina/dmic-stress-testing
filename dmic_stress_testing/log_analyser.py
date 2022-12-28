import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import csv


def read_log():
    rps = []
    seconds = []
    with open('some.csv', 'r', newline='') as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='|')
        for i, row in enumerate(csvreader):
            rps.append(float(row[0]))
            seconds.append(i)
    return rps, seconds

def draw_graph(x, y):
    fig, ax = plt.subplots()
    ax.plot(x, y)
    plt.show()

def main():
    rps, seconds = read_log()
    draw_graph(seconds, rps)

if __name__ == '__main__':
    main()