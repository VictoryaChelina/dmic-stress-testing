import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import csv


def read_log():
    time = []
    rows = []
    rps = []
    with open('some_5.csv', 'r', newline='') as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='|')
        for row in csvreader:
            time.append(float(row[0]))
            rows.append(int(row[1]))
            rps.append(float(row[2]))
    return np.array(time), np.array(rows), np.array(rps)

def draw_graph(x, y):
    fig, ax = plt.subplots()
    ax.plot(x, y)
    ax.set_xlabel('Time')
    ax.set_ylabel('Rows')
    plt.show()

def main():
    time, rows, rps = read_log()
    draw_graph(time, rows)

if __name__ == '__main__':
    main()