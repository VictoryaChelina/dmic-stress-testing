from dmic_stress_testing.connection import process
import csv
import time
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as md
import numpy as np
import re


def write_log():
    admin = process()
    query = f'SELECT COUNT(active), SUM(active) FROM system.parts WHERE table = \'markfact\''
    with open('partition.csv', 'w', newline='') as log:
        writer = csv.writer(log, delimiter=',', quotechar='|')
        try:
            while True:
                t = datetime.datetime.now()
                t_time = t.strftime('%X')
                res = admin.raw(query)
                t_a = re.findall('\d+', res)
                total = t_a[0]
                active = t_a[1]
                writer.writerow([t_time, total, active])
                print(f'time:{t_time}    total:{total}   active:{active}', end='\r')
                time.sleep(1)
        except KeyboardInterrupt:
            print(f'partition metrics in partition.csv')


def read_log():
    time_stamp = []
    total = []
    active = []
    with open('partition.csv', 'r', newline='') as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='|')
        for row in csvreader:
            t_time = md.datestr2num(str(row[0]))
            time_stamp.append(t_time)
            total.append(int(row[1]))
            active.append(int(row[2]))
    return np.array(time_stamp), np.array(total), np.array(active)


def draw_graph(x, y_1, y_2):
    plt.plot_date(x, y_1, linestyle='-', color='r', label='total')
    plt.plot_date(x, y_2, linestyle='-', color='g', label='active')
    plt.xlabel("Time")
    plt.ylabel("Partitions")
    plt.legend()
    plt.show()


def main():
    write_log()
    time_stamp, total, active = read_log() 
    draw_graph(time_stamp, total, active)


if __name__ == '__main__':
    main()
    
