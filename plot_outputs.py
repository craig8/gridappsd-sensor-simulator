import csv;
import numpy as np;
import matplotlib as mpl;
import matplotlib.pyplot as plt;

if __name__ == '__main__':
    d1 = np.loadtxt('Input.csv', dtype=float, skiprows=1, delimiter=',')
    h1 = d1[:,0] / 3600.0
    vraw = d1[:,1]
    iraw = d1[:,2]
    praw = d1[:,3]
    qraw = d1[:,4]

    d2 = np.loadtxt('Output.csv', dtype=float, skiprows=1, delimiter=',')
    h2 = d2[:,0] / 3600.0
    vavg = d2[:,1]
    vmin = d2[:,2]
    vmax = d2[:,3]
    iavg = d2[:,4]
    imin = d2[:,5]
    imax = d2[:,6]
    pavg = d2[:,7]
    pmin = d2[:,8]
    pmax = d2[:,9]
    qavg = d2[:,10]
    qmin = d2[:,11]
    qmax = d2[:,12]

    fig, ax = plt.subplots(4, 1, sharex = 'col')
    ax[0].plot(h1, vraw, color='black', label='Raw')
    ax[0].step(h2, vavg, color='red', label='Avg')
    ax[0].step(h2, vmin, color='blue', label='Min')
    ax[0].step(h2, vmax, color='green', label='Max')
    ax[0].grid()
    ax[0].legend()
    ax[0].set_ylabel ('Voltage [120-V base]')

    ax[1].plot(h1, iraw, color='black', label='Raw')
    ax[1].step(h2, iavg, color='red', label='Avg')
    ax[1].step(h2, imin, color='blue', label='Min')
    ax[1].step(h2, imax, color='green', label='Max')
    ax[1].grid()
    ax[1].legend()
    ax[1].set_ylabel ('Current [A]')

    ax[2].plot(h1, praw, color='black', label='Raw')
    ax[2].step(h2, pavg, color='red', label='Avg')
    ax[2].step(h2, pmin, color='blue', label='Min')
    ax[2].step(h2, pmax, color='green', label='Max')
    ax[2].grid()
    ax[2].legend()
    ax[2].set_ylabel ('Real Power [W]')

    ax[3].plot(h1, qraw, color='black', label='Raw')
    ax[3].step(h2, qavg, color='red', label='Avg')
    ax[3].step(h2, qmin, color='blue', label='Min')
    ax[3].step(h2, qmax, color='green', label='Max')
    ax[3].grid()
    ax[3].legend()
    ax[3].set_ylabel ('Reactive Power [VAR]')

    ax[3].set_xlabel ('Hours')
    plt.show()


