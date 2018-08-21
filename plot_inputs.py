import csv;
import numpy as np;
import matplotlib as mpl;
import matplotlib.pyplot as plt;

if __name__ == '__main__':
    dxf = np.loadtxt('Transformer.csv', dtype=complex, skiprows=9, delimiter=',', usecols=[1,2,3,4,5,6])
    dtm = np.loadtxt('TPM_B0.csv', dtype=float, skiprows=9, delimiter=',', usecols=[1,2,3,4,5,6])

    n = dxf.shape[0]
    hrs = np.linspace(0, n/3600.0, n)
    vmtr = np.array (np.sqrt (np.add (dtm[:,0]*dtm[:,0],dtm[:,1]*dtm[:,1])))
    imtr = np.array (np.sqrt (np.add (dtm[:,2]*dtm[:,2],dtm[:,3]*dtm[:,3])))
    pmtr = dtm[:,4]
    qmtr = dtm[:,5]

    fig, ax = plt.subplots(4, 1, sharex = 'col')
    ax[0].plot(hrs, np.absolute(dxf[:,0])/60.0, color='red', label='Va')
    ax[0].plot(hrs, np.absolute(dxf[:,2])/60.0, color='blue', label='Vb')
    ax[0].plot(hrs, np.absolute(dxf[:,4])/60.0, color='green', label='Vc')
    ax[0].plot(hrs, vmtr, color='magenta', label='Vmtr')
    ax[0].grid()
    ax[0].legend()
    ax[0].set_ylabel ('Voltage [120-V base]')

    ax[1].plot(hrs, np.absolute(dxf[:,1])*0.001, color='red', label='Sa')
    ax[1].plot(hrs, np.absolute(dxf[:,3])*0.001, color='blue', label='Sb')
    ax[1].plot(hrs, np.absolute(dxf[:,5])*0.001, color='green', label='Sc')
    ax[1].grid()
    ax[1].legend()
    ax[1].set_ylabel ('Substation Power [kVA]')

    ax[2].plot(hrs, imtr, color='magenta', label='Imtr')
    ax[2].grid()
    ax[2].legend()
    ax[2].set_ylabel ('Meter Current [A]')

    ax[3].plot(hrs, pmtr*0.001, color='red', label='Pmtr')
    ax[3].plot(hrs, qmtr*0.001, color='blue', label='Qmtr')
    ax[3].grid()
    ax[3].legend()
    ax[3].set_ylabel ('Meter Power [kVA]')

    ax[3].set_xlabel ('Hours')
    plt.show()


