import sqlite3
import pandas as pd
from keplergl import KeplerGl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

from .. import parameters

def plot_intensities(datasets, file_name, fig_name):
    '''
    Plots a given a dataset(s) with same sized arrays datetimes and intensities. 

    Args:
        datasets (dict): Dict(s) with datetime array and intensities array.
    '''
    plt.clf()
    colormap = plt.colormaps['tab10']
    numDatasets = len(datasets)

    for i, (station, data) in enumerate(datasets.items()):
        # std = np.std(data['intensities'])

        color = colormap(i / numDatasets)
        plt.plot(data['datetimes'], data['intensities'], label=station, color=color)
        # plt.axhline(parameters.CUTOFF * std, color=color, lw=1)

    # Rotate the x-axis labels for better readability
    plt.gcf().autofmt_xdate()

    # Add labels and title
    plt.xlabel('Time (UTC -0)')
    plt.ylabel('Max Pixel Intensity')
    plt.title(fig_name)

    # Display the legend
    plt.legend()

    # Show the plot
    plt.savefig(f'{file_name}.png')
    plt.close()
    # plt.show()