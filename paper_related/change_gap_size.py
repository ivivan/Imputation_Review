import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
from matplotlib.patches import Circle, RegularPolygon
from matplotlib.path import Path
from matplotlib.projections.polar import PolarAxes
from matplotlib.projections import register_projection
from matplotlib.spines import Spine
from matplotlib.transforms import Affine2D



if __name__ == '__main__':

    data_source_path = 'data\plot_sample.csv'

    data_source = pd.read_csv(data_source_path,header=0)

    data_source_transposed = data_source.T

    columns_name = ['Dual-SSIM','SSIM','BRITS','M-RNN','EM','MICE','Mean','LOCF','Linear']
    data_source_transposed.columns = columns_name

    print(data_source_transposed)




    # These are the "Tableau 20" colors as RGB.
    tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
                (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
                (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
                (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
                (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]
    # Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
    for i in range(len(tableau20)):
        r, g, b = tableau20[i]
        tableau20[i] = (r / 255., g / 255., b / 255.)

    lines = data_source_transposed.plot.line(figsize=(6,8))
    lines.set_ylabel("Scaled RMSE",size=18)
    lines.set_xlabel("Missing Data Size",size=18)
    lines.legend(bbox_to_anchor=(1.0, 1.0),fontsize=18)

    plt.show()


