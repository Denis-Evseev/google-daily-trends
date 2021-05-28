import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt


class GraphBuilder:

    def __init__(self, figsize=(10,10)):
        self.figsize=figsize

    def build(self, lines, x_label, y_label, grid=True, title="Empty title"):
        # plt.figure(figsize=self.figsize)

        for line in lines:
            plt.plot(line.data, label=line.label)

        plt.grid(grid)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        plt.show()
