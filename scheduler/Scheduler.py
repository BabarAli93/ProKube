import pandas as pd
import numpy as np
import os
from itertools import product
import matplotlib.pyplot as plt
import warnings

# Suppress FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

class Scheduler:
    def __init__(self, path:str):
        self.dataset_path = path

    def dataset_reading(self):

        """This function is responsible to read sv files into a dataframe. Each function can use it as per desire by making a local copy of it"""
        csv_files = [file for file in os.listdir(self.dataset_path) if file.endswith('.csv')]
        dataframes = []
        for csv_file in csv_files:
            path_csv = os.path.join(self.dataset_path, csv_file)
            df_csv = pd.read_csv(path_csv)
            dataframes.append(df_csv)
        
        
        self.df = pd.concat(dataframes)
        core_replacement = {'half':500, 'one': 1000, 'two': 2000}
        self.df['core'] = self.df['core'].replace(core_replacement)
        loc_replacement = {'belgium': 'europe-west1', 'france': 'europe-west9', 'germany': 'europe-west3', 'london': 'europe-west2'}
        self.df['location'] = self.df['location'].replace(loc_replacement)

        return self.df
        