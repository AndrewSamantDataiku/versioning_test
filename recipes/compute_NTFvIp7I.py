# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
episodes_sample = dataiku.Dataset("episodes_sample")
episodes_sample_df = episodes_sample.get_dataframe()




# Write recipe outputs
mp3_files_local = dataiku.Folder("NTFvIp7I")
mp3_files_local_info = mp3_files_local.get_info()
