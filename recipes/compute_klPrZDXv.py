# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
mp3_files_local = dataiku.Folder("NTFvIp7I")
mp3_files_local_info = mp3_files_local.get_info()




# Write recipe outputs
wav_files_local = dataiku.Folder("klPrZDXv")
wav_files_local_info = wav_files_local.get_info()
