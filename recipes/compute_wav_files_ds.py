# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import os

# Read recipe inputs
wav_files_local = dataiku.Folder("klPrZDXv")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
wav_files = wav_files_local.list_paths_in_partition()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
folder_path = wav_files_local.get_path()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
wav_paths = [os.path.join([folder_path,f[1:]]) for f in wav_files]
wav_files = [f[1:-4] for f in wav_files]




wav_files_ds_df = pd.DataFrame({"file":wav_files,"path":wav_paths})


# Write recipe outputs
wav_files_ds = dataiku.Dataset("wav_files_ds")
wav_files_ds.write_with_schema(wav_files_ds_df)