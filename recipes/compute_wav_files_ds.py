# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
wav_files_local = dataiku.Folder("klPrZDXv")
wav_files_local_info = wav_files_local.get_info()


# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

wav_files_ds_df = ... # Compute a Pandas dataframe to write into wav_files_ds


# Write recipe outputs
wav_files_ds = dataiku.Dataset("wav_files_ds")
wav_files_ds.write_with_schema(wav_files_ds_df)
