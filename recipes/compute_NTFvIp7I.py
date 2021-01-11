

# Load Environment
import dataiku
import pandas as pd
import numpy as np
import requests
import os


episodes_sample = dataiku.Dataset("episodes_sample_filtered")
episodes_sample_df = episodes_sample.get_dataframe()
mp3_folder = dataiku.Folder("mp3_files_local")

already_read = 0

for row in episodes_sample.iter_rows():
    
    already_read += 1 
    audio_id = row['id']
    audio_id = os.path.normpath(audio_id)
    url = row['audio_url']
    print("Accessing URL: " + url)
    file = requests.get(url)
    print(audio_id)
    
    if DOWNLOAD_FROM_URL == True:
    try:
        with mp3_folder.get_writer(audio_id + ".mp3") as w:
             w.write(file.content)
    except:
        pass
    
