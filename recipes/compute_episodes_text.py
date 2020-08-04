
max_episodes = 0 # Set to 0 to use all episodes



import os
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from deepspeech import Model
import scipy.io.wavfile as wav
import speech_recognition as sr


# Read recipe inputs
audios = dataiku.Folder("episode_wavs_hdfs")

df = pd.DataFrame(columns=['path', 'text'])
idx = 0

for path in audios.list_paths_in_partition():

    r = sr.Recognizer()
        
    with audios.get_download_stream(path[1:]) as stream:
        with sr.AudioFile(stream) as source:
            audio_google = r.record(source)
    try:
        text = r.recognize_google(audio_google)
    except:
        text = "NA"
    df.loc[idx] = [path, text]
    idx += 1 
    
    if idx == max_episodes:
        break
 
 
 # Write recipe outputs
detected_texts = dataiku.Dataset("episodes_text")
detected_texts.write_with_schema(df)