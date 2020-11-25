import dataiku
import pandas as pd
import numpy as np
import requests
import ffmpeg
import speech_recognition as sr
import subprocess
from pydub import AudioSegment
import os
import scipy.io.wavfile as wav
from mutagen.mp3 import MP3
import math

episodes_sample = dataiku.Dataset("episodes_sample_filtered")
episodes_sample_df = episodes_sample.get_dataframe()

mp3_folder = Dataiku.Folder('temp_mp3_folder')
mp3_folder_path = mp3_folder.get_path()
audio_path = mp3_folder_path + '/audio.mp3'
wav_path = mp3_folder_path + '/audio.wav'

failure_count = 0

def read_episode(url):
    
    
    file = requests.get(url)    
    with mp3_folder.get_writer("audio.mp3") as w:
            w.write(file.content)
    
    duration = MP3(audio_path).info.length
    chunk_count = int(math.ceil(duration/30))
    
    s= list()
    
    import speech_recognition as sr
    r = sr.Recognizer()
    for c in range(1,chunk_count):
        subprocess.call(["ffmpeg","-y",
                             "-ss",str( (c-1)*30),
                             "-i",audio_path,
                             "-r","16000",
                             "-ac","1", 
                             "-t","30",
                             wav_path])
        
        
        with sr.AudioFile(wav_path) as source:
            audio = r.record(source)
            try:
                recognized = r.recognize_google(audio)
            except:
                recognized = ""
        s.append( recognized )
    
    return s

episodes_sample_df['text'] = ''

for index, row in episodes_sample_df.iterrows():
    text = read_episode(row['url'])



# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a SparkSQL dataframe
#episodes_read_df = rdf2.to_pandas() # For this sample code, simply copy input to output

# Write recipe outputs
episodes_read = dataiku.Dataset("episodes_read")
dkuspark.write_with_schema(episodes_read, rdf2)
