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
                try:
                    time.wait(10)
                    r = sr.Recognizer()
                    recognized = r.recognize_google(audio)
                except:
                    try:
                        time.wait(60)
                        r = sr.Recognizer()
                        recognized = r.recognize_google(audio)
                        failure_count = failure_count+1
                    except:
                        if failure_count % 5 == 0:
                            time.wait(300)
                        try:
                            r = sr.Recognizer()
                            recognized = r.recognize_google(audio)
                        except:
                            recognized = ""
        s.append( recognized )
    
    return s

episodes_sample_df['text'] = ''

episodes_sample_df['text'] = episodes_sample_df.apply(lambda row: read_episode(row['link']), axis=1)
    


# Write recipe outputs
episodes_read = dataiku.Dataset("episodes_read")
episodes_read.write_with_schema(episodes_sample_df)
