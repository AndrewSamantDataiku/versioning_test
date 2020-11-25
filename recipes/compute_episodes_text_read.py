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

episodes_sample = dataiku.Dataset("episodes_sample_filtered")
episodes_sample_df = episodes_sample.get_dataframe()

def read_episode(url):
    
    import requests
    file = requests.get(url)
    
    with open("audio.mp3", 'wb') as w:
        w.write(file.content)
    
    from mutagen.mp3 import MP3
    duration = MP3("audio.mp3").info.length
    import math
    chunk_count = int(math.ceil(duration/30))
    
    s= list()
    
    import speech_recognition as sr
    r = sr.Recognizer()
    for c in range(1,chunk_count):
        subprocess.call(["ffmpeg","-y",
                             "-ss",str( (c-1)*30),
                             "-i","audio.mp3",
                             "-r","16000",
                             "-ac","1", 
                             "-t","30",
                             "audio_part_" + str(c) + ".wav"])
        
        
        with sr.AudioFile("audio_part_" + str(c) + ".wav") as source:
            audio = r.record(source)
            #try:
            recognized = r.recognize_google(audio)
            #except:
            #    recognized = ""
        s.append( recognized )
    
    return s

episodes_sample_df['text'] = []
rdf2 = rdf.withColumn( 'text',read_udf('audio_url'))



# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a SparkSQL dataframe
#episodes_read_df = rdf2.to_pandas() # For this sample code, simply copy input to output

# Write recipe outputs
episodes_read = dataiku.Dataset("episodes_read")
dkuspark.write_with_schema(episodes_read, rdf2)
