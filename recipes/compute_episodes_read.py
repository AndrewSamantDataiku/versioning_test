# -*- coding: utf-8 -*-
import dataiku
from dataiku import spark as dkuspark
import dataiku.spark as dspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import pandas as pd
import numpy as np
import requests
import ffmpeg
import speech_recognition as sr
import subprocess
from pydub import AudioSegment
import os
import scipy.io.wavfile as wav

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

MAX_EPISODES = 500


r = sr.Recognizer()

episodes_sample = dataiku.Dataset("episodes_sample_filtered")

episodes_sample_df = dspark.get_dataframe(sqlContext, episodes_sample)

mp3_folder = dataiku.Folder("mp3_files_local")
wav_folder = dataiku.Folder("wav_files_local")

def read_episode(url):

    import requests
#    import dataiku
    file = requests.get(url)
#    mp3_folder = dataiku.Folder("mp3_files_local")
    with open("audio.mp3", 'wb') as w:
        w.write(file.content)
        
    from pydub import AudioSegment
    AudioSegment.from_mp3("audio.mp3").export("/tmp/audio.wav", format="wav")
#    mp3_folder.delete_path(mp3_folder.get_path() + "/" + audio_id + ".mp3")
    r = sr.Recognizer()

    with audios.get_download_stream(path) as stream:
        with sr.AudioFile(stream) as source:
            audio_google = r.record(source)
    try:
        text = r.recognize_google(audio_google)
    except:
        text = "NA"
    return text

read_udf = udf(lambda z: read_episode(z), StringType())

rdf = episodes_sample_df
rdf2 = rdf.withColumn( 'url_out',read_udf('audio_url'))



# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a SparkSQL dataframe
#episodes_read_df = rdf2.to_pandas() # For this sample code, simply copy input to output

# Write recipe outputs
episodes_read = dataiku.Dataset("episodes_read")
dkuspark.write_with_schema(episodes_read, rdf2)
