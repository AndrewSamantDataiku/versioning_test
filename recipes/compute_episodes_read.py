# -*- coding: utf-8 -*-
import dataiku
from dataiku import spark as dkuspark
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

episodes_sample = dataiku.Dataset("episodes_sample")
episodes_sample_df = episodes_sample.get_dataframe()

mp3_folder = dataiku.Folder("mp3_files_local")
wav_folder = dataiku.Folder("wav_files_local")

def read_episode(url):
    import requests
    file = requests.get(url)
    with mp3_folder.get_writer(audio_id + ".mp3") as w:
        w.write(file.content)
    return s * s

sq_udf = udf(lambda z: sq(z), IntegerType())

rdf = sqlContext.createDataFrame(df)
rdf2 = rdf.withColumn( 'c2',sq_udf('c1'))


# Read recipe inputs
episodes_sample = dataiku.Dataset("episodes_sample")
episodes_sample_df = dkuspark.get_dataframe(sqlContext, episodes_sample)

# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a SparkSQL dataframe
episodes_read_df = episodes_sample_df # For this sample code, simply copy input to output

# Write recipe outputs
episodes_read = dataiku.Dataset("episodes_read")
dkuspark.write_with_schema(episodes_read, episodes_read_df)
