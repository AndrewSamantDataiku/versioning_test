# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
max_episodes = 2 # Set to 0 to use all episodes



import os
import dataiku

from dataiku import spark as dkuspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
#from deepspeech import Model
import scipy.io.wavfile as wav
import speech_recognition as sr


from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read recipe inputs

audios = dataiku.Folder("episode_wavs_hdfs")
#episode_wavs_hdfs_info = episode_wavs_hdfs.get_info()

idx = 0

path_list = []
for path in audios.list_paths_in_partition():
    path_list.append(str(path[1:]))
    idx += 1
    if idx == max_episodes:
        break

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

path_df = pd.DataFrame(path_list,columns=['path'])
df = sqlContext.createDataFrame(path_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def read_audio(path):
    r = sr.Recognizer()

    with audios.get_download_stream(path) as stream:
        with sr.AudioFile(stream) as source:
            audio_google = r.record(source)
    try:
        text = r.recognize_google(audio_google)
    except:
        text = "NA"
    return text


read_audio_udf = udf(read_audio, StringType())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df = df.withColumn("text",read_audio_udf(col("path")))
df_pd = df.toPandas()
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
 # Write recipe outputs
detected_texts = dataiku.Dataset("episodes_text_spark")
dkuspark.write_with_schema(df_pd)
#dkuspark.write_with_schema(detected_texts, df)