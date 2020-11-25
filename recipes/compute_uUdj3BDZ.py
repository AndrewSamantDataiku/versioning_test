
# Recipe Parameters

MAX_EPISODES = 50000

DOWNLOAD_FROM_URL = True

CONVERT_TO_WAV = True
SAVE_LOCAL_MP3_FILE = False

UPLOAD_TO_HDFS = True
SAVE_LOCAL_WAV_FILE = False


# Load Environment

import matplotlib
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
import speech_recognition as sr


r = sr.Recognizer()

episodes_sample = dataiku.Dataset("episodes_sample")
episodes_sample_df = episodes_sample.get_dataframe()

mp3_folder = dataiku.Folder("mp3_files_local")
wav_folder = dataiku.Folder("wav_files_local")
episode_wavs_hdfs_folder = dataiku.Folder("episode_wavs_hdfs")

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
        with mp3_folder.get_writer(audio_id + ".mp3") as w:
            w.write(file.content)
    if CONVERT_TO_WAV == True:
        subprocess.call(["/opt/ffmpeg/bin/ffmpeg","-y",
                             "-i",mp3_folder.get_path() + "/" + audio_id + ".mp3",
                             "-r","16000",
                             "-ac","1",
                             "-segment_time","00:00:50",
                             "-f","segment",
                             wav_folder.get_path() + "/" + audio_id+ "_part_%03d.wav"])
    if SAVE_LOCAL_MP3_FILE == False:
        mp3_folder.delete_path(mp3_folder.get_path() + "/" + audio_id + ".mp3")
    
    if UPLOAD_TO_HDFS == True:
        for path in wav_folder.list_paths_in_partition():
            path = path[1:]
            full_path = os.path.join(wav_folder.get_path(), path)
            episode_wavs_hdfs_folder.upload_file(path, full_path)
            if SAVE_LOCAL_WAV_FILE == False:
                wav_folder.delete_path(path)
                

    if already_read == MAX_EPISODES:
        break
    
