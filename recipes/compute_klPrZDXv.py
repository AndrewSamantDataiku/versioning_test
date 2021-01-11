# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Load Environment

import dataiku
import pandas as pd
import numpy as np
import ffmpeg
import subprocess
from pydub import AudioSegment
import os
import scipy.io.wavfile as wav


mp3_folder = dataiku.Folder("mp3_files_local")
mp3_folder_path = mp3_folder.get_path()
wav_folder = dataiku.Folder("wav_files_local")
wav_folder_path = wav_folder.get_path()


ffmpeg_path = '/data/dataiku/data_dir/code-envs/python/ffmpeg_27/bin/ffmpeg/ffmpeg-4.3.1-amd64-static/ffmpeg'


already_read = 0

for mp3 in mp3_folder.list_paths_in_partition()[0:2]:
    print(mp3_folder_path + mp3)
    print(wav_folder_path + mp3[:-4]+ "_part_%03d.wav")
    subprocess.call([ffmpeg_path,"-y",
                             "-i",str(mp3_folder_path + mp3),
                             "-r","16000",
                             "-ac","1",
                             "-segment_time","00:00:50",
                             "-f","segment",
                             str(mp3_folder_path + mp3)[:-4] + "_part_%03d.wav"])