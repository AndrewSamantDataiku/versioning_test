

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
wav_folder = dataiku.Folder("wav_files_local")
wav_folder_path = wav_folder.get_path()

already_read = 0

for row in episodes_sample.iter_rows():
    
    audio_id = os.path.normpath(audio_id)
    

    subprocess.call(["/opt/ffmpeg/bin/ffmpeg","-y",
                             "-i",mp3_folder.get_path() + "/" + audio_id + ".mp3",
                             "-r","16000",
                             "-ac","1",
                             "-segment_time","00:00:50",
                             "-f","segment",
                             wav_folder_path + "/" + audio_id+ "_part_%03d.wav"])
    
