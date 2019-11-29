# Manage data wrangling and manipulation.
#
# Datasets gathered from:
# https://data.world/kcmillersean/billboard-hot-100-1958-2017
# https://www.kaggle.com/gyani95/380000-lyrics-from-metrolyrics
# 

import pandas as pd
import numpy as np
import re


def process_datasets(af_csv, bb_csv, lyrics_csv):
	"""Create consistent keys for identifying songs in corpus
	"""

	df_af = pd.read_csv(af_csv) 
	df_bb = pd.read_csv(bb_csv)
	#df_ly = pd.read_csv(lyrics_csv)

	df_af = process_songkey(df_af)
	df_bb = process_songkey(df_bb)

	return df_af, df_bb


def process_songkey(df):
	"""For original dataworld set where we already have a songid
	"""
	df_ret = df.copy()
	df_ret['songkey'] = df_ret['songid']
	df_ret['songkey'] = df_ret['songkey'].str.lower()
	df_ret['songkey'] = df_ret['songkey'].str.replace(r'[^a-zA-Z\d]+', '')
	df_ret['songkey'] = df_ret['songkey'].str.strip()
	return df_ret
