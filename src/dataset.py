# Manage data wrangling and manipulation.
#
# Datasets gathered from:
# https://data.world/kcmillersean/billboard-hot-100-1958-2017
# https://www.kaggle.com/gyani95/380000-lyrics-from-metrolyrics
# 

from pandarallel import pandarallel
import lyricsgenius
import pandas as pd
import numpy as np
import re


def process_datasets(af_csv, bb_csv, sample_size=None, filter='hip hop'):
	"""Create consistent keys for identifying songs in corpus

	"""

	df_af = pd.read_csv(af_csv) 
	df_af.dropna(inplace=True)
	df_af = df_af.loc[df_af['artist_genre'].str.contains(filter)]

	if sample_size:
		df_af = df_af.sample(sample_size)

	df_bb = pd.read_csv(bb_csv)

	# Subset dataframe and scrape song lyrics
	pandarallel.initialize()
	df_ly = df_bb[['songid', 'performer', 'song']].drop_duplicates()
	df_ly = df_ly.loc[df_ly['songid'].isin(df_af['songid'])]
	df_ly['lyrics'] = df_ly.parallel_apply(get_lyrics, axis=1)

	return df_af, df_bb, df_ly


def get_lyrics(row):
	genius = lyricsgenius.Genius('QOkPNsgDXBxuex27C6TWN2R0EQJ5pol4baxhYde0rxlGoAZ2Hfyb3OpLXm52e8ta')
	song = genius.search_song(row.performer, row.song)
	return song.lyrics


def process_songkey(df):
	"""For original dataworld set where we already have a songid
	"""
	df_ret = df.copy()
	df_ret['songkey'] = df_ret['songid']
	df_ret['songkey'] = df_ret['songkey'].str.lower()
	df_ret['songkey'] = df_ret['songkey'].str.replace(r'[^a-zA-Z\d]+', '')
	df_ret['songkey'] = df_ret['songkey'].str.strip()
	return df_ret
