import numpy as np
import pandas as pd

import lyricsgenius
import spotipy
import multiprocessing as mp
import time
from spotipy.oauth2 import SpotifyClientCredentials

sp_auth_manager = SpotifyClientCredentials(client_id="1d4690895143483784c3af5f23e9401a", client_secret="8e26c441fecb49a3bec41a3160eac3a1")
sp = spotipy.Spotify(auth_manager=sp_auth_manager)

genius_auth_token = "OIkdgTyf7Dn3BS5srExF7gmC8vIABJjdhuZ_F0_i6HNiLdD5d0bctrNBwjjkp3QoN4rIsKhSLLI_TweVE7K29A"
genius = lyricsgenius.Genius(genius_auth_token)

playlists = pd.read_csv("playlists.csv")

def convert_playlists(playlists):
    asg = open("asg.csv", "w+")
    asg.truncate(0)
    asg.write("name, artist, genre\n")

    for i in range(len(playlists)):
        tracks = sp.playlist_tracks(playlists.URI[i])
        for j in range(len(tracks['items'])):
            name = tracks['items'][j]['track']['name']
            artist = tracks['items'][j]['track']['album']['artists'][0]['name']
            if(not("," in name or "," in artist)):
                asg.write(name + "," + artist + "," + playlists.Genre[i] + "\n")

def grab_lyrics(row):
    song = genius.search_song(row[0], row[1], get_full_info=False)
    return None if not song else song.lyrics

def apply_grab_lyrics(df):
    lyrics = df.apply(grab_lyrics, axis=1)
    return lyrics

#convert_playlists(playlists)
df = pd.read_csv("asg.csv")
split_num = int(df.shape[0] / (mp.cpu_count() * 20))
split_df = np.array_split(df, split_num)
lyrics = []
for df in split_df:
    n_proc = min(mp.cpu_count() * 10, df.shape[0])
    p = mp.Pool(n_proc)
    split_dfs = np.array_split(df, n_proc)
    pool_res = p.map(apply_grab_lyrics, split_dfs)
    p.close()
    p.join()

    lyrics.append(pd.concat(pool_res, axis=0))
    time.sleep(2)

df["Lyrics"] = lyrics
df = df.dropna()

print(df)
