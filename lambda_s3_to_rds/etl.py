import pandas as pd

def data_cleaning(df):
    # Gets rid of T's and Z's in df["time"]
    df['time'] = df['time'].str.replace('T', ' ')
    df['time'] = df['time'].str.replace('Z', '')
    df['time'] = pd.to_datetime(df['time'])
    # Gets rid of T's and Z's in df["updated"]
    df['updated'] = df['updated'].str.replace('T', ' ')
    df['updated'] = df['updated'].str.replace('Z', '')
    df['updated'] = pd.to_datetime(df['updated'])

    df["nst"] = df["nst"].fillna(0).astype(int)
    df["magNst"] = df["nst"].fillna(0).astype(int)

    # Drops the place column
    df.drop(columns=["place"], inplace=True)

    df.drop(columns=["status"], inplace=True)

    # Drops nulls for time, latitude, longitude, type, and mag (there are none to begin with)
    cleaned_df = df.dropna(subset=["time", "latitude", "longitude", "type", "mag"])

    cleaned_df.columns = map(str.lower, cleaned_df.columns)
    cleaned_df.rename(columns={'time': 'event_time'}, inplace=True)

    return cleaned_df

