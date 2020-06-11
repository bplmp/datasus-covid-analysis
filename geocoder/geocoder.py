import pandas as pd
import geopy
import json
from pandas.io.json import json_normalize
import glob
import os
from pathlib import Path
from slugify import slugify
import hashlib


def load_geocoded_addresses(path_to_folder):
    json_files = glob.glob(path_to_folder + '*.json')
    frames = []
    for f in json_files:
        e = json.load(open(f))
        if 'error' not in e:
            filename = os.path.splitext(os.path.basename(f))[0]
            e['filename'] = filename
            e_df = pd.DataFrame.from_dict(json_normalize(e))
            frames.append(e_df)
    geocoded_data = pd.concat(frames, sort=True)
    return geocoded_data


def join_geocoded_addresses_to_df(df, geocoded_data):
    cols = ['geocode_hash', 'formatted_address', 'geometry.location.lat', 'geometry.location.lng', 'geometry.location_type', 'place_id', 'types']
    df_geo = df.merge(geocoded_data[cols], how='left', on='geocode_hash')
    df_geo.rename(columns={'geometry.location.lat': 'lat', 'geometry.location.lng': 'lng', 'geometry.location_type': 'location_type', 'formatted_address': 'google_formatted_address'}, inplace=True)
    return df_geo


def geocode_addresses(addresses, address_col, id_col, folder_path, geopy_geocoder):
    i = 0
    addresses.is_copy = False  # to stop SettingWithCopyWarning
    print('--->geocoding ' + str(len(addresses)) + ' addresses.')
    for index, row in addresses.iterrows():
        i += 1
        address = row[address_col]
        pkey = str(row[id_col])
        # print(pkey)
        file = Path(folder_path + pkey + ".json")
        if file.exists():
            pass
            # print('--->file already exists, skipping')
        else:
            print(str(i) + '/' + str(len(addresses)) + ' || ' + pkey + ': ', address)
            print('--->making API request')
            try:
                geocoded = geopy_geocoder.geocode(query=address)
                addresses.loc[index, address_col] = address
                addresses.loc[index, id_col] = pkey
                geocoded.raw[address_col] = address
                geocoded.raw[id_col] = pkey
                addresses.loc[index, 'latitude'] = geocoded.latitude
                addresses.loc[index, 'longitude'] = geocoded.longitude
                addresses.loc[index, 'address_geocoded'] = geocoded.address
                if 'plus_code' in geocoded.raw:
                    addresses.loc[index, 'global_plus_code'] = geocoded.raw['plus_code']['global_code']
                if 'place_id' in geocoded.raw:
                    addresses.loc[index, 'google_place_id'] = geocoded.raw['place_id']
                addresses.loc[index, 'raw_geocoded'] = json.dumps(geocoded.raw)
                with open(folder_path + pkey + '.json', 'w') as fp:
                    json.dump(geocoded.raw, fp)
            except Exception as e:
                with open(folder_path + pkey + '.json', 'w') as fp:
                    json.dump({'error': True}, fp)
                print(e)
    print('--->done.')


def geocode(df, address_col, data_folder, GOOGLE_MAPS_API_KEY, use_slugify=False):
    geopy_geocoder = geopy.geocoders.GoogleV3(api_key=GOOGLE_MAPS_API_KEY, timeout=20)

    # drop null
    df = df[df[address_col].notna()]

    # format address to reduce need for geocoding
    df['geocode'] = df[address_col].str.upper()

    # generate hash for file name and joins
    if use_slugify:
        df['geocode_hash'] = df['geocode'].apply(lambda x: slugify(x))
        df_address = df[df.geocode.notna()][['geocode', 'geocode_hash']]
        df_address = df_address.drop_duplicates('geocode')
    else:
        df['geocode_hash'] = df['geocode'].apply(lambda x: hashlib.sha1(x.encode('utf-8')).hexdigest())
        df_address = df[df.geocode.notna()][['geocode', 'geocode_hash']]
        df_address = df_address.drop_duplicates('geocode')

    # check for already geocoded
    json_files = glob.glob(data_folder + '*.json')
    hashes = []
    for f in json_files:
        hashes.append(os.path.splitext(os.path.basename(f))[0])
    not_geo = df_address[~(df_address.geocode_hash.isin(hashes))]
    already_geo = df_address[(df_address.geocode_hash.isin(hashes))]
    print(f'--->{len(already_geo)} already geocoded, will geocode {len(not_geo)} addresses')

    # geocode
    Path(data_folder).mkdir(parents=True, exist_ok=True)
    geocode_addresses(not_geo, 'geocode', 'geocode_hash', data_folder, geopy_geocoder)

    # join back on original df and return
    df_geocoded_data = load_geocoded_addresses(data_folder)
    df_address_geocoded = join_geocoded_addresses_to_df(df_address, df_geocoded_data)
    df_geocoded = df.merge(df_address_geocoded.drop('geocode', axis=1), how='left', on='geocode_hash')
    return df_geocoded
