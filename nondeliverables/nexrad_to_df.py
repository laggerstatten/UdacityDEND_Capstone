from xml.dom import minidom
from sys import stdin
from urllib import request
from subprocess import call
import numpy as np
import pandas as pd
import itertools
from tabulate import tabulate
import pyart
from sphere import RegionCoverer, Cell, LatLng, LatLngRect, CellId
from datetime import datetime, timedelta
import time

### read in NEXRAD file
radar = pyart.io.read('data/NEXRAD/KCRP20200101_000431_V06')

### build station data table
station_info = ['StationName', 'Product', 'Pattern', 'Latitude', 'Longitude', 'Altitude', 'StartTime']
station_row = []

station_name = radar.metadata['instrument_name']
product = radar.metadata['original_container']
pattern = radar.metadata['vcp_pattern']
latitude0 = radar.latitude['data'][0]
longitude0 = radar.longitude['data'][0]
altitude0 = radar.altitude['data'][0]
volume_start = datetime.strptime(radar.time['units'][14:34], '%Y-%m-%dT%H:%M:%SZ')

station_row.extend([station_name, product, pattern, latitude0, longitude0, altitude0, volume_start] )

stations = pd.DataFrame(
    {'StationName': station_name, 
     'Product': product, 
     'Pattern': pattern, 
     'Latitude': latitude0,
     'Longitude': longitude0,
     'Altitude': altitude0,
     'StartTime': volume_start     
    })

stations.to_csv

### build initial samples table
merged_lat = list(itertools.chain.from_iterable(radar.gate_latitude['data']))
merged_lon = list(itertools.chain.from_iterable(radar.gate_longitude['data']))
merged_alt = list(itertools.chain.from_iterable(radar.gate_altitude['data']))
merged_refl = list(itertools.chain.from_iterable(radar.fields['reflectivity']['data']))
merged_velo = list(itertools.chain.from_iterable(radar.fields['velocity']['data']))
time_x1 = [volume_start + timedelta(seconds=s) for s in radar.time['data']]
time_xgates = [val for val in time_x1 for _ in range(radar.ngates)]

samples = pd.DataFrame(
    {'GateLat': merged_lat,
     'GateLon': merged_lon,
     'GateAlt': merged_alt,
     'GateTime': time_xgates,
     'Reflectivity': merged_refl,
     'Velocity': merged_velo
    })
    

### calculate spatial index
s2level = 10

samples['S2LL'] = [LatLng.from_degrees(x, y) for x, y in zip(samples['GateLat'], samples['GateLon'])]
samples['S2CellID'] = [CellId().from_lat_lng(xy) for xy in samples['S2LL']]
samples['S2Region'] = [z.parent(s2level) for z in samples['S2CellID']]




