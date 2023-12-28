from datetime import datetime, timedelta
from sgp4.api import Satrec, jday
from sgp4.io import twoline2rv
from sgp4.earth_gravity import wgs72
from memory_profiler import profile
from line_profiler import LineProfiler
import math
import pyproj
import ast
import time
import psutil
import cProfile
import os

class JulianDateFormat:
    '''
    This class is used for converting the time into julian date format. Which is being used to find the satellite location
    '''
    def __init__(self, timeStep, days) -> None:
        self.timeStep = timeStep
        self.days = days
        self.values = self.get_value()
    
    def get_value(self):
        value = []
        start_time = datetime.now()
        total_intervals = math.ceil(self.days*24*60*60/self.timeStep)
        for interval in range(total_intervals):
            jd, fr = jday(start_time.year, start_time.month, start_time.day, start_time.hour, start_time.minute, start_time.second)
            value.append((start_time, jd, fr))
            start_time = start_time + timedelta(self.timeStep)

        return value        
    
class LatLongAlt():
    '''
    This class is being used to find the latitude, longitude and altitude of the satellite positions. 
    '''
    def __init__(self) -> None:
        self.ecef = pyproj.Proj(proj="geocent", ellps="WGS84", datum="WGS84")
        self.lla = pyproj.Proj(proj="latlong", ellps="WGS84", datum="WGS84")
        self.cache = {}  # it stores the lat, long, alt values for a particular position. This will help to not to calculate again for the same positions.

    def get_value(self, pos_x, pos_y, pos_z):
        if (pos_x, pos_y, pos_z) in self.cache:
            return self.cache[(pos_x, pos_y, pos_z)]
        
        lat, long, alt = pyproj.transform(self.ecef, self.lla, pos_x, pos_y, pos_z, radians=False)
        self.cache[(pos_x, pos_y, pos_z)] = (lat, long, alt)
        return (lat, long, alt)
    
class Satellite:
    '''
    This is the main satellite class. Each satellite has this class object. 
    This satellite has vectors object which is used to get the details of the satellite, position for all the satellite which are reside within the rectagle given by user. 
    '''

    def __init__(self, source_data, target_data, julian_dates, LatLongAlt, locations) -> None:
        self.source = source_data
        self.target = target_data
        self.julian_date = julian_dates
        self.LatLongAlt = LatLongAlt
        self.locations = locations
        self.satellite = self.get_satellite()
        self.vectors = []
    
    def get_satellite(self):
        assert twoline2rv(self.source, self.target, wgs72)
        return Satrec.twoline2rv(self.source, self.target) 
    
    def get_vectors(self):
        for time, jd, fr in self.julian_date:
            e, p, v = self.satellite.sgp4(jd, fr)
            lat, long, alt = self.LatLongAlt.get_value(p[0], p[1], p[2])
            if self.locations.is_lat_long_exist(lat, long):
                self.vectors.append([time, p[0], p[1], p[2], v[0], v[1], v[2],  lat, long, alt])

class Locations():
    '''
    Location class, basically used to check the location of the satellite is exist in the rectangle positions given by user. 
    '''
    def __init__(self) -> None:
        self.locations = []
        self.correct_location = {}

    def set_rectangle_locations(self, regions):
        for region in regions:
            lat1, long1 = region[0]
            lat2, long2 = region[1]
            lat3, long3 = region[2]
            lat4, long4 = region[3]
            max_lat = max(lat1, lat2, lat3, lat4)
            min_lat = min(lat1, lat2, lat3, lat4)
            max_long = max(long1, long2, long3, long4)
            min_long = min(long1, long2, long3, long4)
            self.locations.append((min_lat, max_lat, min_long, max_long))
    
    def is_lat_long_exist(self, lat, long):
        if (lat, long) in self.correct_location:   # cache the results for faster look up
            return self.correct_location[(lat, long)]
        for min_lat, max_lat, min_long, max_long in self.locations:
            if (min_lat <= lat <= max_lat) and (min_long <= long <= max_long):
                self.correct_location[(lat, long)] = True
                return True
        self.correct_location[(lat, long)] = False
        return False

def get_input(file_path):
    execution_mode = input('Execution Mode is Test or Prod? ')
    if execution_mode == 'Prod':
        file_path = '27000sats.txt'
    with open(file_path, 'r') as file:
        lines = file.readlines()
    defaultTimeStep = 0.1 if '27000sats.txt' in file_path else 1
    defaultDays = 5

    timeStep = ast.literal_eval(input('Enter TimeStep: '))
    if timeStep == '':
        timeStep = defaultTimeStep
    days = ast.literal_eval(input('Enter Number of days for which need to find Satellite positions: '))
    if days == '':
        days = defaultDays
    regions = ast.literal_eval(input("Enter regions: "))
    return regions, lines, timeStep, days

def starting_point():
    regions, lines, timeStep, days = get_input('30sats.txt')
    julianDates = JulianDateFormat(timeStep, days).values
    lat_long_alt = LatLongAlt()
    locations = Locations()
    locations.set_rectangle_locations(regions)
    result = []
    max_time = 0
    min_time = 0
    total_time = 0
    min_sat = 0
    max_sat = 0
    sat_len = {} 
    for i in range(0, len(lines), 2):
        start_time = time.time()
        satellite = Satellite(lines[i], lines[i+1], julianDates, lat_long_alt, locations)
        satellite.get_vectors()
        result.append(satellite)
        sat_len[i] = len(satellite.vectors)
        end_time = time.time()
        sat_time = end_time - start_time
        total_time = total_time + (sat_time)
        if max_time == 0 or max_time < sat_time:
            max_time = sat_time 
        if min_time == 0 or min_time > sat_time:
            min_time = sat_time 
    print(f'Avg Time Taken by Satellite: {total_time/len(lines)/2} || MaxTime Per Satellite: {max_time} || MinTime per satellite: {min_time} || max_sat: {max_sat} || min_sat: {min_sat} || valid_locations: {sat_len}')


if __name__ == '__main__':
    start_time = time.time()
    starting_point()
    cpu_percent = psutil.cpu_percent()
    print(f'CPU Percent: {cpu_percent}')
    end_time = time.time()
    print(f'total_time_taken: {end_time - start_time} seconds')
    pid = os.getpid()
    memory_info = psutil.Process(pid).memory_info()[0]/2.**20
    print(f'Memory Info: {memory_info}')