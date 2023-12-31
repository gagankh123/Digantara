from datetime import datetime, timedelta
from sgp4.api import Satrec, jday
from sgp4.io import twoline2rv
from sgp4.earth_gravity import wgs72
from memory_profiler import profile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from line_profiler import LineProfiler
import math
import pyproj
import ast
import time
import psutil
import cProfile
import multiprocessing
import os

spark = SparkSession.builder.appName('satellite').getOrCreate()
# lprofiler = LineProfiler()

class JulianDateFormat:
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
    def __init__(self) -> None:
        self.ecef = pyproj.Proj(proj="geocent", ellps="WGS84", datum="WGS84")
        self.lla = pyproj.Proj(proj="latlong", ellps="WGS84", datum="WGS84")
        self.cache = {}

    def get_value(self, pos_x, pos_y, pos_z):
        if (pos_x, pos_y, pos_z) in self.cache:
            return self.cache[(pos_x, pos_y, pos_z)]
        
        lat, long, alt = pyproj.transform(self.ecef, self.lla, pos_x, pos_y, pos_z, radians=False)
        self.cache[(pos_x, pos_y, pos_z)] = (lat, long, alt)
        return (lat, long, alt)
    
class Satellite:
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
        counter = 0
        for time, jd, fr in self.julian_date:
            e, p, v = self.satellite.sgp4(jd, fr)
            lat, long, alt = self.LatLongAlt.get_value(p[0], p[1], p[2])
            if self.locations.is_lat_long_exist(lat, long):
                self.vectors.append([time, p[0], p[1], p[2], v[0], v[1], v[2],  lat, long, alt])
            counter = counter + 1
            if counter % 40000 == 0:
                print(f'counter: {counter}')

class Locations():
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
        if (lat, long) in self.correct_location:
            return self.correct_location[(lat, long)]
        for min_lat, max_lat, min_long, max_long in self.locations:
            if (min_lat <= lat <= max_lat) and (min_long <= long <= max_long):
                self.correct_location[(lat, long)] = True
                return True
        self.correct_location[(lat, long)] = False
        return False

def get_input(file_path):
    lines = spark.read.text(file_path)
    with open(file_path, 'r') as file:
        lines = file.readlines()
    timeStep = ast.literal_eval(input('Enter TimeStep: '))
    days = ast.literal_eval(input('Enter Number of days for which need to find Satellite positions: '))
    regions = ast.literal_eval(input("Enter regions: "))
    return regions, lines, timeStep, days

@udf(returnType=ArrayType(ArrayType(TimestampType(), IntegerType(), StringType())))
def sat_processing(satellite_inp, julianDates, lat_long_alt, locations):
    source, target = satellite_inp
    satellite = Satellite(source, target, julianDates, lat_long_alt, locations)
    satellite.get_vectors()
    return satellite.vectors


# @profile
def starting_point():
    regions, lines, timeStep, days = get_input('30sats.txt')

    julianDates = JulianDateFormat(timeStep, days).values
    lat_long_alt = LatLongAlt()
    locations = Locations()
    locations.set_rectangle_locations(regions)
     
    # lprofiler.add_function(locations.is_lat_long_exist)
    # lprofiler.add_function(lat_long_alt.get_value)
    
    start_time = time.time()
    sat_arr = [(lines[i], lines[i+1]) for i in range(0, len(lines), 2)]
    sat_arr = sat_arr[:16]
    rdd = spark.sparkContext.parallelize(sat_arr)
    rdd = rdd.repartition(8)

    results = rdd.map(lambda sat: sat_processing(sat, julianDates, lat_long_alt, locations))
    result = results.collect()
    end_time = time.time()
    print(f'avg_time: {(end_time - start_time)/len(sat_arr)}')

    # lprofiler.print_stats()
    # print(f'Avg Time Taken by Satellite: {total_time/counter} || MaxTime Per Satellite: {max_time} || MinTime per satellite: {min_time} || max_sat: {max_sat} || min_sat: {min_sat} || valid_locations: {sat_len}')


# [[(16.66673, 103.58196), (69.74973, -120.64459), (-21.09096, -119.71009), (-31.32309, -147.79778)]]
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