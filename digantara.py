from sgp4.api import accelerated

def read_tle_data():
    # tle - two line element, which has satellite orbits informations.
    # Each 2 line represent to a different satellite 
    
    file_path = '30sats.txt'
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    tle_lines_arr = []
    for i in range(0, len(lines), 2):
        tle_lines_arr.append(lines[i:i+2])

    return tle_lines_arr

read_tle_data()