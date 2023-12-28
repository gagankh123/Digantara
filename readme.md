Digantara Interview Questions
 
1. satellite_python.py  - This is being used to run the code in pure python. For 30 satellites, it took 13h to complete.
2. satellite_spark.py - This is being used to run the code in spark mode. For 30 satellites, it took 2.38h to complete. I tried multiple options of repartitions, but 8 worked best as my system is 8 core cpu.


Steps to execute:
1. Create a conda environment with the requirements.yml file.   conda env create --name digantara --file requirements.yml
2. Activate Conda environment    conda activate digantara
3. python satellite_spark.py   (make sure all your data should be in same directory)
4. Inputs: 
I) Prod
II) 0.1
III) 5
IV) [[(16.66673, 103.58196), (69.74973, -120.64459), (-21.09096, -119.71009), (-31.32309, -147.79778)]]
V) 8


