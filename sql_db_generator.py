import csv
import mysql.connector
from db_funcs import *
from itertools import chain

db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="test123",
    db="dv2018",
    autocommit=True
)



# generate for each file a dict that contains the requested fields in the faostat format
# taking the first row as the key to store the values under
path = "C:\\Users\\Niels\\Documents\\data visualization\\datasets\\"
#the part of the string that is always the same for faostat normalized data
post_file_name = '_E_All_Data_(Normalized).csv'
extraction_structure = {
    'Population': ['Area', 'Year', 'Unit', 'Value', 'Element'],
    'Production_Crops': ['Area', 'Item', 'Element', 'Year', 'Value', 'Unit'],
    'Production_Livestock': ['Area', 'Item', 'Element', 'Year', 'Value', 'Unit'],
    'Value_of_Production': ['Area', 'Item', 'Year', 'Value', 'Element', 'Unit'],
    'Environment_Emissions_intensities': ['Area', 'Item', 'Year', 'Value', 'Element', 'Unit'],
    'Environment_Energy': ['Country', 'Item', 'Year', 'Value', 'Element', 'Unit'],
    'Environment_Fertilizers': ['Area', 'Item', 'Year', 'Value', 'Element', 'Unit'],
    'Environment_LandUse': ['Area', 'Item', 'Year', 'Value', 'Element', 'Unit'],
    'Environment_Pesticides': ['Area', 'Item', 'Year', 'Value', 'Element', 'Unit'],
    'Environment_Temperature_change': ['Area', 'Year', 'Months', 'Value', 'Element', 'Unit'],
    'Environment_Water': ['Country', 'Item', 'Year', 'Value', 'Element', 'Unit'],


}

# to remove predictions from dataset
cut_off_year = 2018

result = {}

for file_name, requested_columns in extraction_structure.items():
    with open(path+file_name+post_file_name) as file:
        reader = csv.DictReader(file)
        file_result = []
        print(file_name)
        for row in reader:
            if int(row['Year']) <= 2018:
                row_result = {}
                for requested_column in requested_columns:
                    row_result[requested_column] = row[requested_column]

                file_result.append(row_result)
        result[file_name] = file_result



#assuming the db is empty
cursor = db.cursor()
# structure_generator(cursor)

for row in result['Population']:
    add_population_row(cursor, row)

for row in chain(result['Production_Crops'], result['Production_Livestock']):
    add_food_row(cursor, row)

for row in result['Value_of_Production']:
    add_value_of_production_row(cursor, row)

# todo decide what to do with emissions (mismatching Item compared to rest of data)
for row in result['Environment_Emissions_intensities']:
    add_emmissions(cursor, row)

for row in result['Environment_Energy']:
    add_energy(cursor, row)

for row in result['Environment_Fertilizers']:
    add_fertilizers(cursor, row)

for row in result['Environment_LandUse']:
    add_land_use(cursor, row)

for row in result['Environment_Pesticides']:
    add_pesticide_use(cursor, row)

for row in result['Environment_Temperature_change']:
    add_temperature_changes(cursor, row)

for row in result['Environment_Water']:
    add_water_use(cursor, row)

db.commit()
cursor.close()
db.close()

print(result)

