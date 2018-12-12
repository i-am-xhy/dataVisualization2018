
def structure_generator(cursor):
    cursor.executemany('''
CREATE TABLE Years
(
  Year INT NOT NULL,
  PRIMARY KEY (Year)
);

CREATE TABLE Countries
(
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Country)
);

CREATE TABLE Population
(
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Type VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE FoodGroups
(
  FoodGroupId INT NOT NULL,
  Name INT NOT NULL,
  PRIMARY KEY (FoodGroupId)
);

CREATE TABLE Foods
(
  Amount INT NOT NULL,
  Type VARCHAR(100) NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE TemperatureChanges
(
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Type VARCHAR(200) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE LandUsagePercentages
(
  Type VARCHAR(200) NOT NULL,
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE Fertilizers
(
  Unit VARCHAR(100) NOT NULL,
  Type VARCHAR(200) NOT NULL,
  Amount INT NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE Emissions
(
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Type VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Type, Year, Country) REFERENCES Foods(Type, Year, Country)
);

CREATE TABLE ValueOfProduction
(
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Type VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Type, Year, Country) REFERENCES Foods(Type, Year, Country)
);

CREATE TABLE Pesticides
(
  Type VARCHAR(200) NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Amount INT NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE WaterUsage
(
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Type VARCHAR(200) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  PRIMARY KEY (Type, Year, Country),
  FOREIGN KEY (Year) REFERENCES Years(Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country)
);

CREATE TABLE Energy
(
  Type VARCHAR(200) NOT NULL,
  Amount INT NOT NULL,
  Unit VARCHAR(100) NOT NULL,
  Country VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  PRIMARY KEY (Type, Country, Year),
  FOREIGN KEY (Country) REFERENCES Countries(Country),
  FOREIGN KEY (Year) REFERENCES Years(Year)
);

CREATE TABLE FoodGroupings
(
  FoodGroupId INT NOT NULL,
  Type VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Country VARCHAR(100) NOT NULL,
  FOREIGN KEY (FoodGroupId) REFERENCES FoodGroups(FoodGroupId),
  FOREIGN KEY (Type, Year, Country) REFERENCES Foods(Type, Year, Country)
);
 ''', None)


def add_country_if_missing(cursor, country):
    # result = cursor.execute('select * from Countries where Country=%s' % country)
    # if cursor.rowcount == 0:
    cursor.execute("""INSERT IGNORE INTO Countries(Country) VALUES (%s)""", (country,))


def add_year_if_missing(cursor, year):
    cursor.execute("""INSERT IGNORE INTO years(year) VALUES (%s)""", (year,))

def add_missing_foreign_keys(cursor, row):
    year = int(row['Year'])
    add_year_if_missing(cursor, year)
    add_country_if_missing(cursor, row['Area'])

def add_population_row(cursor, row):
    if row['Item'] == 'Population - Est. & Proj.':
        add_missing_foreign_keys(cursor, row)
        population = int(float(row['Value']) * 1000)  # times 1000 to compensate for the unit being in thousands
        cursor.execute('''INSERT INTO Population(Amount, Country, Year, Unit, Type) VALUES (%s, %s, %s, %s, %s) ''',
                       (population, row['Area'], int(row['Year']), row['Unit'], row['Element']))

def add_food_row(cursor, row):
    # aggregate columns to allow all units be stored
    if row['Unit'] == 'tonnes':
        add_missing_foreign_keys(cursor, row)
        cursor.execute('''INSERT IGNORE INTO Foods(Amount, Type, Unit, Year, Country) VALUES (%s, %s, %s, %s, %s)''',
                   (row['Value'], row['Item'], row['Unit'], row['Year'], row['Area']))

def add_value_of_production_row(cursor, row):
    try:
        if row['Element'] == 'Gross Production Value (constant 2004-2006 1000 I$)':
            cursor.execute('''INSERT INTO ValueOfProduction(Amount,Unit, Type, Year, Country) VALUES (%s, %s, %s, %s, %s)''',
                           (row['Value'], row['Unit'], row['Item'], row['Year'], row['Area']))
    except:
        pass

def add_emmissions(cursor, row):
    if row['Element'] == 'Emissions (CO2eq)':
        add_missing_foreign_keys(cursor, row)
        try:
            cursor.execute('''INSERT INTO Emissions(Amount, Unit, Type, Year, Country) VALUES (%s, %s, %s, %s, %s)''',
                   (row['Value'], row['Unit'], row['Item'], row['Year'], row['Area']))
        except:
            # print("this emissions' foodstuff may be mislabeled in faostat")
            # print(row)
            pass

def add_energy(cursor, row):
    try:
        cursor.execute('''INSERT INTO Energy(Type, Country, Year, Amount, Unit) VALUES (%s, %s, %s, %s, %s)''',
                       (row['Item'], row['Country'], row['Year'], row['Value'], row['Unit']))
    except:
        pass

def add_fertilizers(cursor, row):
    try:
        cursor.execute('''INSERT INTO Fertilizers(Amount, Unit, Type, Year, Country) VALUES (%s, %s, %s, %s, %s)''',
                       (row['Value'], row['Unit'], row['Item'], row['Year'], row['Area']))
    except:
        pass

def add_land_use(cursor, row):
    if row['Element'] is 'Share in Agricultural land':
        cursor.execute('''INSERT INTO LandUsagePercentages(Year, Country, Type, Amount, Unit) VALUES (%s, %s, %s, %s, %s)''',
                       (row['Year'], row['Area'], row['Item'], row['Value'], row['Unit']))

def add_pesticide_use(cursor, row):

    cursor.execute('''INSERT INTO Pesticides(Year, Country, Type, Unit, Amount) VALUES (%s, %s, %s, %s, %s)''',
                       (row['Year'], row['Area'], row['Item'], row['Unit'], row['Value']))

def add_temperature_changes(cursor, row):

    if row['Element']=='Standard Deviation' and row['Unit'] == '�C':
        add_missing_foreign_keys(cursor, row)
        cursor.execute('''INSERT INTO TemperatureChanges(Country, Year, Type, Amount, Unit) VALUES (%s, %s, %s, %s, %s)''',
                           (row['Area'], row['Year'], row['Months'], row['Value'], row['Unit']))

def add_water_use(cursor, row):
    try:
        cursor.execute('''INSERT INTO WaterUsage(Country, Year, Type, Amount, Unit) VALUES (%s, %s, %s, %s, %s)''',
                    (row['Country'], row['Year'], row['Item'], row['Value'], row['Unit']))
    except:
        pass
