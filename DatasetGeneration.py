import random
import names
from faker import Faker
from datetime import datetime, timedelta
import json
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
from itertools import count
from dateutil.relativedelta import relativedelta
import pandas as pd

# Load JSON configuration
with open("config.json") as f:
    config_data = json.load(f)

fake = Faker('en_US')

# Define the range of dates
start_date = datetime.strptime(config_data["date"]["startdate"], "%Y, %m, %d")
end_date = datetime.strptime(config_data["date"]["enddate"], "%Y, %m, %d")

# increment according to the date_order
if config_data["date"]["date_order"] == "daily" or config_data["date"]["date_order"] == "monthly":
    i = int(config_data["date"]["incrementDateByNumber"])
elif config_data["date"]["date_order"] == "weekly": 
    i = int(config_data["date"]["incrementDateByNumber"]) * 7

# counter function for date increment
count_date = count(start=0, step=i)

# function for generating date according to the date_order 
def date_generator(start, end):
    if config_data["date"]["date_order"] == "random":
        delta = end - start
        random_days = random.randint(0, delta.days)
        random_date = start + timedelta(days=random_days)
        final_date = random_date.strftime("%Y-%m-%d")
        return final_date
    elif config_data["date"]["date_order"] == "daily" or config_data["date"]["date_order"] == "weekly":
        delta = start
        delta += timedelta(days=next(count_date))
        return delta
    elif config_data["date"]["date_order"] == "monthly":
        delta = start
        delta += relativedelta(months=next(count_date))
        return delta

df = pd.DataFrame()

# Initialize the counter for id
counter = count(start=1, step=1)
coun = count(start=1, step=1)
# Generate data based on data_types
data_types = {
    "id": lambda: next(counter),
    "firstname": lambda: names.get_first_name(),
    "lastname": lambda: names.get_last_name(),
    "gender": lambda: random.choice(["Male", "Female"]),
    "incomerange": lambda: random.choice(["High", "Medium", "Low"]),
    "internettype": lambda: random.choice(["Cable", "DSL", "Fiber Optic"]),
    # "contract": lambda: random.choice(["Month-to-Month", "One Year", "Two Year"]),
    "contract": lambda: random.choice([ i for i in [["Month-to-Month"], ["Year Basis"]][int(next(coun))%2]]),
    "payment_method": lambda: random.choice(["Bank Withdrawal", "Credit Card", "Mailed Check"]),
    "city": lambda: random.choice(["Tomorrowland","Los Vegas","Riverside","Summerside"]),
    "churn_category": lambda: random.choice(["Competitor","Dissatisfaction","Price","Other","Attitude"]),
    # "under_30": lambda: random.choice(["Yes", "No"]),
    "under_30": lambda: random.choice([ i for i in [["Yes"], ["No"]][int(next(coun))%2]]),
    "offer": lambda: random.choice(["Offer A", "Offer B", "Offer C"]),
    "int": lambda: random.randint(1, 100),
    "float": lambda: random.uniform(4000.0, 25000.0).__round__(2),
    "str": lambda: fake.word(),
    "bool": lambda: random.choice([True, False]),
    "date": lambda: date_generator(start_date, end_date)
}

# Declare a base class for declarative models
Base = declarative_base()

# Define the data model
class GeneratedData(Base):
    __tablename__ = 'generated_data'

    # Define columns based on config_data["thisdict"]
    id = Column(Integer, primary_key=True)
    firstname = Column(String(50))
    lastname = Column(String(50))
    gender = Column(String(10))
    incomerange = Column(String(10))
    _int = Column(Integer)
    _float = Column(Float)
    _str = Column(String(50))
    _bool = Column(Boolean)
    _date = Column(Date)

# Determine operation from config
operation = config_data["operation"]

# Creating list of column names.
lst = {}

i = 1
for x, y in config_data["thisdict"].items(): # Storing data from thisdict in lst
    lst[x] = []
    for col in range(y):
        lst[x].append(fake.word() + f"_{i}_{x}")
        i += 1

for colName in config_data["colname"]:   # Storing data from colname in list
    lst[colName] += config_data["colname"][colName]

# When Operation is save as CSV
if operation == "csv":
    output_file = config_data["tableName"]
    num_rows = config_data["num_rows"]
    for x, y in lst.items():
        for col in y:
            df[col] = [data_types[x]() for _ in range(num_rows)]

    df.to_csv(output_file, index=False)

    print(f"Generated data saved in {output_file}")

elif operation == "oracle" or operation == "postgre":
    db_type = operation + "_db"
    db_config = config_data[db_type]

    if operation == "oracle":
        # Establish Oracle database connection using sqlalchemy
        connection_string = f"oracle+cx_oracle://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['service']}"
        engine = create_engine(connection_string)

    elif operation == "postgre":
        # Establish PostgreSQL database connection
        connection = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["username"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cursor = connection.cursor()

        # Generate the dynamic CREATE SCHEMA SQL statement

        create_schema_sql = "CREATE SCHEMA IF NOT EXISTS dataset_schema;"
        cursor.execute(create_schema_sql)
        print("Create schema query sucessfully executed")

        # Generate the dynamic CREATE TABLE SQL statement
        tableName = config_data["tableName"]
        create_table_sql = f"CREATE TABLE IF NOT EXISTS dataset_schema.{tableName} ("
        for x, y in lst.items():
            for col in y: 
                create_table_sql += f"{col} {config_data['datatype'][x]}, "
        create_table_sql = create_table_sql.rstrip(", ") + ");"

        print(create_table_sql)
        # Create the table if it doesn't exist
        cursor.execute(create_table_sql)
        print("create table query executed sucessfully")

        connection.commit()
        print("connection commited sucessfully")
        # cursor.close()
        # print("cursor closed sucessfully")
        # connection.close()
        # print("connection closed sucessfully")

    # Create the table if it doesn't exist
    if operation == "oracle":
        cursor.execute(create_table_sql)

        # Generate and insert data for Oracle
        num_rows = config_data["num_rows"]
        lst = config_data["colname"]

        for _ in range(num_rows):
            data = {col: data_types(data_type) for col, data_type in lst.items()}
            psycopg2.session.add(GeneratedData(**data))

        # psycopg2.session.commit()
        # psycopg2.session.close()

        print("Data inserted into the Oracle database.")

    elif operation == "postgre":
        # Generate and insert data for PostgreSQL
        num_rows = config_data["num_rows"]
        # lst = config_data["colname"]
        
        for _ in range(num_rows):
            values = [data_types(data_type) for data_type in config_data["thisdict"].values()]
            insert_sql = f"INSERT INTO dataset_schema.{tableName} ({', '.join(lst)}) VALUES ({', '.join(['%s'] * len(lst))})"
            cursor.execute(insert_sql, values)

        connection.commit()
        cursor.close()
        connection.close()

        print("Data inserted into the PostgreSQL database.")

    else:
        print("Invalid operation specified in config.json.")
    

    # Close the database connection (for PostgreSQL)
    # if operation == "postgre":
        # connection.commit()
        # cursor.close()
        # connection.close()

    print(f"Data inserted into the {operation.capitalize()} database.")
else:
    print("Invalid operation specified in config.json.")
