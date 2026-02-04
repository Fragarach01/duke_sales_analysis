import subprocess # control inputs & outputs
#from sqlalchemy import create_engine, text
import time
import os

# double check that postgres is running and 
# that the database is ready to accept connections
def wait_for_postgres(host, max_retries=20, delay_seconds=5):

    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                print(f"Postgres is ready on {host}.")
                return True
            else:
                print(f"Postgres not ready, retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)
        except Exception as e:
            print(f"Error checking Postgres readiness: {e}")
            time.sleep(delay_seconds)
        retries += 1
    print(f"Postgres is not ready after {max_retries} retries.")
    return False




# Check if source Postgres is ready
if not wait_for_postgres(host="source_postgres"):
    raise RuntimeError("Source postgres DB is not ready, exiting script.")
    exit(1)  # Exit if Postgres is not ready

# Check if target Postgres is ready
if not wait_for_postgres(host="target_postgres"):
    raise RuntimeError("Target postgres DB is not ready, exiting script.")
    exit(1)  # Exit if Postgres is not ready


# Run the ELT script
print("Running ELT script...")


# Configuration for source and target Postgres databases
source_config = {
    # Use the service name from docker-compose as the hostname
    "host": "source_postgres",
 #   "port": 5432, don't need to specify due to compose file(??)
    "database": "source_db",
    "user": "postgres",
    "password": "secret"
}



target_config = {
    "host": "target_postgres",
    "database": "target_db",
    "user": "postgres",
    "password": "secret"
}

# Prepare the extract command to extract from csv files

def init_tracking():

    # Create tracking table in DB and copy contents to tracking file
    """ with open("track_files.sql", 'w') as f:

        f.write("-- SQL to create table to track loaded files\n" \
        "CREATE TABLE IF NOT EXISTS loaded_files (filename TEXT PRIMARY KEY);\n" \
        "COPY loaded_files (filename) TO '/var/lib/postgresql/sales_data/loaded_files.txt' WITH (FORMAT csv, HEADER FALSE);\n") """

    # Ensure tracking file exists
    if not os.path.exists("sales_data/loaded_files.txt"):
        open("sales_data/loaded_files.txt", 'x').close()
        print("Created loaded_files.txt to track loaded files.")
    
    # Prepare tracking command
    track_init_command = [
    "psql",
    "--host", source_config["host"],
    "--dbname", source_config["database"],
    "--username", source_config["user"],
    "-c", "CREATE TABLE IF NOT EXISTS loaded_files (filename TEXT PRIMARY KEY);",
    "-c", "COPY loaded_files (filename) TO '/var/lib/postgresql/sales_data/loaded_files.txt' WITH (FORMAT csv, HEADER FALSE);"
        ]
    
    # Run the track command to update tracking info file
    try:
        print("Initialising tracking...")
        subprocess.run(
            track_init_command,
            env=subprocess_env,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error during data tracking: {e}")
        pass


def update_tracking():

    # Prepare command to update tracking file from DB
    track_update_command = [
    "psql",
    "--host", source_config["host"],
    "--dbname", source_config["database"],
    "--username", source_config["user"],
    "-c", "COPY loaded_files (filename) TO '/var/lib/postgresql/sales_data/loaded_files.txt' WITH (FORMAT csv, HEADER FALSE);"
        ]
    
    # Run the track command to update tracking info file
    try:
        print("Updating tracking info...")
        subprocess.run(
            track_update_command,
            env=subprocess_env,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error during data tracking update: {e}")
        pass


def get_loaded_files():
    # Get already loaded files from tracking file
    print("Retrieving tracking info...")
    already_loaded = []

    try:
        with open("sales_data/loaded_files.txt", 'r') as f:
            already_loaded = [line.strip() for line in f.readlines()]
    except FileNotFoundError as e:
        print("No loaded_files.txt found, assuming no files have been loaded yet.")

    print("Already loaded files:", already_loaded)

    return already_loaded



def extract_raw_data():

    connection_string = f"postgresql://{source_config['user']}:{source_config['password']}@{source_config['host']}/{source_config['database']}"

 #   engine = create_engine(connection_string)

 #   with engine.connect() as connection:
 #       already_loaded_query = connection.execute(text("SELECT filename FROM loaded_files;"))
 #       already_loaded = [row[0] for row in already_loaded_query.fetchall()]

    # Get already loaded files
    already_loaded = get_loaded_files()

    """ try:
        with open("sales_data/loaded_files.txt", 'r') as f:
            already_loaded = [line.strip() for line in f.readlines()]
    except FileNotFoundError as e:
        print("No loaded_files.txt found, assuming no files have been loaded yet.") """
    

    with open("extract.sql", 'w') as f:

        for file in os.listdir("sales_data"):

            if file.endswith(".csv"):

                if file in already_loaded:
                    print(f"Skipping already loaded file: {file}")
                    continue
                else:
                    print(f"Processing file: {file}")
                    table_name = file.replace(".csv", "")

                    f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (LIKE transactions);\n \copy {table_name} FROM 'sales_data/{file}' WITH (FORMAT csv, HEADER true, LOG_VERBOSITY verbose);\n INSERT INTO transactions (SELECT * FROM {table_name} WHERE date NOT IN (SELECT date FROM transactions));\n INSERT INTO loaded_files (filename) VALUES ('{file}');\n")

                    

                    

                    #with open("sales_data/loaded_files.txt", 'a') as lf:
                    #    lf.write(f"{file}\n")

               # f.write(f"INSERT INTO transactions (SELECT * FROM {table_name} WHERE date NOT IN (SELECT date FROM transactions));\n")

              # f.write(f"INSERT INTO loaded_files (filename) VALUES ('{file}');\n")

            # WHERE NOT(date IN (SELECT date FROM transactions)

    with open("extract.sql", 'r') as f:
        print("Generated extract.sql:")
        print(f.read())


    extract_command = [
    "psql",
    "--host", source_config["host"],
    "--dbname", source_config["database"],
    "--username", source_config["user"],
 #   "-c", ""  # Placeholder for the copy command
    "-f", "extract.sql"
    ]


    


    print("Extracting raw data...")
    # Run the extract command to load data into source Postgres
    try:
        subprocess.run(
            extract_command,
            env=subprocess_env,
            check=True
        )
        print("Data extraction completed.")
     #   return 0
    except subprocess.CalledProcessError as e:
        print(f"Error during data extraction: {e}")
        pass

    # Update tracking info after extraction
    update_tracking()

    # Clean up temporary extract.sql file
    if os.path.exists("extract.sql"):
        os.remove("extract.sql")
        print("Removed temporary extract.sql file.")




# Set the environment variable for PGPASSWORD to avoid password prompt
subprocess_env = dict(PGPASSWORD=source_config["password"])

# Initialise file tracking
init_tracking()

# Extract raw data and load into Postgres
extract_raw_data()




 #   extract_command = [
 #   "psql",
 #   "--host", source_config["host"],
 #   "--dbname", source_config["database"],
#    "--username", source_config["user"],
 #   "-c", ""  # Placeholder for the copy command
 #   "-c", ""
#    ]
    
#    for file in os.listdir("sales_data"):
 #       print(f"Processing file: {file}")

 #       table_name = file.replace(".csv", "")

 #       extract_command[-1] = f"\copy {table_name} FROM 'sales_data/{file}' WITH (FORMAT csv, HEADER true, LOG_VERBOSITY verbose)"
        

        # Run the extract command to load data into source Postgres
 #       try:
 #           subprocess.run(
 #               extract_command,
 #               env=subprocess_env,
 #               check=True
 #           )
 #           print(f"{file} extraction completed.")
 #       except subprocess.CalledProcessError as e:
 #           print(f"Error during {file} extraction: {e}")
 #           pass



# Prepare the pg_dump command to dump data from source Postgres
""" dump_command = [
    "pg_dump",
    "--host", source_config["host"],
    "--dbname", source_config["database"],
    "--username", source_config["user"],
 #   "--password", source_config["password"],
 #   "--format", "c",
    "-f", "/usr/lib/postgresql/17/bin/data_dump.sql",
    "-w",  # Disable password prompt
    "--verbose" 
] """


# Prepare the psql command to load data into target Postgres
""" load_command = [
    "psql",
    "--host", target_config["host"],
    "--dbname", target_config["database"],
    "--username", target_config["user"],
#    -a, 
    "-f", "/usr/lib/postgresql/17/bin/data_dump.sql" 
] """





# Run the pg_dump command to dump data from source Postgres
""" try:
    print("Starting data dump from source Postgres...")
    subprocess.run(
        dump_command,
        env=subprocess_env,
        check=True
    )
    print("Data dump completed.")
except subprocess.CalledProcessError as e:
    print(f"Error during pg_dump: {e}")
    pass  """


""" subprocess_env = dict(PGPASSWORD=target_config["password"])

try:
    print("Starting data load into target Postgres...")
    result = subprocess.run(
        load_command,
        env=subprocess_env,
        check=True
    )
    print("Data load completed.")
except subprocess.CalledProcessError as e:
    print(f"Error during data load: {e}")
    pass """

print("ELT script completed successfully.")