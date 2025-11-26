import subprocess # control inputs & outputs
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



def extract_raw_data():

    print("Extracting raw data...")

    extract_command = [
    "psql",
    "--host", source_config["host"],
    "--dbname", source_config["database"],
    "--username", source_config["user"],
    "-c", ""  # Placeholder for the copy command
    ]
    
    for file in os.listdir("sales_data"):
        print(f"Processing file: {file}")

        extract_command[-1] = f"\copy transactions FROM 'sales_data/{file}' WITH (FORMAT csv, HEADER true, LOG_VERBOSITY verbose)"
        

        # Run the extract command to load data into source Postgres
        try:
            subprocess.run(
                extract_command,
                env=subprocess_env,
                check=True
            )
            print(f"{file} extraction completed.")
        except subprocess.CalledProcessError as e:
            print(f"Error during {file} extraction: {e}")
            pass


# Prepare the pg_dump command to dump data from source Postgres
dump_command = [
    "pg_dump",
    "--host", source_config["host"],
    "--dbname", source_config["database"],
    "--username", source_config["user"],
 #   "--password", source_config["password"],
 #   "--format", "c",
    "-f", "/usr/lib/postgresql/17/bin/data_dump.sql",
    "-w",  # Disable password prompt
    "--verbose"
]

# Prepare the psql command to load data into target Postgres
load_command = [
    "psql",
    "--host", target_config["host"],
    "--dbname", target_config["database"],
    "--username", target_config["user"],
 #   "-a",
    "-f", "/usr/lib/postgresql/17/bin/data_dump.sql"
]

# Set the environment variable for PGPASSWORD to avoid password prompt
subprocess_env = dict(PGPASSWORD=source_config["password"])

# First extract raw data into source Postgres
extract_raw_data()

# Run the pg_dump command to dump data from source Postgres
try:
    print("Starting data dump from source Postgres...")
    subprocess.run(
        dump_command,
        env=subprocess_env,
        check=True
    )
    print("Data dump completed.")
except subprocess.CalledProcessError as e:
    print(f"Error during pg_dump: {e}")
    pass


subprocess_env = dict(PGPASSWORD=target_config["password"])

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
    pass

print("ELT script completed successfully.")