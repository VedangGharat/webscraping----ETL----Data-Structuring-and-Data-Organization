import psycopg2
import pandas as pd
import glob
import os

# Database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'PASS@KEY1',
    'host': 'localhost',  # Change if your database is on a different host
    'port': '5432'        # Change if your PostgreSQL server uses a different port
}

# Path to the directory containing CSV files
csv_dir_path = "/Users/vedanggharat/Movies/LinkedIn Jobs/jobs_scraped_files/"  # Directory containing multiple CSV files

# SQL command to create a table for job links
create_table_command = """
CREATE TABLE IF NOT EXISTS job_links(
    link TEXT PRIMARY KEY
);
"""

# SQL command to insert data into job_links table
insert_data_command = """
INSERT INTO job_links (link)
VALUES (%s)
ON CONFLICT (link) DO NOTHING;
"""

def create_table():
    """Create the job_links table in the PostgreSQL database."""
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(create_table_command)
        print("Table created successfully.")
        connection.commit()
    except Exception as error:
        print(f"Error creating table: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def load_csv_and_insert_data():
    """Load data from multiple CSV files and insert unique data into the PostgreSQL table."""
    connection = None
    cursor = None
    try:
        # Collect all CSV file paths from the specified directory
        csv_files = glob.glob(os.path.join(csv_dir_path, "*.csv"))

        # Establish a connection to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Fetch existing links from the database
        cursor.execute("SELECT link FROM job_links;")
        existing_links = set(row[0] for row in cursor.fetchall())

        # Iterate over each CSV file
        for csv_file_path in csv_files:
            print(f"Processing file: {csv_file_path}")
            # Load CSV data into a pandas DataFrame
            df = pd.read_csv(csv_file_path)

            # Ensure that the DataFrame has the correct columns
            required_columns = {'joblinks'}
            if not required_columns.issubset(df.columns):
                raise ValueError(f"CSV file must contain the following columns: {required_columns}")

            # Filter out rows that already exist in the database
            new_links = df['joblinks'].astype(str).tolist()
            unique_links = set(new_links) - existing_links

            # Convert unique data to list of tuples for insertion
            data_to_insert = [(link,) for link in unique_links]

            # Insert unique data into the table
            if data_to_insert:
                cursor.executemany(insert_data_command, data_to_insert)
                print(f"{len(data_to_insert)} new rows inserted from file {csv_file_path}.")

                # Commit the transaction
                connection.commit()

                # Update the existing_links set
                existing_links.update(unique_links)

    except Exception as error:
        print(f"Error inserting data: {error}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    create_table()  # Create the table if it doesn't exist
    load_csv_and_insert_data()  # Load data from CSV files and insert unique data into the table
