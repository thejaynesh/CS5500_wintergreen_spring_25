import os
import json
import pymysql

def initialize_tables():
    # Database configuration without specifying the database
    db_config = {
        'host': os.environ['HOST'],
        'user': os.environ['USER_NAME'],
        'password': os.environ['PASSWORD'],
        'port': int(3306),
        'cursorclass': pymysql.cursors.DictCursor
    }

    # Database name from environment variables
    database_name = os.environ['DB_NAME']

    try:
        print("Connecting to MySQL to check/create database...")
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`;")
        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

        print(f"Database '{database_name}' is ready.")

    except Exception as e:
        print("Error creating database:", e)

    # Now that the database exists, proceed to initialize the tables
    try:
        # Add the database name to the configuration
        db_config['database'] = database_name
        print("Connecting to MySQL to initialize tables...")
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        # Drop existing tables in the correct order (respecting foreign key constraints)
        print("Dropping existing tables if they exist...")
        
        # First drop the tables with foreign key dependencies
        cursor.execute("DROP TABLE IF EXISTS data_fetch_history;")
        print("data_fetch_history table dropped if it existed.")
        
        # Then drop the main tables
        cursor.execute("DROP TABLE IF EXISTS healthcare_providers;")
        print("healthcare_providers table dropped if it existed.")
        
        cursor.execute("DROP TABLE IF EXISTS ehr_systems;")
        print("ehr_systems table dropped if it existed.")
        
        # Create healthcare_providers table with updated schema
        create_healthcare_providers_table = """
            CREATE TABLE healthcare_providers (
              provider_id VARCHAR(36) NOT NULL DEFAULT (UUID()),
              provider_name VARCHAR(255) NOT NULL,
              provider_type ENUM('Hospital', 'Clinic', 'Private Practice', 'Specialist Center', 'Other') NOT NULL,
              contact_email VARCHAR(255) NOT NULL,
              contact_phone VARCHAR(20) NOT NULL,
              address TEXT,
              ehr_id VARCHAR(36),
              tenant_id VARCHAR(255),
              bulk_fhir_url VARCHAR(255),
              secret_name VARCHAR(255),
              onboarded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              last_data_fetch TIMESTAMP DEFAULT NULL,
              status ENUM('Active', 'Inactive', 'Pending', 'Error') NOT NULL DEFAULT 'Pending',
              notes TEXT,
              PRIMARY KEY (provider_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        # Create ehr_systems table
        create_ehr_systems_table = """
            CREATE TABLE ehr_systems (
              ehr_id VARCHAR(36) NOT NULL DEFAULT (UUID()),
              ehr_name VARCHAR(255) NOT NULL,
              documentation_link VARCHAR(255),
              authorization_url VARCHAR(255),
              connection_url VARCHAR(255),
              description TEXT,
              is_supported BOOLEAN,
              is_tenant_id_required BOOLEAN DEFAULT FALSE,
              added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (ehr_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        # Create data_fetch_history table
        create_data_fetch_history_table = """
            CREATE TABLE data_fetch_history (
              fetch_id VARCHAR(36) NOT NULL DEFAULT (UUID()),
              provider_id VARCHAR(36) NOT NULL,
              group_id VARCHAR(255),
              fetch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              status ENUM('Success', 'Partial', 'Failed') NOT NULL DEFAULT 'Success',
              s3_location VARCHAR(255),
              error_details TEXT,
              PRIMARY KEY (fetch_id),
              FOREIGN KEY (provider_id) REFERENCES healthcare_providers(provider_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        print("Creating fresh tables...")
        cursor.execute(create_healthcare_providers_table)
        print("healthcare_providers table created.")
        
        cursor.execute(create_ehr_systems_table)
        print("ehr_systems table created.")
        
        cursor.execute(create_data_fetch_history_table)
        print("data_fetch_history table created.")
        
        # Insert Athena Health EHR system
        athena_health_insert = """
            INSERT INTO ehr_systems (
                ehr_name, documentation_link, authorization_url, connection_url, 
                description, is_supported, is_tenant_id_required
            ) VALUES (
                'Athena Health',
                'https://docs.athenahealth.com/api/guides/overview',
                'https://api.preview.platform.athenahealth.com/oauth2/v1/token',
                'api.preview.platform.athenahealth.com',
                'Athena health EHR system',
                true,
                false
            );
        """
        cursor.execute(athena_health_insert)
        print("Athena Health EHR system inserted.")
        
        conn.commit()

        cursor.close()
        conn.close()

        print("All tables have been successfully initialized.")

    except Exception as e:
        print("Error initializing tables:", e)

# Run the table initialization when the module is loaded
initialize_tables()

def lambda_handler(event, context):
    try:
        # Database connection settings from environment variables
        db_config = {
            'host': os.environ['HOST'],
            'user': os.environ['USER_NAME'],
            'password': os.environ['PASSWORD'],
            'database': os.environ['DB_NAME'],
            'port': int(3306),
            'cursorclass': pymysql.cursors.DictCursor
        }

        print("Connecting to the database in lambda_handler...")
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        print("Database connection established.")

        # Execute a "SHOW TABLES" query to list current tables
        print("Executing SHOW TABLES query to list current tables...")
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        print("Current tables in the database:", tables)

        # Describe each table's structure
        for table_row in tables:
            # Extract the table name from the dictionary
            table_name = list(table_row.values())[0]
            print(f"Describing table structure for: {table_name}")
            query = f"DESCRIBE `{table_name}`;"
            cursor.execute(query)
            structure = cursor.fetchall()
            print(f"Structure of table '{table_name}':", structure)

        cursor.close()
        conn.close()
        print("Database connection closed.")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Database tables initialized and verified successfully.'})
        }

    except Exception as e:
        print("Error in lambda_handler:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'An error occurred: {str(e)}'})
        }