import os
import json
import pymysql

def lambda_handler(event, context):
    """
    Lambda function that retrieves and returns all healthcare providers 
    from the healthcare_providers table.
    """
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

        # Connect to the database
        print("Connecting to the database...")
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        print("Database connection established.")

        # Check if a specific provider_id was provided in the query parameters
        provider_id = None
        if 'queryStringParameters' in event and event['queryStringParameters']:
            provider_id = event['queryStringParameters'].get('provider_id')

        if provider_id:
            # Retrieve a specific provider
            print(f"Retrieving provider with ID: {provider_id}")
            query = "SELECT * FROM healthcare_providers WHERE provider_id = %s"
            cursor.execute(query, (provider_id,))
            providers = cursor.fetchone()
            
            if not providers:
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'error': 'Provider not found',
                        'provider_id': provider_id
                    })
                }
                
            # Print provider details for logging
            print(f"Provider details: {json.dumps(providers, default=str)}")
            
            # Format the response
            response = {
                'provider': json.loads(json.dumps(providers, default=str))
            }
        else:
            # Retrieve all providers
            print("Retrieving all providers")
            query = "SELECT * FROM healthcare_providers"
            cursor.execute(query)
            providers = cursor.fetchall()
            
            # Print number of providers found
            print(f"Found {len(providers)} providers")
            
            # Print each provider for logging
            for provider in providers:
                print(f"Provider: {json.dumps(provider, default=str)}")
            
            # Format the response
            response = {
                'count': len(providers),
                'providers': json.loads(json.dumps(providers, default=str))
            }

        cursor.close()
        conn.close()
        print("Database connection closed.")

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response)
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Failed to retrieve providers',
                'details': str(e)
            })
        }