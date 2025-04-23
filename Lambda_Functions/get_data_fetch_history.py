import os
import json
import pymysql

def lambda_handler(event, context):
    """
    Lambda function that retrieves data fetch history records
    from the data_fetch_history table.
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

        # Parse query parameters
        query_params = event.get('queryStringParameters', {}) or {}
        fetch_id = query_params.get('fetch_id')
        provider_id = query_params.get('provider_id')
        group_id = query_params.get('group_id')
        status = query_params.get('status')
        include_provider_details = query_params.get('include_provider_details') == 'true'
        
        # Build the query based on parameters
        base_query = "SELECT * FROM data_fetch_history"
        where_clauses = []
        params = []
        
        if fetch_id:
            where_clauses.append("fetch_id = %s")
            params.append(fetch_id)
            
        if provider_id:
            where_clauses.append("provider_id = %s")
            params.append(provider_id)
            
        if group_id:
            where_clauses.append("group_id = %s")
            params.append(group_id)
            
        if status:
            where_clauses.append("status = %s")
            params.append(status)
            
        # Add WHERE clause if any filters were applied
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
            
        # Add order by most recent first
        base_query += " ORDER BY fetch_time DESC"
        
        # Execute the query
        print(f"Executing query: {base_query} with params: {params}")
        cursor.execute(base_query, params)
        fetch_records = cursor.fetchall()
        
        # Print number of records found
        print(f"Found {len(fetch_records)} data fetch history records")
        
        # If requested, include provider details for each record
        if include_provider_details and fetch_records:
            # Get all unique provider IDs
            provider_ids = list(set(record['provider_id'] for record in fetch_records))
            
            # Query provider details
            provider_query = "SELECT provider_id, provider_name, provider_type FROM healthcare_providers WHERE provider_id IN ({})".format(
                ','.join(['%s'] * len(provider_ids))
            )
            cursor.execute(provider_query, provider_ids)
            providers = {p['provider_id']: p for p in cursor.fetchall()}
            
            # Attach provider details to each fetch record
            for record in fetch_records:
                provider_id = record['provider_id']
                if provider_id in providers:
                    record['provider_details'] = providers[provider_id]
        
        # Format the response
        if fetch_id and fetch_records:
            # Single record response
            response = {
                'data_fetch': json.loads(json.dumps(fetch_records[0], default=str))
            }
        else:
            # Multiple records response
            response = {
                'count': len(fetch_records),
                'data_fetch_history': json.loads(json.dumps(fetch_records, default=str))
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
                'error': 'Failed to retrieve data fetch history',
                'details': str(e)
            })
        }