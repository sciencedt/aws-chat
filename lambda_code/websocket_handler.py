import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
connection_table_name = "connections"
# Get reference to the table
con_table = dynamodb.Table(connection_table_name)
inbox_table = dynamodb.Table("inbox")
message_table = dynamodb.Table("messages")


def generate_thread_id(user_id_1, user_id_2):
    """
    Generate a consistent thread ID for two users (sorted by user_id to avoid duplication).
    """
    return f"thread#{min(user_id_1, user_id_2)}#{max(user_id_1, user_id_2)}"

def extract_connection_and_user(sk_value):
    # Split the SK string by '#'
    parts = sk_value.split('#')
    
    # Check the expected format of the parts and extract values
    if len(parts) >= 5:
        connection_id = parts[2]  # 3rd part (index 2) should be the connection_id
        user = parts[4]           # 5th part (index 4) should be the user
        return connection_id, user
    else:
        raise ValueError("SK format is invalid. Expected format: '#conn#{connection_id}#user#{user}'")

def extract_user_and_connection(sk_value):
    # Split the SK string by '#'
    parts = sk_value.split('#')
    
    # Check the expected format of the parts and extract values
    if len(parts) >= 5:
        user = parts[2]  # 3rd part (index 2) should be the user
        connection_id = parts[4]           # 5th part (index 4) should be the user
        return user, connection_id
    else:
        raise ValueError("SK format is invalid. Expected format: '#conn#{connection_id}#user#{user}'")

def lambda_handler(event, context):
    route_key = event.get('requestContext', {}).get('routeKey', None)
    if route_key == '$connect':
        return handle_connect(event)
    elif route_key == '$disconnect':
        return handle_disconnect(event)
    elif route_key == "get_messages":
        return get_messages(event)
    else:
        return handle_default(event)

def handle_connect(event):
    connection_id = event['requestContext']['connectionId']
    user = event['queryStringParameters']['user']
    item = {'PK': "#conn", 'SK': f"#conn#{connection_id}#user#{user}"}
    item_r = {'PK': "#conn", 'SK': f"#user#{user}#conn#{connection_id}"}
    try:
        response = con_table.put_item(Item=item)
        response = con_table.put_item(Item=item_r)
        print(f"Connection Established user: {user}, connection_id: {connection_id} ")
        return {
            'statusCode': 200,
            'body':  json.dumps('Record inserted successfully')
        }
    
    except Exception as e:
        # Handle potential errors
        return {
            'statusCode': 500,
            'body':  json.dumps(f"Error inserting record: {str(e)}")
        }
def handle_disconnect(event):
    # Extract connectionId from the event
    connection_id = event['requestContext']['connectionId']
    print(f"Get Connection ID as {connection_id}")
    try:
        # Query the table for all items with the given PK
        response = con_table.query(
            KeyConditionExpression=Key('PK').eq("#conn")& Key('SK').begins_with(f"#conn#{connection_id}#")
        )
        print("RESp", response)

        print(f"Items found for deletion: {response['Items']}")  # Debugging output

        # Delete each item that matches the condition
        for item in response['Items']:
            connection_id, user = extract_connection_and_user(item['SK'])
            con_table.delete_item(
                Key={
                    'PK': item['PK'],  # Use the PK
                    'SK': item['SK']   # Use the matching SK
                }
            )
            con_table.delete_item(
                Key={
                    'PK': item['PK'],  # Use the PK
                    'SK': f"#user#{user}#conn#{connection_id}"   # Use the matching SK
                }
            )
        
        print(f"Connection Disconnected user: {user}, connection_id: {connection_id} ")
        return {
            'statusCode': 200,
            'body': json.dumps('Record(s) deleted successfully')
        }
    
    except Exception as e:
        # Handle potential errors
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error deleting record: {str(e)}")
        }

def handle_default(event):
    # Handle all other routes
    print(f"Received message: {event}")
    connection_id = event['requestContext']['connectionId']
    # user = event['queryStringParameters']['user']
    msg_body = json.loads(event['body'])
    # {"to": "second", "from":"first", "message": "hi hello, how are you"}
    to_user = msg_body.get("to")
    from_user = msg_body.get("from")
    # Generate a unique thread ID based on user IDs (you can hash it to make it unique)
    thread_id = generate_thread_id(from_user, to_user)
    # Generate a unique message ID (timestamp or UUID can be used)
    message_id = f"msg#{datetime.utcnow().isoformat()}"
        # Insert the message in the message table
    message_item = {
        "PK": thread_id,            # Thread ID for both users
        "SK": message_id,           # Unique message ID
        "sender_id": from_user,
        "receiver_id": to_user,
        "content": msg_body.get("message"),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Save the message to the message table
    message_table.put_item(Item=message_item)
    # Update both users' inboxes with the message thread info
    update_inbox(from_user, to_user, thread_id, msg_body.get("message"))
    update_inbox(to_user, from_user, thread_id, msg_body.get("message"))
    print(f"Quering for #user#{to_user}#")
    response = con_table.query(
            KeyConditionExpression=Key('PK').eq("#conn")& Key('SK').begins_with(f"#user#{to_user}#")
        )
    print(response)
    to_connection_id = None
    for item in response['Items']:
        print(item)
        user, to_connection_id = extract_user_and_connection(item['SK'])
        break
       # Send the message
    try:
        print(f"Intiating Send message: {to_connection_id}, {msg_body}, {event}")
        response = send_message(to_connection_id, message_item, event)
        return {
            'statusCode': 200,
            'body': json.dumps(f"Message sent to connection: {connection_id}")
        }
    
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Failed to send message: {e.response['Error']['Message']}")
        }
def send_message(connection_id, message, event):
    # Get the API Gateway Management API endpoint from the event
    domain_name = event['requestContext']['domainName']
    stage = event['requestContext']['stage']
    
    # Construct the API Gateway endpoint
    api_gateway_management_url = f"https://{domain_name}/{stage}"
    
    # Create a client for the API Gateway Management API
    apigw_client = boto3.client('apigatewaymanagementapi', endpoint_url=api_gateway_management_url)
    
    try:
        if connection_id:
            # Send the message to the connection ID
            apigw_client.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps(message)  # The data must be a string or binary data
            )
            print(f"Message sent to connection: {connection_id}")
        else:
            print("Receiver is not on line skipping sending to socket")
    
    except ClientError as e:
        # Handle potential errors
        print(f"Error sending message: {str(e)}")

def update_inbox(user_id, other_user_id, thread_id, message_content):
    """
    Update the inbox table for the user.
    user_id: The user whose inbox we are updating.
    other_user_id: The other user involved in the conversation.
    thread_id: The conversation thread ID.
    message_content: The latest message to show in the inbox preview.
    """
    inbox_item = {
        "PK": f"user#{user_id}",         # Primary key for the user
        "SK": f"thread#{thread_id}",     # Unique thread ID for this conversation
        "other_user_id": other_user_id,  # The other person in the conversation
        "last_message": message_content, # Store the latest message
        "timestamp": datetime.utcnow().isoformat()  # Last message timestamp
    }

    # Update the user's inbox
    inbox_table.put_item(Item=inbox_item)

def get_messages(event):
    """
    Get all messages between two users.
    """
    user_id = event["user_id"]
    other_user_id = event["other_user_id"]

    # Get the thread ID based on the user combination
    thread_id = generate_thread_id(user_id, other_user_id)

    try:
        # Query the message table to get all messages in this thread
        response = message_table.query(
            KeyConditionExpression=Key('PK').eq(thread_id)
        )

        messages = response.get("Items", [])

        return {
            "statusCode": 200,
            "body": json.dumps(messages)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error retrieving messages: {str(e)}")
        }
