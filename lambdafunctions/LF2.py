import boto3
from botocore.vendored import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth
import json
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    
    # Poll an item from SQS
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/447749499317/Q1'
    # Poll the SQS queue for a message
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        VisibilityTimeout=30,
        WaitTimeSeconds=0
    )

    if 'Messages' in response:
        message = response['Messages'][0]
        message_body=json.loads(message['Body'])
        
        # Extract the cuisine value from the message body
        cuisine = message_body['cuisine'] 
        
        #query elastic search connection
        es_host= 'search-restaurants-x5rqlzhfdeitt5ubcf4ay6uvhy.us-east-1.es.amazonaws.com'
        index_path = '/restaurant/_doc/1/' 
        region = 'us-east-1' 
        service = 'es'
        access_key='AKIAWQP7TFG2SWTBWUV6'
        secret_key='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'

        
        auth = AWSRequestsAuth(aws_access_key=access_key,
                              aws_secret_access_key=secret_key,
                              aws_host=es_host,
                              aws_region='us-east-1',
                              aws_service='es')
        
    
        url = "https://search-restaurants-x5rqlzhfdeitt5ubcf4ay6uvhy.us-east-1.es.amazonaws.com"
        
        # test ES connection
        response = requests.get(url, auth=auth)
        return response.content
        
        url = f'https://{es_host}/restaurants/_search'
        query = {
            "query": {
                "match": {
                    "cuisine": cuisine
                }
            }
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, auth=auth, headers=headers, json=query)
        hits = response.json()['hits']['hits']
        business_ids = [hit['_source']['Business ID'] for hit in hits]

        #query dynamoDB
        dynamodb = boto3.client('dynamodb')
        dbres = []
        # Fetch the details of each business from DynamoDB
        for business_id in business_ids:
            try:
                response = dynamodb.get_item(
                    TableName='yelp-restaurants',
                    Key={
                        'Business ID': {'S': business_id},
                        'cuisine' : {'S':cuisine}
                    }
                )
                dbres.append(response['Item'])
            except ClientError as e:
                return {'statusCode': 500, 'body': f"DynamoDB Error: query for {business_id} failed. {e.response['Error']['Message']}"}
        
        #beautify results
        restaurants = []

        for i, restaurant in enumerate(dbres):
            email = restaurant['email']['S']
            cuisine = restaurant['cuisine']['S']
            location = restaurant['location']['S']
            diningTime = restaurant['diningTime']['S']
            peopleCount = restaurant['peopleCount']['N']
            
            restaurant_info = {
                'email': email,
                'cuisine': cuisine,
                'location': location,
                'diningTime': diningTime,
                'peopleCount': peopleCount,
            }
            
            restaurants.append(restaurant_info)
        
        lines = []
        for i, item in enumerate(restaurants):
            line = f"----------------------------\nRestaurant {i+1}\n----------------------------\n"
            for key, value in item.items():
                line += f"{key}: {value}\n"
            lines.append(line)
        
        email_content = "".join(lines)

        # Use SES to send an email
        ses = boto3.client('ses')
        try:
            response = ses.send_email(
                Destination={
                    'ToAddresses': [
                        'kf1685@nyu.edu',
                    ],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': 'UTF-8',
                            'Data': email_content,
                        },
                    },
                    'Subject': {
                        'Charset': 'UTF-8',
                        'Data': 'Your Restaurant Suggestions Are Ready!',
                    },
                },
                Source='kf1685@nyu.edu',
            )
            
        except ClientError as e:
            return {'statusCode': 500, 'body': f"SES Error: {e.response['Error']['Message']}"}
        
        #delete processed request from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        
        print(f'REQUEST PROCESSED: \n {message}')
        
        return {'statusCode': 200, 'body': 'Email Sent!'}
        
    return {'statusCode':500,'body':'SQS Exception: No Messages Found in Queue.'} 