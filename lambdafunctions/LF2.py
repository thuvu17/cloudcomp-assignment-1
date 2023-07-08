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
        email = message_body['email'] 
        peopleCount = message_body['peopleCount'] 
        diningTime = message_body['diningTime'] 
        date = message_body['date'] 
        
        #query elastic search connection
        es_host= 'search-restaurants-x5rqlzhfdeitt5ubcf4ay6uvhy.us-east-1.es.amazonaws.com'
        index_path = '/restaurant/_doc/1/' 
        region = 'us-east-1' 
        service = 'es'
        access_key='AKIAWQP7TFG26FILF44I'
        secret_key='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'

        
        auth = AWSRequestsAuth(aws_access_key=access_key,
                              aws_secret_access_key=secret_key,
                              aws_host=es_host,
                              aws_region='us-east-1',
                              aws_service='es')
        
    
        url = "https://search-restaurants-x5rqlzhfdeitt5ubcf4ay6uvhy.us-east-1.es.amazonaws.com/restaurant/_search"
        
        # # test ES connection
        # response = requests.get(url, auth=auth)

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
        business_ids = [hit['_source']['restaurant-id'] for hit in hits]

        #query dynamoDB
        dynamodb = boto3.client('dynamodb')
        dbres = []
        # Fetch the details of each business from DynamoDB
        for business_id in business_ids:
            try:
                response = dynamodb.get_item(
                    TableName='yelp-restaurants',
                    Key={
                        'business-id': {'S': business_id}
                    }
                )
                dbres.append(response['Item'])
            except ClientError as e:
                return {'statusCode': 500, 'body': f"DynamoDB Error: query for {business_id} failed. {e.response['Error']['Message']}"}
        
        #beautify results
        restaurants = []

        for i, restaurant in enumerate(dbres):
            name = restaurant['name']['S']
            location = restaurant['address']['M']['display_address']['L']
            address = ''
            for i in range (len(location)):
                address += location[i]['S']
            restaurant_info = {
                'name': name,
                'location': address
            }
            
            restaurants.append(restaurant_info)
        
        lines = ["“Hello! Here are my {} restaurant suggestions for {} people, for {} at {}:\n ".format(cuisine, peopleCount, date, diningTime)]
        for i, item in enumerate(restaurants):
            line = "{}. {}, located at {}\n".format(i + 1, item['name'], item['location'])
            lines.append(line)
        
        email_content = "".join(lines)

        # Use SES to send an email
        ses = boto3.client('ses')
        try:
            response = ses.send_email(
                Destination={
                    'ToAddresses': [
                        email,
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
