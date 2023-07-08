import json
import random
import decimal
import boto3

sqs = boto3.client('sqs')
_url = 'https://sqs.us-east-1.amazonaws.com/447749499317/Q1'

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']
    
def get_slot(intent_request, slotName):
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['interpretedValue']
    else:
        return None
        
def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']
    return {}
    
def close(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }
    
def DiningSuggestion(intent_request):
    session_attributes = get_session_attributes(intent_request)
    location = get_slot(intent_request, 'Location')
    date = get_slot(intent_request, 'Date')
    cuisine = get_slot(intent_request, 'Cuisine')
    diningTime = get_slot(intent_request, 'DiningTime')
    peopleCount =get_slot(intent_request, 'NumberOfPeople')
    email = get_slot(intent_request, 'Email')
    # Create a dictionary object from the slot values
    data = {
        'location': location,
        'date': date,
        'cuisine': cuisine,
        'diningTime': diningTime,
        'peopleCount': peopleCount,
        'email': email
    }
    # Convert the dictionary object to a JSON string
    message_body = json.dumps(data)
    print(message_body)
    # # log request in dynamoDB
    # dynamodb = boto3.resource('dynamodb')
    # table = dynamodb.Table('user-last-request')
    # item={'email':email, 'requestDetails': message_body}
    # table.put_item(Item=item)
    # Send the message to the SQS 
    response = sqs.send_message(
        QueueUrl=_url,
        MessageBody=message_body
    )
    text = "You’re all set. Expect my suggestions shortly! Have a good day."
    message =  {
            'contentType': 'PlainText',
            'content': text
        }
    fulfillment_state = "Fulfilled"
    return close(intent_request, session_attributes, fulfillment_state, message)
    
def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    response = None
    # Dispatch to your bot's intent handlers
    print(intent_name)
    if intent_name == 'DiningSuggestionsIntent':
        return DiningSuggestion(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')
    
def lambda_handler(event, context):
    response = dispatch(event)
    return response