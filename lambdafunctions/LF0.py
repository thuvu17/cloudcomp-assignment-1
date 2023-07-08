import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
lex = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    # Extract the user input text from the input event
    user_input = event['messages'][0]['unstructured']['text']
    # logger.debug(event)
    # Invoke the Lex v2 bot with the user input
    bot_id = 'DUSR3TRW1F'
    bot_alias_id = 'TSTALIASID'
    locale_id = 'en_US'
    response = lex.recognize_text(
        botId=bot_id,
        botAliasId=bot_alias_id,
        localeId=locale_id,
        sessionId='abc1234',
        text=user_input
    )
    # Return the bot response as the output of the Lambda function
    return response