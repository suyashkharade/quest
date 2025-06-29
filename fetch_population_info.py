import requests
import boto3

response = requests.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population')
s3_client = boto3.client('s3', aws_access_key_id='AKIAYSRBUYSI7CINTAUT',
                             aws_secret_access_key='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',
                             region_name='ap-south-1')
s3_client.put_object(Bucket='bls-data-assgn', Key='population_data/data.json', Body=response.text.encode('utf-8'))

print(response.text)