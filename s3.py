import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')

s3 = boto3.client('s3')

def display_json_content(json_data, indent_level=0):
    """Helper function to display JSON content in a formatted way"""
    indent = "  " * indent_level
    
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if isinstance(value, (dict, list)):
                print(f"{indent}{key}:")
                display_json_content(value, indent_level + 1)
            else:
                print(f"{indent}{key}: {value}")
    elif isinstance(json_data, list):
        for i, item in enumerate(json_data):
            if isinstance(item, (dict, list)):
                print(f"{indent}[{i}]:")
                display_json_content(item, indent_level + 1)
            else:
                print(f"{indent}[{i}]: {item}")
    else:
        print(f"{indent}{json_data}")

try:
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    
    if 'Contents' in response:
        print(f"Files in bucket '{BUCKET_NAME}':")
        print("=" * 60)
        
        for obj in response['Contents']:
            file_name = obj['Key']
            file_size = obj['Size']
            last_modified = obj['LastModified']
            
            print(f"\n📄 File: {file_name}")
            print(f"   Size: {file_size:,} bytes | Last Modified: {last_modified}")
            print("-" * 60)
            
            try:
                # Download the file content
                file_response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
                file_content = file_response['Body'].read().decode('utf-8')
                
                # Parse JSON content
                json_data = json.loads(file_content)
                
                # Display the JSON content
                print("Content:")
                display_json_content(json_data, indent_level=1)
                
            except json.JSONDecodeError as e:
                print(f"  ⚠️  Error parsing JSON: {str(e)}")
                print(f"  Raw content preview (first 200 chars):")
                print(f"  {file_content[:200]}...")
            except Exception as e:
                print(f"  ⚠️  Error reading file: {str(e)}")
            
            print()  # Empty line between files
    else:
        print(f"No files found in bucket '{BUCKET_NAME}'")
        
except Exception as e:
    print(f"Error accessing bucket '{BUCKET_NAME}': {str(e)}")
    print("Please check that the bucket exists and you have the necessary permissions.")