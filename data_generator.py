import json
import random
import os
import boto3
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

BUCKET_NAME = os.getenv('BUCKET_NAME')
s3 = boto3.client('s3')

class CallDataGenerator:
    def __init__(self):
        self.agents = [
            "Sarah Johnson", "Michael Chen", "Emily Rodriguez", "David Smith",
            "Lisa Thompson", "Robert Wilson", "Jennifer Davis", "James Brown",
            "Maria Garcia", "Christopher Lee", "Amanda White", "Kevin Martinez"
        ]
        
        self.conversion_phrases = [
            "Yes, I'll book that appointment",
            "Sign me up for that",
            "Let's proceed with the purchase",
            "I'd like to move forward",
            "That sounds perfect, let's do it",
            "I'm ready to buy",
            "Count me in",
            "I'll take it",
            "Let's get started",
            "Perfect, I'm interested"
        ]
        
        self.upsell_phrases = [
            "I'll take the premium package for an extra $50",
            "Add the extended warranty for $25",
            "Include the premium support for $75",
            "I'll upgrade to the deluxe version for $100",
            "Add the insurance package for $30",
            "Include the installation service for $45",
            "I want the premium features for $60",
            "Add the express delivery for $15",
            "Include the maintenance plan for $80",
            "Upgrade to professional tier for $120"
        ]
        
        self.positive_phrases = [
            "This is fantastic",
            "Super helpful",
            "Excellent service",
            "I'm very satisfied",
            "Outstanding support",
            "Really impressed",
            "Great experience",
            "Wonderful product",
            "Amazing quality",
            "Highly recommend this"
        ]
        
        self.negative_phrases = [
            "I'm very frustrated",
            "This is unacceptable",
            "Poor customer service",
            "Very disappointed",
            "Terrible experience",
            "Not happy at all",
            "This is ridiculous",
            "Completely unsatisfied",
            "Waste of time",
            "Very unprofessional"
        ]
        
        self.neutral_phrases = [
            "Hello, I'm calling about",
            "I have some questions",
            "Can you help me with",
            "I'm looking into",
            "I need information about",
            "Could you explain",
            "I'm considering",
            "What are the options for",
            "I'd like to know more about",
            "Can you tell me about"
        ]
        
    def generate_call_transcript(self):
        will_convert = random.random() < 0.3
        will_upsell = random.random() < 0.15 if will_convert else False
        
        if will_convert:
            sentiment_choice = random.choices(
                ['positive', 'neutral', 'negative'],
                weights=[0.7, 0.25, 0.05]
            )[0]
        else:
            sentiment_choice = random.choices(
                ['positive', 'neutral', 'negative'],
                weights=[0.2, 0.5, 0.3]
            )[0]
        
        transcript_parts = []
        
        transcript_parts.append("Agent: Hello, thank you for calling. How can I help you today?")
        transcript_parts.append(f"Customer: {random.choice(self.neutral_phrases)} your services.")
        
        num_exchanges = random.randint(3, 6)
        for i in range(num_exchanges):
            if i == 0:
                transcript_parts.append("Agent: I'd be happy to help you with that. Let me explain our options.")
                transcript_parts.append("Customer: That sounds interesting. Tell me more about the features.")
            else:
                transcript_parts.append(f"Agent: {fake.sentence()}")
                if sentiment_choice == 'positive':
                    if random.random() < 0.4:
                        transcript_parts.append(f"Customer: {random.choice(self.positive_phrases)}. {fake.sentence()}")
                    else:
                        transcript_parts.append(f"Customer: {fake.sentence()}")
                elif sentiment_choice == 'negative':
                    if random.random() < 0.4:
                        transcript_parts.append(f"Customer: {random.choice(self.negative_phrases)}. {fake.sentence()}")
                    else:
                        transcript_parts.append(f"Customer: {fake.sentence()}")
                else:
                        transcript_parts.append(f"Customer: {fake.sentence()}")
        
        if will_convert:
            transcript_parts.append("Agent: Great! So would you like to proceed?")
            transcript_parts.append(f"Customer: {random.choice(self.conversion_phrases)}.")
            
            if will_upsell:
                transcript_parts.append("Agent: Excellent! I also have some additional options that might interest you.")
                transcript_parts.append(f"Customer: {random.choice(self.upsell_phrases)}.")
                transcript_parts.append("Agent: Perfect! I'll add that to your order.")
        else:
            transcript_parts.append("Agent: Is there anything else I can help you with today?")
            transcript_parts.append("Customer: Let me think about it and get back to you.")
        
        transcript_parts.append("Agent: Thank you for your time. Have a great day!")
        transcript_parts.append("Customer: Thank you, goodbye.")
        
        return " ".join(transcript_parts)
    
    def generate_call_record(self):
        call_id = f"CALL_{fake.uuid4()[:8].upper()}"
        agent_name = random.choice(self.agents)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        transcript = self.generate_call_transcript()
        
        return {
            "call_id": call_id,
            "agent_name": agent_name,
            "timestamp": timestamp.isoformat(),
            "transcript_text": transcript
        }
    
    def generate_batch(self, num_calls=100):
        calls_generated = []
        
        for i in range(num_calls):
            call_record = self.generate_call_record()
            
            filename = f"{call_record['call_id']}.json"
            
            json_content = json.dumps(call_record, indent=2)
            
            try:
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=filename,
                    Body=json_content,
                    ContentType='application/json'
                )
                calls_generated.append(call_record)
                
                if (i + 1) % 10 == 0:
                    print(f"Uploaded {i + 1} calls to S3...")
            except Exception as e:
                print(f"Error uploading {filename} to S3: {str(e)}")
        
        print(f"Successfully uploaded {num_calls} call records to S3 bucket '{BUCKET_NAME}'")
        
        return calls_generated

def main():
    generator = CallDataGenerator()
    
    print(f"Generating and uploading synthetic call data to S3 bucket '{BUCKET_NAME}'...")
    calls = generator.generate_batch(num_calls=50)
    
    print(f"\nUploaded {len(calls)} call records to S3")
    if calls:
        print("Sample call:")
        print(json.dumps(calls[0], indent=2))

if __name__ == "__main__":
    main()