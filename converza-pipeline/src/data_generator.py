"""
Synthetic Call Data Generator for Converza Performance Pipeline
Generates realistic call transcripts with conversion, upsell, and sentiment indicators
"""

import json
import random
from datetime import datetime, timedelta
from faker import Faker
import os
from pathlib import Path

fake = Faker()

class CallDataGenerator:
    def __init__(self):
        self.agents = [
            "Sarah Johnson", "Michael Chen", "Emily Rodriguez", "David Smith",
            "Lisa Thompson", "Robert Wilson", "Jennifer Davis", "James Brown",
            "Maria Garcia", "Christopher Lee", "Amanda White", "Kevin Martinez"
        ]
        
        # Conversion phrases that indicate a successful sale
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
        
        # Upsell phrases with monetary values
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
        
        # Positive sentiment phrases
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
        
        # Negative sentiment phrases
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
        
        # Neutral conversation starters and fillers
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
        """Generate a realistic call transcript with business indicators"""
        
        # Determine call outcome probabilities
        will_convert = random.random() < 0.3  # 30% conversion rate
        will_upsell = random.random() < 0.15 if will_convert else False  # 15% upsell rate
        
        # Determine sentiment (weighted towards positive for conversions)
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
        
        # Build transcript
        transcript_parts = []
        
        # Opening
        transcript_parts.append(f"Agent: Hello, thank you for calling. How can I help you today?")
        transcript_parts.append(f"Customer: {random.choice(self.neutral_phrases)} your services.")
        
        # Middle conversation (3-6 exchanges)
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
        
        # Closing - add conversion/upsell
        if will_convert:
            transcript_parts.append(f"Agent: Great! So would you like to proceed?")
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
        """Generate a complete call record"""
        call_id = f"CALL_{fake.uuid4()[:8].upper()}"
        agent_name = random.choice(self.agents)
        
        # Generate timestamp within last 30 days
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
    
    def generate_batch(self, num_calls=100, output_dir="data/raw_calls"):
        """Generate a batch of call records and save as JSON files"""
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        calls_generated = []
        
        for i in range(num_calls):
            call_record = self.generate_call_record()
            
            # Save individual call as JSON file
            filename = f"{call_record['call_id']}.json"
            file_path = output_path / filename
            
            with open(file_path, 'w') as f:
                json.dump(call_record, f, indent=2)
            
            calls_generated.append(call_record)
            
            if (i + 1) % 10 == 0:
                print(f"Generated {i + 1} calls...")
        
        print(f"Successfully generated {num_calls} call records in {output_dir}")
        
        # Also save a summary file
        summary_path = output_path / "batch_summary.json"
        with open(summary_path, 'w') as f:
            json.dump({
                "batch_info": {
                    "total_calls": num_calls,
                    "generated_at": datetime.now().isoformat(),
                    "agents_involved": list(set([call["agent_name"] for call in calls_generated]))
                },
                "sample_calls": calls_generated[:5]  # Include first 5 as samples
            }, f, indent=2)
        
        return calls_generated

def main():
    """Main function to generate synthetic data"""
    generator = CallDataGenerator()
    
    # Generate initial batch of calls
    print("Generating synthetic call data for Converza Pipeline...")
    calls = generator.generate_batch(num_calls=50)
    
    print(f"\nGenerated {len(calls)} call records")
    print("Sample call:")
    print(json.dumps(calls[0], indent=2))

if __name__ == "__main__":
    main()