"""
ETL Processors for Converza Pipeline
Handles data transformation and KPI extraction from call transcripts
"""

import re
import string
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CallTranscriptProcessor:
    def __init__(self):
        # Conversion indicators - phrases that suggest a successful sale
        self.conversion_patterns = [
            r"yes,?\s+i'?ll\s+book",
            r"sign\s+me\s+up",
            r"let'?s\s+proceed",
            r"i'?d\s+like\s+to\s+move\s+forward",
            r"sounds\s+perfect,?\s+let'?s\s+do\s+it",
            r"i'?m\s+ready\s+to\s+buy",
            r"count\s+me\s+in",
            r"i'?ll\s+take\s+it",
            r"let'?s\s+get\s+started",
            r"i'?m\s+interested"
        ]
        
        # Upsell patterns with monetary extraction
        self.upsell_patterns = [
            r"premium\s+package\s+for\s+an?\s+extra\s+\$(\d+)",
            r"extended\s+warranty\s+for\s+\$(\d+)",
            r"premium\s+support\s+for\s+\$(\d+)",
            r"deluxe\s+version\s+for\s+\$(\d+)",
            r"insurance\s+package\s+for\s+\$(\d+)",
            r"installation\s+service\s+for\s+\$(\d+)",
            r"premium\s+features\s+for\s+\$(\d+)",
            r"express\s+delivery\s+for\s+\$(\d+)",
            r"maintenance\s+plan\s+for\s+\$(\d+)",
            r"professional\s+tier\s+for\s+\$(\d+)",
            r"upgrade.*?for\s+\$(\d+)",
            r"add.*?for\s+\$(\d+)"
        ]
        
        # Positive sentiment indicators
        self.positive_keywords = [
            'fantastic', 'super', 'helpful', 'excellent', 'satisfied',
            'outstanding', 'impressed', 'great', 'wonderful', 'amazing',
            'perfect', 'love', 'awesome', 'brilliant', 'superb',
            'terrific', 'marvelous', 'fabulous', 'exceptional', 'incredible'
        ]
        
        # Negative sentiment indicators
        self.negative_keywords = [
            'frustrated', 'unacceptable', 'poor', 'disappointed', 'terrible',
            'unhappy', 'ridiculous', 'unsatisfied', 'waste', 'unprofessional',
            'awful', 'horrible', 'worst', 'hate', 'disgusted',
            'angry', 'furious', 'outraged', 'appalled', 'disgusting'
        ]
    
    def clean_transcript(self, transcript: str) -> str:
        """Clean and normalize transcript text"""
        if not transcript:
            return ""
        
        # Convert to lowercase
        cleaned = transcript.lower()
        
        # Remove extra whitespace
        cleaned = ' '.join(cleaned.split())
        
        # Remove special characters but keep basic punctuation
        cleaned = re.sub(r'[^\w\s\$\.\,\!\?\-\:\;]', ' ', cleaned)
        
        # Normalize common contractions
        contractions = {
            "i'll": "i will",
            "we'll": "we will",
            "you'll": "you will",
            "can't": "cannot",
            "won't": "will not",
            "don't": "do not",
            "doesn't": "does not",
            "isn't": "is not",
            "aren't": "are not",
            "wasn't": "was not",
            "weren't": "were not",
            "let's": "let us"
        }
        
        for contraction, expansion in contractions.items():
            cleaned = cleaned.replace(contraction, expansion)
        
        return cleaned
    
    def detect_conversion(self, transcript: str) -> bool:
        """Detect if the call resulted in a conversion"""
        transcript_lower = transcript.lower()
        
        for pattern in self.conversion_patterns:
            if re.search(pattern, transcript_lower, re.IGNORECASE):
                logger.info(f"Conversion detected with pattern: {pattern}")
                return True
        
        return False
    
    def extract_upsell_amount(self, transcript: str) -> float:
        """Extract upsell amount from transcript"""
        transcript_lower = transcript.lower()
        total_upsell = 0.0
        
        for pattern in self.upsell_patterns:
            matches = re.findall(pattern, transcript_lower, re.IGNORECASE)
            for match in matches:
                try:
                    amount = float(match)
                    total_upsell += amount
                    logger.info(f"Upsell detected: ${amount}")
                except ValueError:
                    continue
        
        return total_upsell
    
    def analyze_sentiment(self, transcript: str) -> str:
        """Analyze sentiment of the call transcript"""
        transcript_lower = transcript.lower()
        
        # Count positive and negative indicators
        positive_count = sum(1 for keyword in self.positive_keywords 
                           if keyword in transcript_lower)
        negative_count = sum(1 for keyword in self.negative_keywords 
                           if keyword in transcript_lower)
        
        # Simple rule-based sentiment classification
        if positive_count > negative_count and positive_count > 0:
            return "Positive"
        elif negative_count > positive_count and negative_count > 0:
            return "Negative"
        else:
            return "Neutral"
    
    def extract_all_kpis(self, call_data: Dict) -> Dict:
        """Extract all KPIs from call data"""
        transcript = call_data.get('transcript_text', '')
        cleaned_transcript = self.clean_transcript(transcript)
        
        # Extract KPIs
        is_conversion = self.detect_conversion(transcript)
        upsell_amount = self.extract_upsell_amount(transcript)
        sentiment_score = self.analyze_sentiment(transcript)
        
        # Prepare results
        kpis = {
            'call_id': call_data.get('call_id'),
            'agent_name': call_data.get('agent_name'),
            'timestamp': call_data.get('timestamp'),
            'cleaned_transcript': cleaned_transcript,
            'is_conversion': is_conversion,
            'upsell_amount': upsell_amount,
            'sentiment_score': sentiment_score
        }
        
        logger.info(f"Processed call {kpis['call_id']}: "
                   f"Conversion={is_conversion}, Upsell=${upsell_amount}, "
                   f"Sentiment={sentiment_score}")
        
        return kpis

class BatchProcessor:
    def __init__(self):
        self.processor = CallTranscriptProcessor()
    
    def process_bronze_to_silver(self, bronze_calls: List[Dict]) -> List[Dict]:
        """Process bronze layer calls to silver layer (cleaning)"""
        silver_calls = []
        
        for call in bronze_calls:
            cleaned_call = {
                'call_id': call['call_id'],
                'agent_name': call['agent_name'],
                'timestamp': call['timestamp'],
                'cleaned_transcript': self.processor.clean_transcript(call['transcript_text'])
            }
            silver_calls.append(cleaned_call)
        
        logger.info(f"Processed {len(silver_calls)} calls from bronze to silver")
        return silver_calls
    
    def process_silver_to_gold(self, silver_calls: List[Dict]) -> List[Dict]:
        """Process silver layer calls to gold layer (KPI extraction)"""
        gold_metrics = []
        
        for call in silver_calls:
            # Reconstruct call data for KPI extraction
            call_data = {
                'call_id': call['call_id'],
                'agent_name': call['agent_name'],
                'timestamp': call['timestamp'],
                'transcript_text': call['cleaned_transcript']  # Use cleaned transcript
            }
            
            kpis = self.processor.extract_all_kpis(call_data)
            
            # Prepare gold layer record
            gold_record = {
                'call_id': kpis['call_id'],
                'agent_name': kpis['agent_name'],
                'timestamp': kpis['timestamp'],
                'is_conversion': kpis['is_conversion'],
                'upsell_amount': kpis['upsell_amount'],
                'sentiment_score': kpis['sentiment_score']
            }
            
            gold_metrics.append(gold_record)
        
        logger.info(f"Processed {len(gold_metrics)} calls from silver to gold")
        return gold_metrics
    
    def generate_performance_report(self, gold_metrics: List[Dict]) -> Dict:
        """Generate performance summary report"""
        if not gold_metrics:
            return {}
        
        total_calls = len(gold_metrics)
        conversions = sum(1 for call in gold_metrics if call['is_conversion'])
        total_upsell = sum(call['upsell_amount'] for call in gold_metrics)
        
        # Agent performance
        agent_stats = {}
        for call in gold_metrics:
            agent = call['agent_name']
            if agent not in agent_stats:
                agent_stats[agent] = {
                    'total_calls': 0,
                    'conversions': 0,
                    'upsell_revenue': 0,
                    'positive_calls': 0,
                    'negative_calls': 0
                }
            
            agent_stats[agent]['total_calls'] += 1
            if call['is_conversion']:
                agent_stats[agent]['conversions'] += 1
            agent_stats[agent]['upsell_revenue'] += call['upsell_amount']
            
            if call['sentiment_score'] == 'Positive':
                agent_stats[agent]['positive_calls'] += 1
            elif call['sentiment_score'] == 'Negative':
                agent_stats[agent]['negative_calls'] += 1
        
        # Calculate conversion rates
        for agent in agent_stats:
            stats = agent_stats[agent]
            stats['conversion_rate'] = (stats['conversions'] / stats['total_calls'] * 100) if stats['total_calls'] > 0 else 0
        
        report = {
            'summary': {
                'total_calls': total_calls,
                'total_conversions': conversions,
                'overall_conversion_rate': (conversions / total_calls * 100) if total_calls > 0 else 0,
                'total_upsell_revenue': total_upsell,
                'average_upsell_per_call': total_upsell / total_calls if total_calls > 0 else 0
            },
            'agent_performance': agent_stats,
            'generated_at': datetime.now().isoformat()
        }
        
        return report

def test_processor():
    """Test the transcript processor with sample data"""
    processor = CallTranscriptProcessor()
    
    # Test data
    test_transcript = """
    Agent: Hello, thank you for calling. How can I help you today?
    Customer: I'm looking into your services.
    Agent: I'd be happy to help you with that. Let me explain our options.
    Customer: That sounds interesting. Tell me more about the features.
    Agent: Great! So would you like to proceed?
    Customer: Yes, I'll book that appointment. This is fantastic.
    Agent: Excellent! I also have some additional options that might interest you.
    Customer: I'll take the premium package for an extra $50.
    Agent: Perfect! I'll add that to your order.
    """
    
    test_call = {
        'call_id': 'TEST_001',
        'agent_name': 'Test Agent',
        'timestamp': datetime.now().isoformat(),
        'transcript_text': test_transcript
    }
    
    # Process the call
    kpis = processor.extract_all_kpis(test_call)
    
    print("Test Results:")
    print(f"Conversion: {kpis['is_conversion']}")
    print(f"Upsell Amount: ${kpis['upsell_amount']}")
    print(f"Sentiment: {kpis['sentiment_score']}")

if __name__ == "__main__":
    test_processor()