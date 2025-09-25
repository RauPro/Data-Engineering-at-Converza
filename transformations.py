"""
Transformation module for Gold Layer metrics.

Contains business logic for extracting insights from call transcripts,
including conversion detection, upsell extraction, and sentiment analysis.
"""

import re
import logging
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)


@dataclass
class ConversionResult:
    """Result of conversion detection analysis."""
    is_conversion: bool
    conversion_phrase: Optional[str] = None
    confidence: float = 0.0


@dataclass
class UpsellResult:
    """Result of upsell extraction analysis."""
    has_upsell: bool
    amount: Decimal
    product: Optional[str] = None


@dataclass
class SentimentResult:
    """Result of sentiment analysis."""
    sentiment: str  # 'positive', 'negative', or 'neutral'
    confidence: float
    positive_count: int
    negative_count: int


class TransformationEngine:
    """Engine for applying business transformations to call data."""
    
    def __init__(self):
        """Initialize transformation engine with keyword patterns."""
        self._setup_conversion_patterns()
        self._setup_upsell_patterns()
        self._setup_sentiment_keywords()
    
    def _setup_conversion_patterns(self) -> None:
        """Define conversion detection patterns."""
        self.conversion_phrases = [
            r"yes.*(?:book|appointment|schedule)",
            r"sign\s+me\s+up",
            r"(?:let's|lets)\s+(?:proceed|move\s+forward)",
            r"i(?:'ll|'d|'m)\s+(?:take|buy|purchase)",
            r"(?:sounds?|that's?)\s+(?:perfect|great|good).*(?:do\s+it|proceed)",
            r"i'm\s+(?:ready|interested|in)",
            r"count\s+me\s+in",
            r"let's\s+get\s+started",
            r"i\s+want\s+(?:to|it)",
            r"go\s+ahead"
        ]
        
        # Compile patterns for efficiency
        self.conversion_pattern = re.compile(
            '|'.join(self.conversion_phrases),
            re.IGNORECASE
        )
    
    def _setup_upsell_patterns(self) -> None:
        """Define upsell extraction patterns."""
        # Pattern to extract monetary amounts
        self.amount_pattern = re.compile(
            r'\$?\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            re.IGNORECASE
        )
        
        # Upsell keywords indicating additional purchase
        self.upsell_keywords = [
            r"(?:premium|deluxe|pro)\s+(?:package|version|tier|plan)",
            r"(?:extended|additional)\s+(?:warranty|coverage|protection)",
            r"(?:upgrade|add|include)\s+(?:to|the)",
            r"(?:extra|additional)\s+(?:features?|services?)",
            r"(?:express|priority)\s+(?:delivery|shipping|service)",
            r"(?:maintenance|support)\s+(?:plan|package)",
            r"insurance\s+(?:package|coverage)",
            r"installation\s+service"
        ]
        
        self.upsell_pattern = re.compile(
            '|'.join(self.upsell_keywords),
            re.IGNORECASE
        )
    
    def _setup_sentiment_keywords(self) -> None:
        """Define sentiment analysis keywords."""
        self.positive_keywords = {
            'excellent', 'fantastic', 'great', 'wonderful', 'amazing',
            'perfect', 'outstanding', 'impressive', 'satisfied', 'happy',
            'helpful', 'professional', 'recommend', 'pleased', 'delighted',
            'love', 'best', 'awesome', 'brilliant', 'superb'
        }
        
        self.negative_keywords = {
            'terrible', 'horrible', 'awful', 'disappointed', 'frustrated',
            'unacceptable', 'poor', 'bad', 'worst', 'angry',
            'upset', 'unsatisfied', 'ridiculous', 'waste', 'useless',
            'unprofessional', 'rude', 'incompetent', 'disgusted', 'pathetic'
        }
        
        # Negation words that can flip sentiment
        self.negation_words = {
            'not', 'no', 'never', 'neither', 'nor',
            "don't", "doesn't", "didn't", "won't", "wouldn't",
            "shouldn't", "couldn't", "can't", "cannot"
        }
    
    def detect_conversion(self, transcript: str) -> ConversionResult:
        """
        Detect if a call resulted in a conversion.
        
        Args:
            transcript: Call transcript text
        
        Returns:
            ConversionResult with detection details
        """
        if not transcript:
            return ConversionResult(is_conversion=False)
        
        # Search for conversion patterns
        match = self.conversion_pattern.search(transcript.lower())
        
        if match:
            # Calculate confidence based on match position
            # Higher confidence if conversion happens later in conversation
            position_ratio = match.start() / len(transcript)
            confidence = 0.7 + (position_ratio * 0.3)
            
            return ConversionResult(
                is_conversion=True,
                conversion_phrase=match.group()[:255],  # Limit phrase length
                confidence=min(confidence, 1.0)
            )
        
        return ConversionResult(is_conversion=False)
    
    def extract_upsell(self, transcript: str) -> UpsellResult:
        """
        Extract upsell information from transcript.
        
        Args:
            transcript: Call transcript text
        
        Returns:
            UpsellResult with extracted amount and product
        """
        if not transcript:
            return UpsellResult(has_upsell=False, amount=Decimal('0.00'))
        
        transcript_lower = transcript.lower()
        
        # Check for upsell keywords
        upsell_match = self.upsell_pattern.search(transcript_lower)
        
        if not upsell_match:
            return UpsellResult(has_upsell=False, amount=Decimal('0.00'))
        
        # Extract the context around the upsell mention
        start = max(0, upsell_match.start() - 100)
        end = min(len(transcript), upsell_match.end() + 100)
        context = transcript[start:end]
        
        # Look for monetary amounts in the context
        amount_matches = self.amount_pattern.findall(context)
        
        if amount_matches:
            # Take the first amount found near the upsell mention
            amount_str = amount_matches[0].replace(',', '')
            try:
                amount = Decimal(amount_str)
            except:
                amount = Decimal('0.00')
            
            # Extract product name from the match
            product = self._extract_product_name(upsell_match.group())
            
            return UpsellResult(
                has_upsell=True,
                amount=amount,
                product=product[:255] if product else None
            )
        
        return UpsellResult(has_upsell=False, amount=Decimal('0.00'))
    
    def _extract_product_name(self, upsell_text: str) -> Optional[str]:
        """Extract product name from upsell text."""
        # Clean and format the product name
        product = upsell_text.strip()
        
        # Capitalize first letter of each word
        product = ' '.join(word.capitalize() for word in product.split())
        
        return product if product else None
    
    def analyze_sentiment(self, transcript: str) -> SentimentResult:
        """
        Analyze sentiment of the call transcript.
        
        Args:
            transcript: Call transcript text
        
        Returns:
            SentimentResult with sentiment score and details
        """
        if not transcript:
            return SentimentResult(
                sentiment='neutral',
                confidence=0.5,
                positive_count=0,
                negative_count=0
            )
        
        words = re.findall(r'\b\w+\b', transcript.lower())
        
        positive_count = 0
        negative_count = 0
        
        # Check for sentiment keywords with negation handling
        for i, word in enumerate(words):
            # Check for negation in previous 2 words
            is_negated = self._check_negation(words, i)
            
            if word in self.positive_keywords:
                if is_negated:
                    negative_count += 1
                else:
                    positive_count += 1
            elif word in self.negative_keywords:
                if is_negated:
                    positive_count += 1
                else:
                    negative_count += 1
        
        # Calculate sentiment score
        total_sentiment_words = positive_count + negative_count
        
        if total_sentiment_words == 0:
            return SentimentResult(
                sentiment='neutral',
                confidence=0.5,
                positive_count=0,
                negative_count=0
            )
        
        # Determine sentiment based on counts
        sentiment_score = (positive_count - negative_count) / total_sentiment_words
        
        if sentiment_score > 0.2:
            sentiment = 'positive'
        elif sentiment_score < -0.2:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        # Calculate confidence based on how many sentiment words were found
        confidence = min(total_sentiment_words / 20, 1.0)  # Cap at 1.0
        
        return SentimentResult(
            sentiment=sentiment,
            confidence=confidence,
            positive_count=positive_count,
            negative_count=negative_count
        )
    
    def _check_negation(self, words: List[str], index: int) -> bool:
        """Check if a word at index is negated by previous words."""
        # Check previous 2 words for negation
        for i in range(max(0, index - 2), index):
            if words[i] in self.negation_words:
                return True
        return False
    
    def transform_call_record(self, call_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply all transformations to a call record.
        
        Args:
            call_data: Dictionary containing call information
        
        Returns:
            Enriched call data with Gold Layer metrics
        """
        transcript = call_data.get('transcript_text', '')
        
        # Detect conversion
        conversion_result = self.detect_conversion(transcript)
        
        # Extract upsell
        upsell_result = self.extract_upsell(transcript)
        
        # Analyze sentiment
        sentiment_result = self.analyze_sentiment(transcript)
        
        # Calculate additional metrics
        transcript_length = len(transcript)
        estimated_duration = self._estimate_call_duration(transcript_length)
        
        # Build enriched record
        enriched_data = {
            'call_id': call_data.get('call_id'),
            'agent_name': call_data.get('agent_name'),
            'call_timestamp': call_data.get('timestamp'),
            'is_conversion': conversion_result.is_conversion,
            'conversion_phrase': conversion_result.conversion_phrase,
            'upsell_amount': upsell_result.amount,
            'upsell_product': upsell_result.product,
            'sentiment_score': sentiment_result.sentiment,
            'sentiment_confidence': sentiment_result.confidence,
            'positive_keywords_count': sentiment_result.positive_count,
            'negative_keywords_count': sentiment_result.negative_count,
            'transcript_length': transcript_length,
            'call_duration_seconds': estimated_duration
        }
        
        return enriched_data
    
    def _estimate_call_duration(self, transcript_length: int) -> int:
        """
        Estimate call duration based on transcript length.
        
        Assumes average speaking rate of 150 words per minute.
        """
        # Rough estimate: 5 characters per word
        estimated_words = transcript_length / 5
        
        # Convert to seconds (150 words per minute)
        estimated_seconds = int((estimated_words / 150) * 60)
        
        # Add some buffer for pauses and thinking time
        return int(estimated_seconds * 1.2)
