from typing import Any, Dict, Optional
from datetime import datetime, date
import re
import logging

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

class DataValidator:
    """Validator class for insurance data"""
    
    @staticmethod
    def validate_email(email: Optional[str]) -> Optional[str]:
        """Validate and clean email address"""
        if not email:
            return None
            
        email = email.strip().lower()
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, email):
            raise ValidationError(f"Invalid email format: {email}")
        return email

    @staticmethod
    def validate_phone(phone: Optional[str]) -> Optional[str]:
        """Validate and clean phone number"""
        if not phone:
            return None
            
        # Remove all non-numeric characters
        cleaned = re.sub(r'\D', '', phone)
        
        # Check if it's a valid length (10 digits for US)
        if len(cleaned) == 10:
            return f"{cleaned[:3]}-{cleaned[3:6]}-{cleaned[6:]}"
        elif len(cleaned) == 11 and cleaned[0] == '1':
            return f"{cleaned[1:4]}-{cleaned[4:7]}-{cleaned[7:]}"
        else:
            raise ValidationError(f"Invalid phone number format: {phone}")

    @staticmethod
    def validate_zip_code(zip_code: Optional[str]) -> Optional[str]:
        """Validate and clean ZIP code"""
        if not zip_code:
            return None
            
        # Remove all whitespace
        cleaned = zip_code.strip()
        
        # Check for 5-digit or 5+4 format
        if re.match(r'^\d{5}$', cleaned) or re.match(r'^\d{5}-\d{4}$', cleaned):
            return cleaned
        else:
            raise ValidationError(f"Invalid ZIP code format: {zip_code}")

    @staticmethod
    def validate_state(state: Optional[str]) -> Optional[str]:
        """Validate US state code"""
        if not state:
            return None
            
        state = state.strip().upper()
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC', 'PR', 'VI', 'GU', 'MP', 'AS'
        }
        if state not in valid_states:
            raise ValidationError(f"Invalid state code: {state}")
        return state

    @staticmethod
    def validate_date(date_str: Optional[str]) -> Optional[datetime]:
        """Validate and parse date string"""
        if not date_str:
            return None
            
        try:
            return datetime.fromisoformat(date_str)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid date format: {date_str}")

    @staticmethod
    def validate_currency_amount(amount: Any) -> Optional[float]:
        """Validate and clean currency amount"""
        if amount is None:
            return None
            
        try:
            cleaned = float(str(amount).replace(',', ''))
            if cleaned < 0:
                raise ValidationError(f"Currency amount cannot be negative: {amount}")
            return round(cleaned, 2)
        except (ValueError, TypeError):
            raise ValidationError(f"Invalid currency amount: {amount}")

    @staticmethod
    def validate_policy_number(policy_number: Optional[str]) -> Optional[str]:
        """Validate policy number format"""
        if not policy_number:
            return None
            
        # Remove whitespace and convert to uppercase
        cleaned = policy_number.strip().upper()
        
        # Check for common policy number format (alphanumeric, may include hyphens)
        if not re.match(r'^[A-Z0-9-]{5,20}$', cleaned):
            raise ValidationError(f"Invalid policy number format: {policy_number}")
        return cleaned

    @staticmethod
    def validate_status(status: Optional[str], valid_statuses: set) -> Optional[str]:
        """Validate status against a set of valid values"""
        if not status:
            return None
            
        status = status.strip().upper()
        if status not in valid_statuses:
            raise ValidationError(f"Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}")
        return status

    @staticmethod
    def clean_text(text: Optional[str]) -> Optional[str]:
        """Clean and normalize text fields"""
        if not text:
            return None
            
        # Remove extra whitespace and normalize
        return ' '.join(text.strip().split())

    @staticmethod
    def validate_uuid(uuid_str: Optional[str]) -> Optional[str]:
        """Validate UUID format"""
        if not uuid_str:
            return None
            
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if not re.match(uuid_pattern, str(uuid_str).lower()):
            raise ValidationError(f"Invalid UUID format: {uuid_str}")
        return uuid_str.lower() 