"""
Filtering Plugin System for Cloudant Data Extraction
Provides modular filters that can be enabled/disabled dynamically
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class FilterPlugin(ABC):
    """Base class for all filter plugins"""
    
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.records_filtered = 0
        self.records_passed = 0
    
    @abstractmethod
    def get_name(self) -> str:
        """Return the filter name"""
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """Return the filter description"""
        pass
    
    @abstractmethod
    def should_include(self, record: Dict) -> bool:
        """
        Determine if a record should be included.
        
        Args:
            record: The record to filter
            
        Returns:
            True if record should be included, False otherwise
        """
        pass
    
    def filter(self, record: Dict) -> bool:
        """
        Filter a record (wrapper that tracks statistics).
        
        Args:
            record: The record to filter
            
        Returns:
            True if record should be included, False otherwise
        """
        if not self.enabled:
            return True
        
        result = self.should_include(record)
        
        if result:
            self.records_passed += 1
        else:
            self.records_filtered += 1
        
        return result
    
    def get_stats(self) -> Dict:
        """Get filter statistics"""
        return {
            'name': self.get_name(),
            'enabled': self.enabled,
            'records_passed': self.records_passed,
            'records_filtered': self.records_filtered,
            'total_processed': self.records_passed + self.records_filtered
        }


class ISVValidationFilter(FilterPlugin):
    """Filter 1: ISV Validation API - Validates records against ISV API"""
    
    def __init__(self, enabled: bool = True, api_url: Optional[str] = None):
        super().__init__(enabled)
        self.api_url = api_url or "https://api.example.com/isv/validate"
        self.cache = {}  # Simple cache to avoid repeated API calls
    
    def get_name(self) -> str:
        return "ISV Validation"
    
    def get_description(self) -> str:
        return "Validates records against ISV (Independent Software Vendor) API"
    
    def should_include(self, record: Dict) -> bool:
        """
        Validate record against ISV API.
        
        For now, this is a placeholder implementation.
        In production, you would call the actual ISV API.
        """
        try:
            # Extract user ID or relevant field
            doc_id = record.get('id', '')
            value = record.get('value', {})
            
            # Check cache first
            if doc_id in self.cache:
                return self.cache[doc_id]
            
            # Placeholder logic - replace with actual API call
            # Example: Check if user has valid ISV status
            # response = requests.get(f"{self.api_url}/{doc_id}", timeout=5)
            # is_valid = response.json().get('is_valid', False)
            
            # For now, include all records (placeholder)
            is_valid = True
            
            # Cache result
            self.cache[doc_id] = is_valid
            
            return is_valid
            
        except Exception as e:
            logger.error(f"ISV validation error for record {record.get('id')}: {e}")
            # On error, include the record (fail-open)
            return True


class DormancyCheckFilter(FilterPlugin):
    """Filter 2: Dormancy Check - Filters out accounts inactive for >3 years"""
    
    def __init__(self, enabled: bool = True, inactivity_years: int = 3):
        super().__init__(enabled)
        self.inactivity_years = inactivity_years
        self.cutoff_date = datetime.now() - timedelta(days=365 * inactivity_years)
    
    def get_name(self) -> str:
        return "Dormancy Check"
    
    def get_description(self) -> str:
        return f"Filters out accounts inactive for more than {self.inactivity_years} years"
    
    def should_include(self, record: Dict) -> bool:
        """
        Check if account has been active within the threshold period.
        """
        try:
            # Extract timestamp from key
            key = record.get('key', [])
            
            if len(key) >= 7:
                # key format: [boolean, year, month, day, hour, minute, second]
                year, month, day = key[1], key[2], key[3]
                last_activity = datetime(year, month, day)
                
                # Include if activity is within threshold
                is_active = last_activity >= self.cutoff_date
                
                if not is_active:
                    logger.debug(
                        f"Filtered dormant record: last activity {last_activity.date()}, "
                        f"cutoff {self.cutoff_date.date()}"
                    )
                
                return is_active
            
            # If we can't determine activity date, include it
            return True
            
        except Exception as e:
            logger.error(f"Dormancy check error for record {record.get('id')}: {e}")
            # On error, include the record
            return True


class FederatedIDFilter(FilterPlugin):
    """Filter 3: Federated ID Removal - Removes non-@ibm.com email addresses"""
    
    def __init__(self, enabled: bool = True, allowed_domains: Optional[List[str]] = None):
        super().__init__(enabled)
        self.allowed_domains = allowed_domains or ['@ibm.com']
    
    def get_name(self) -> str:
        return "Federated ID Removal"
    
    def get_description(self) -> str:
        return f"Removes records with email addresses not in: {', '.join(self.allowed_domains)}"
    
    def should_include(self, record: Dict) -> bool:
        """
        Check if record has an allowed email domain.
        """
        try:
            # Extract email from record value
            value = record.get('value', {})
            email = value.get('email', '') or value.get('mail', '') or value.get('uid', '')
            
            if not email:
                # If no email found, check doc_id
                email = record.get('id', '')
            
            # Check if email contains any allowed domain
            for domain in self.allowed_domains:
                if domain.lower() in email.lower():
                    return True
            
            logger.debug(f"Filtered non-federated ID: {email}")
            return False
            
        except Exception as e:
            logger.error(f"Federated ID check error for record {record.get('id')}: {e}")
            # On error, include the record
            return True


class CloudActivityFilter(FilterPlugin):
    """Filter 4: Cloud Activity Validation - Validates cloud activity status"""
    
    def __init__(self, enabled: bool = True, api_url: Optional[str] = None):
        super().__init__(enabled)
        self.api_url = api_url or "https://api.example.com/cloud/activity"
        self.cache = {}
    
    def get_name(self) -> str:
        return "Cloud Activity Validation"
    
    def get_description(self) -> str:
        return "Validates records have active cloud activity"
    
    def should_include(self, record: Dict) -> bool:
        """
        Validate record has cloud activity.
        
        For now, this is a placeholder implementation.
        In production, you would call the actual cloud activity API.
        """
        try:
            doc_id = record.get('id', '')
            
            # Check cache first
            if doc_id in self.cache:
                return self.cache[doc_id]
            
            # Placeholder logic - replace with actual API call
            # Example: Check if user has cloud activity
            # response = requests.get(f"{self.api_url}/{doc_id}", timeout=5)
            # has_activity = response.json().get('has_activity', False)
            
            # For now, include all records (placeholder)
            has_activity = True
            
            # Cache result
            self.cache[doc_id] = has_activity
            
            return has_activity
            
        except Exception as e:
            logger.error(f"Cloud activity validation error for record {record.get('id')}: {e}")
            # On error, include the record
            return True


class FilterManager:
    """Manages multiple filter plugins"""
    
    def __init__(self, filter_config: Optional[Dict[str, bool]] = None):
        """
        Initialize filter manager with configuration.
        
        Args:
            filter_config: Dictionary mapping filter names to enabled status
                          e.g., {'isv_validation': True, 'dormancy_check': False}
        """
        self.filter_config = filter_config or {}
        self.filters: List[FilterPlugin] = []
        self._initialize_filters()
    
    def _initialize_filters(self):
        """Initialize all available filters based on configuration"""
        # Create filter instances
        self.filters = [
            ISVValidationFilter(
                enabled=self.filter_config.get('isv_validation', False)
            ),
            DormancyCheckFilter(
                enabled=self.filter_config.get('dormancy_check', False)
            ),
            FederatedIDFilter(
                enabled=self.filter_config.get('federated_id_removal', False)
            ),
            CloudActivityFilter(
                enabled=self.filter_config.get('cloud_activity', False)
            )
        ]
        
        logger.info("Initialized filters:")
        for f in self.filters:
            logger.info(f"  - {f.get_name()}: {'ENABLED' if f.enabled else 'DISABLED'}")
    
    def filter_record(self, record: Dict) -> bool:
        """
        Apply all enabled filters to a record.
        
        Args:
            record: The record to filter
            
        Returns:
            True if record passes all filters, False otherwise
        """
        for filter_plugin in self.filters:
            if not filter_plugin.filter(record):
                return False
        return True
    
    def filter_batch(self, records: List[Dict]) -> List[Dict]:
        """
        Apply all enabled filters to a batch of records.
        
        Args:
            records: List of records to filter
            
        Returns:
            List of records that passed all filters
        """
        return [record for record in records if self.filter_record(record)]
    
    def get_stats(self) -> Dict:
        """Get statistics for all filters"""
        return {
            'filters': [f.get_stats() for f in self.filters],
            'total_filters': len(self.filters),
            'enabled_filters': sum(1 for f in self.filters if f.enabled)
        }
    
    def get_available_filters(self) -> List[Dict]:
        """Get list of available filters with metadata"""
        return [
            {
                'id': f.get_name().lower().replace(' ', '_'),
                'name': f.get_name(),
                'description': f.get_description(),
                'enabled': f.enabled
            }
            for f in self.filters
        ]

# Made with Bob
