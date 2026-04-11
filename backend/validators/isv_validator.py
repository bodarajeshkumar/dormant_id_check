"""
ISV Validator - Validates users against ISV (IBM Users API)

This module provides a pluggable wrapper around the existing IBM Users Resolver.
It checks if users exist in the ISV system and fetches their email addresses.

Workflow:
1. Takes user IDs from extraction file
2. Queries IBM Users API
3. Returns:
   - Users found in ISV (with email)
   - Users not found in ISV (alert list)
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ibm_users_resolver_async import IBMUsersResolverAsync
import asyncio


class ISVValidationError(Exception):
    """Custom exception for ISV validation errors"""
    pass


async def validate_isv(
    input_file: str,
    output_dir: str = "backend/resolutions",
    batch_size: int = 100,
    max_concurrent: int = 10
) -> Dict:
    """
    Validate users against ISV (IBM Users API).
    
    This is a wrapper around IBMUsersResolverAsync that provides a clean,
    pluggable interface for ISV validation.
    
    Args:
        input_file: Path to extraction JSON file with user IDs
        output_dir: Directory to save output files
        batch_size: Number of users to process per batch
        max_concurrent: Maximum concurrent API requests
        
    Returns:
        Dictionary with validation results:
        {
            "success": True,
            "validator": "isv_validation",
            "input_count": 1000,
            "output": {
                "found_in_isv": 950,
                "not_found_in_isv": 50
            },
            "files_created": {
                "resolved": "path/to/resolved_users.json",
                "failed": "path/to/failed_ids.json"
            },
            "timestamp": "2026-04-06T10:00:00",
            "duration_seconds": 45
        }
        
    Raises:
        ISVValidationError: If validation fails
    """
    try:
        start_time = datetime.now()
        
        # Validate input file exists
        if not Path(input_file).exists():
            raise ISVValidationError(f"Input file not found: {input_file}")
        
        # Load user IDs from extraction file
        with open(input_file, 'r') as f:
            extraction_data = json.load(f)
        
        # Extract unique user IDs
        user_ids = set()
        for record in extraction_data:
            # The extraction format is: {"id": "doc_id", "key": [...], "value": "USER_ID"}
            if 'value' in record and isinstance(record['value'], str):
                user_ids.add(record['value'])
            elif 'value' in record and isinstance(record['value'], dict) and 'uid' in record['value']:
                user_ids.add(record['value']['uid'])
            elif 'id' in record and not 'value' in record:
                # Fallback: if no value field, try id (for backwards compatibility)
                user_ids.add(record['id'])
        
        input_count = len(user_ids)
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp for output files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize resolver
        resolver = IBMUsersResolverAsync(
            batch_size=batch_size,
            max_concurrent=max_concurrent
        )
        
        # Run resolution
        results = await resolver.resolve_all(list(user_ids))
        
        # Create output file paths
        # Resolved users go to resolutions directory
        resolved_file = Path(output_dir) / f"isv_resolved_users_{timestamp}.json"
        
        # Failed IDs go to outputs directory (always created even if empty)
        outputs_dir = Path("backend/outputs")
        outputs_dir.mkdir(parents=True, exist_ok=True)
        failed_file = outputs_dir / f"isv_failed_ids_{timestamp}.json"
        
        # Save results
        resolver.save_results(results, str(resolved_file))
        resolver.save_failed_ids(list(user_ids), set(results.keys()), str(failed_file))
        
        # Get counts
        resolved_count = len(results)
        failed_count = input_count - resolved_count
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Return standardized result
        return {
            "success": True,
            "validator": "isv_validation",
            "input_count": input_count,
            "output": {
                "found_in_isv": resolved_count,
                "not_found_in_isv": failed_count
            },
            "files_created": {
                "resolved": str(resolved_file),
                "failed": str(failed_file)
            },
            "timestamp": end_time.isoformat(),
            "duration_seconds": int(duration)
        }
        
    except Exception as e:
        raise ISVValidationError(f"ISV validation failed: {str(e)}")


def validate_isv_sync(
    input_file: str,
    output_dir: str = "backend/resolutions",
    batch_size: int = 100,
    max_concurrent: int = 50
) -> Dict:
    """
    Synchronous wrapper for validate_isv.
    
    Use this when calling from non-async code.
    """
    return asyncio.run(validate_isv(input_file, output_dir, batch_size, max_concurrent))


# For backward compatibility and direct usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python isv_validator.py <extraction_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        result = validate_isv_sync(input_file)
        print(f"\n✓ ISV Validation Complete!")
        print(f"  Found in ISV: {result['output']['found_in_isv']}")
        print(f"  Not found in ISV: {result['output']['not_found_in_isv']}")
        print(f"  Duration: {result['duration_seconds']}s")
        print(f"\nFiles created:")
        print(f"  - Resolved: {result['files_created']['resolved']}")
        print(f"  - Failed: {result['files_created']['failed']}")
    except ISVValidationError as e:
        print(f"Error: {e}")
        sys.exit(1)

# Made with Bob
