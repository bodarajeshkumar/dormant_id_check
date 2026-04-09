"""
BluPages Validator - Validates users against IBM BluPages

This module provides a pluggable wrapper around the existing BluPages validator.
It checks if users exist in IBM BluPages directory.

Workflow:
1. Takes users with old login (>1065 days)
2. Queries IBM BluPages API
3. Returns:
   - Users found in BluPages → Not to be deleted
   - Users not found in BluPages → To be deleted
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bluepages_validator_async import validate_users_async
import asyncio


class BluePagesError(Exception):
    """Custom exception for BluPages validation errors"""
    pass


async def validate_bluepages(
    input_file: str,
    output_dir: str = "backend/resolutions",
    timestamp: Optional[str] = None,
    max_concurrent: int = 50,
    batch_size: int = 100
) -> Dict:
    """
    Validate users against IBM BluPages.
    
    This is a wrapper around the existing bluepages_validator_async that provides
    a clean, pluggable interface for BluPages validation.
    
    Args:
        input_file: Path to users JSON file (old login users)
        output_dir: Directory to save output files
        timestamp: Optional timestamp string for filenames (auto-generated if None)
        max_concurrent: Maximum concurrent API requests
        batch_size: Number of users to process per batch
        
    Returns:
        Dictionary with validation results:
        {
            "success": True,
            "validator": "bluepages",
            "input_count": 234,
            "output": {
                "found_in_bluepages": 200,
                "not_found_in_bluepages": 34
            },
            "files_created": {
                "to_delete": "path/to/to_be_deleted.json",
                "not_to_delete": "path/to/not_to_be_deleted.json"
            },
            "timestamp": "2026-04-06T10:00:00",
            "duration_seconds": 120
        }
        
    Raises:
        BluePagesError: If validation fails
    """
    try:
        start_time = datetime.now()
        
        # Validate input file exists
        if not Path(input_file).exists():
            raise BluePagesError(f"Input file not found: {input_file}")
        
        # Load users
        with open(input_file, 'r') as f:
            users = json.load(f)
        
        input_count = len(users)
        
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Create outputs directory for final results
        outputs_dir = Path(output_dir).parent / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output file paths - final outputs go to outputs folder
        to_delete_file = outputs_dir / f"to_be_deleted_{timestamp}.json"
        not_to_delete_file = outputs_dir / "not_to_be_deleted.json"
        
        # Use a temporary file for BluPages results
        temp_bluepages_file = Path(output_dir) / f"temp_bluepages_{timestamp}.json"
        
        # Run BluPages validation using existing async validator
        await validate_users_async(
            input_file=input_file,
            to_delete_file=str(to_delete_file),
            not_to_delete_file=str(temp_bluepages_file),  # Use temp file
            test_mode=False,
            resume=False,
            max_concurrent=max_concurrent,
            batch_size=batch_size
        )
        
        # Append BluPages results to not_to_be_deleted.json
        bluepages_users = []
        if temp_bluepages_file.exists():
            with open(temp_bluepages_file, 'r') as f:
                bluepages_users = json.load(f)
        
        # Merge with existing not_to_be_deleted.json
        if not_to_delete_file.exists():
            try:
                with open(not_to_delete_file, 'r') as f:
                    existing_users = json.load(f)
                
                # Avoid duplicates by user_id
                existing_user_ids = {user.get('user_id') for user in existing_users if 'user_id' in user}
                new_users = [user for user in bluepages_users if user.get('user_id') not in existing_user_ids]
                
                combined_users = existing_users + new_users
                
                with open(not_to_delete_file, 'w') as f:
                    json.dump(combined_users, f, indent=2)
            except (json.JSONDecodeError, KeyError):
                # If existing file is invalid, overwrite it
                with open(not_to_delete_file, 'w') as f:
                    json.dump(bluepages_users, f, indent=2)
        else:
            with open(not_to_delete_file, 'w') as f:
                json.dump(bluepages_users, f, indent=2)
        
        # Clean up temp file
        if temp_bluepages_file.exists():
            temp_bluepages_file.unlink()
        
        # Load results to get counts
        to_delete_count = 0
        not_to_delete_count = 0
        
        if to_delete_file.exists():
            with open(to_delete_file, 'r') as f:
                to_delete_data = json.load(f)
                to_delete_count = len(to_delete_data)
        
        if not_to_delete_file.exists():
            with open(not_to_delete_file, 'r') as f:
                not_to_delete_data = json.load(f)
                not_to_delete_count = len(not_to_delete_data)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Return standardized result
        return {
            "success": True,
            "validator": "bluepages",
            "input_count": input_count,
            "output": {
                "found_in_bluepages": not_to_delete_count,
                "not_found_in_bluepages": to_delete_count
            },
            "files_created": {
                "to_delete": str(to_delete_file),
                "not_to_delete": str(not_to_delete_file)
            },
            "timestamp": end_time.isoformat(),
            "duration_seconds": int(duration)
        }
        
    except Exception as e:
        raise BluePagesError(f"BluPages validation failed: {str(e)}")


def validate_bluepages_sync(
    input_file: str,
    output_dir: str = "backend/resolutions",
    timestamp: Optional[str] = None,
    max_concurrent: int = 50,
    batch_size: int = 100
) -> Dict:
    """
    Synchronous wrapper for validate_bluepages.
    
    Use this when calling from non-async code.
    """
    return asyncio.run(validate_bluepages(
        input_file, output_dir, timestamp, max_concurrent, batch_size
    ))


# For direct usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python bluepages_validator.py <old_login_users_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        result = validate_bluepages_sync(input_file)
        print(f"\n✓ BluPages Validation Complete!")
        print(f"  Found in BluPages: {result['output']['found_in_bluepages']}")
        print(f"  Not found in BluPages: {result['output']['not_found_in_bluepages']}")
        print(f"  Duration: {result['duration_seconds']}s")
        print(f"\nFiles created:")
        print(f"  - To delete: {result['files_created']['to_delete']}")
        print(f"  - Not to delete: {result['files_created']['not_to_delete']}")
    except BluePagesError as e:
        print(f"Error: {e}")
        sys.exit(1)

# Made with Bob
