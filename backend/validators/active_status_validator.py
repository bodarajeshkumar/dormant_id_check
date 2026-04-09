"""
Active Status Validator - Splits users by active/inactive status

This module provides a pluggable validator that checks user active status
and splits them into two groups.

Workflow:
1. Takes resolved users (from ISV validation)
2. Checks 'active' field
3. Returns:
   - Active users (active: true)
   - Inactive users (active: false) → ISV inactive list
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional


class ActiveStatusError(Exception):
    """Custom exception for active status validation errors"""
    pass


def validate_active_status(
    input_file: str,
    output_dir: str = "backend/resolutions",
    timestamp: Optional[str] = None
) -> Dict:
    """
    Validate and split users by active status.
    
    Args:
        input_file: Path to resolved users JSON file (from ISV validation)
        output_dir: Directory to save output files
        timestamp: Optional timestamp string for filenames (auto-generated if None)
        
    Returns:
        Dictionary with validation results:
        {
            "success": True,
            "validator": "active_status",
            "input_count": 1000,
            "output": {
                "active": 950,
                "inactive": 50
            },
            "files_created": {
                "active": "path/to/isv_active_users.json",
                "inactive": "path/to/isv_inactive_users.json"
            },
            "timestamp": "2026-04-06T10:00:00",
            "duration_seconds": 2
        }
        
    Raises:
        ActiveStatusError: If validation fails
    """
    try:
        start_time = datetime.now()
        
        # Validate input file exists
        if not Path(input_file).exists():
            raise ActiveStatusError(f"Input file not found: {input_file}")
        
        # Load resolved users
        with open(input_file, 'r') as f:
            users = json.load(f)
        
        input_count = len(users)
        
        # Split by active status
        active_users = [user for user in users if user.get('active') == True]
        inactive_users = [user for user in users if user.get('active') == False]
        
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Create outputs directory for inactive users
        outputs_dir = Path(output_dir).parent / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output file paths
        # Active users stay in resolutions
        active_file = Path(output_dir) / f"isv_active_users_{timestamp}.json"
        # Inactive users go to outputs (always created even if empty)
        inactive_file = outputs_dir / f"isv_inactive_users_{timestamp}.json"
        
        # Save files (always save both, even if empty)
        with open(active_file, 'w') as f:
            json.dump(active_users, f, indent=2)
        
        with open(inactive_file, 'w') as f:
            json.dump(inactive_users, f, indent=2)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Return standardized result
        return {
            "success": True,
            "validator": "active_status",
            "input_count": input_count,
            "output": {
                "active": len(active_users),
                "inactive": len(inactive_users)
            },
            "files_created": {
                "active": str(active_file),
                "inactive": str(inactive_file)
            },
            "timestamp": end_time.isoformat(),
            "duration_seconds": int(duration)
        }
        
    except Exception as e:
        raise ActiveStatusError(f"Active status validation failed: {str(e)}")


# For direct usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python active_status_validator.py <resolved_users_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        result = validate_active_status(input_file)
        print(f"\n✓ Active Status Validation Complete!")
        print(f"  Active users: {result['output']['active']}")
        print(f"  Inactive users: {result['output']['inactive']}")
        print(f"  Duration: {result['duration_seconds']}s")
        print(f"\nFiles created:")
        print(f"  - Active: {result['files_created']['active']}")
        print(f"  - Inactive: {result['files_created']['inactive']}")
    except ActiveStatusError as e:
        print(f"Error: {e}")
        sys.exit(1)

# Made with Bob
