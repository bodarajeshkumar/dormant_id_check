"""
Login Validator - Filters users by last login date

This module provides a pluggable validator that checks last login dates
and splits users based on a threshold (default: 1065 days = ~3 years).

Workflow:
1. Takes active users (from active status validation)
2. Checks 'lastLogin' field
3. Returns:
   - Old login users (>1065 days) → Send to BluPages check
   - Recent login users (≤1065 days) → Not to be deleted list
"""

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional


class LoginValidationError(Exception):
    """Custom exception for login validation errors"""
    pass


def validate_last_login(
    input_file: str,
    days_threshold: int = 1065,
    output_dir: str = "backend/resolutions",
    timestamp: Optional[str] = None,
    append_recent: bool = True
) -> Dict:
    """
    Validate and filter users by last login date.
    
    Args:
        input_file: Path to active users JSON file
        days_threshold: Number of days threshold (default: 1065 = ~3 years)
        output_dir: Directory to save output files
        timestamp: Optional timestamp string for filenames (auto-generated if None)
        append_recent: If True, append recent users to existing not_to_be_deleted.json
        
    Returns:
        Dictionary with validation results:
        {
            "success": True,
            "validator": "last_login",
            "input_count": 950,
            "output": {
                "old_login": 234,
                "recent_login": 716
            },
            "files_created": {
                "old_login": "path/to/isv_last_login_>3.json",
                "recent_login": "path/to/not_to_be_deleted.json"
            },
            "threshold_days": 1065,
            "timestamp": "2026-04-06T10:00:00",
            "duration_seconds": 1
        }
        
    Raises:
        LoginValidationError: If validation fails
    """
    try:
        start_time = datetime.now()
        
        # Validate input file exists
        if not Path(input_file).exists():
            raise LoginValidationError(f"Input file not found: {input_file}")
        
        # Load active users
        with open(input_file, 'r') as f:
            users = json.load(f)
        
        input_count = len(users)
        
        # Current time for comparison
        current_time = datetime.now(timezone.utc)
        
        # Lists to store filtered users
        old_login_users = []  # last login > threshold days
        recent_login_users = []  # last login <= threshold days
        
        # Filter users by last login date
        for user in users:
            last_login = user.get('lastLogin')
            
            if last_login is None or last_login == "":
                # No login data - treat as old (needs BluPages check)
                old_login_users.append(user)
            else:
                try:
                    # Parse the last login date
                    last_login_date = datetime.fromisoformat(last_login.replace('Z', '+00:00'))
                    
                    # Calculate days since last login
                    days_since_login = (current_time - last_login_date).days
                    
                    if days_since_login > days_threshold:
                        old_login_users.append(user)
                    else:
                        recent_login_users.append(user)
                except (ValueError, AttributeError):
                    # Treat unparseable dates as old
                    old_login_users.append(user)
        
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Create outputs directory for final results
        outputs_dir = Path(output_dir).parent / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output file paths
        old_login_file = Path(output_dir) / f"isv_last_login_>3_{timestamp}.json"
        recent_login_file = outputs_dir / "not_to_be_deleted.json"  # Final output goes to outputs folder
        
        # Save old login users
        with open(old_login_file, 'w') as f:
            json.dump(old_login_users, f, indent=2)
        
        # Save recent login users (with append logic)
        if append_recent and recent_login_file.exists():
            try:
                with open(recent_login_file, 'r') as f:
                    existing_users = json.load(f)
                
                # Avoid duplicates by user_id
                existing_user_ids = {user['user_id'] for user in existing_users}
                new_users = [user for user in recent_login_users if user['user_id'] not in existing_user_ids]
                
                combined_users = existing_users + new_users
                
                with open(recent_login_file, 'w') as f:
                    json.dump(combined_users, f, indent=2)
            except (json.JSONDecodeError, KeyError):
                # If existing file is invalid, overwrite it
                with open(recent_login_file, 'w') as f:
                    json.dump(recent_login_users, f, indent=2)
        else:
            with open(recent_login_file, 'w') as f:
                json.dump(recent_login_users, f, indent=2)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Return standardized result
        return {
            "success": True,
            "validator": "last_login",
            "input_count": input_count,
            "output": {
                "old_login": len(old_login_users),
                "recent_login": len(recent_login_users)
            },
            "files_created": {
                "old_login": str(old_login_file),
                "recent_login": str(recent_login_file)
            },
            "threshold_days": days_threshold,
            "timestamp": end_time.isoformat(),
            "duration_seconds": int(duration)
        }
        
    except Exception as e:
        raise LoginValidationError(f"Login validation failed: {str(e)}")


# For direct usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python login_validator.py <active_users_file> [days_threshold]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    days_threshold = int(sys.argv[2]) if len(sys.argv) > 2 else 1065
    
    try:
        result = validate_last_login(input_file, days_threshold)
        print(f"\n✓ Last Login Validation Complete!")
        print(f"  Threshold: {result['threshold_days']} days (~{result['threshold_days']/365:.1f} years)")
        print(f"  Old login (>{result['threshold_days']} days): {result['output']['old_login']}")
        print(f"  Recent login (≤{result['threshold_days']} days): {result['output']['recent_login']}")
        print(f"  Duration: {result['duration_seconds']}s")
        print(f"\nFiles created:")
        print(f"  - Old login: {result['files_created']['old_login']}")
        print(f"  - Recent login: {result['files_created']['recent_login']}")
    except LoginValidationError as e:
        print(f"Error: {e}")
        sys.exit(1)

# Made with Bob
