"""
User Filtering Module - Pluggable functions for filtering and processing user data.

This module provides reusable functions that can be:
1. Imported and called directly in Python code
2. Exposed via API endpoints
3. Used in CLI scripts or scheduled jobs

Example usage:
    from backend.user_filters import split_by_active_status, filter_by_login_date
    
    # Direct function call
    active_users, inactive_users = split_by_active_status("resolved_users.json")
    
    # Or use via API endpoints defined in app.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional


class UserFilterError(Exception):
    """Custom exception for user filtering operations."""
    pass


def load_users_from_file(file_path: str) -> List[Dict]:
    """
    Load users from a JSON file.
    
    Args:
        file_path: Path to the JSON file containing user data
        
    Returns:
        List of user dictionaries
        
    Raises:
        UserFilterError: If file cannot be read or parsed
    """
    try:
        with open(file_path, 'r') as f:
            users = json.load(f)
        
        if not isinstance(users, list):
            raise UserFilterError(f"Expected list of users, got {type(users)}")
        
        return users
    except FileNotFoundError:
        raise UserFilterError(f"File not found: {file_path}")
    except json.JSONDecodeError as e:
        raise UserFilterError(f"Invalid JSON in file {file_path}: {e}")
    except Exception as e:
        raise UserFilterError(f"Error loading users from {file_path}: {e}")


def save_users_to_file(users: List[Dict], file_path: str, append: bool = False) -> int:
    """
    Save users to a JSON file.
    
    Args:
        users: List of user dictionaries to save
        file_path: Path where to save the file
        append: If True, append to existing file (avoiding duplicates by user_id)
        
    Returns:
        Number of users saved
        
    Raises:
        UserFilterError: If file cannot be written
    """
    try:
        # Ensure directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Handle append mode
        if append and Path(file_path).exists():
            try:
                existing_users = load_users_from_file(file_path)
                existing_user_ids = {user['user_id'] for user in existing_users}
                new_users = [user for user in users if user['user_id'] not in existing_user_ids]
                users = existing_users + new_users
            except UserFilterError:
                # If existing file is invalid, overwrite it
                pass
        
        # Write to file
        with open(file_path, 'w') as f:
            json.dump(users, f, indent=2)
        
        return len(users)
    except Exception as e:
        raise UserFilterError(f"Error saving users to {file_path}: {e}")


def split_by_active_status(
    input_file: str,
    output_dir: str = "backend/resolutions",
    timestamp: Optional[str] = None
) -> Tuple[str, str, int, int]:
    """
    Split users into active and inactive based on 'active' field.
    
    Args:
        input_file: Path to input JSON file with user data
        output_dir: Directory to save output files
        timestamp: Optional timestamp string for filenames (auto-generated if None)
        
    Returns:
        Tuple of (active_file_path, inactive_file_path, active_count, inactive_count)
        
    Raises:
        UserFilterError: If operation fails
    """
    # Load users
    all_users = load_users_from_file(input_file)
    
    # Filter by active status
    active_users = [user for user in all_users if user.get('active') == True]
    inactive_users = [user for user in all_users if user.get('active') == False]
    
    # Generate timestamp if not provided
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output file paths
    active_file = Path(output_dir) / f"isv_active_users_{timestamp}.json"
    inactive_file = Path(output_dir) / f"isv_inactive_users_{timestamp}.json"
    
    # Save files
    save_users_to_file(active_users, str(active_file))
    save_users_to_file(inactive_users, str(inactive_file))
    
    return str(active_file), str(inactive_file), len(active_users), len(inactive_users)


def filter_by_login_date(
    input_file: str,
    days_threshold: int = 1095,
    output_dir: str = "backend/resolutions",
    timestamp: Optional[str] = None,
    append_recent: bool = True
) -> Tuple[str, str, int, int]:
    """
    Filter users based on last login date.
    
    Args:
        input_file: Path to input JSON file with user data
        days_threshold: Number of days threshold (default: 1095 = 3 years)
        output_dir: Directory to save output files
        timestamp: Optional timestamp string for filenames (auto-generated if None)
        append_recent: If True, append recent users to existing not_to_be_deleted.json
        
    Returns:
        Tuple of (old_login_file, recent_login_file, old_count, recent_count)
        
    Raises:
        UserFilterError: If operation fails
    """
    # Load users
    users = load_users_from_file(input_file)
    
    # Current time for comparison
    current_time = datetime.now(timezone.utc)
    
    # Lists to store filtered users
    old_login_users = []  # last login > threshold days
    recent_login_users = []  # last login <= threshold days
    
    # Filter users by last login date
    for user in users:
        last_login = user.get('lastLogin')
        
        if last_login is None or last_login == "":
            # No login data - treat as old
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
    
    # Create output file paths
    old_login_file = Path(output_dir) / f"isv_last_login_>3_{timestamp}.json"
    recent_login_file = Path(output_dir) / "not_to_be_deleted.json"
    
    # Save files
    save_users_to_file(old_login_users, str(old_login_file))
    save_users_to_file(recent_login_users, str(recent_login_file), append=append_recent)
    
    return str(old_login_file), str(recent_login_file), len(old_login_users), len(recent_login_users)


def get_user_statistics(users: List[Dict]) -> Dict:
    """
    Calculate statistics for a list of users.
    
    Args:
        users: List of user dictionaries
        
    Returns:
        Dictionary with statistics
    """
    if not users:
        return {
            "total": 0,
            "active": 0,
            "inactive": 0,
            "with_login": 0,
            "without_login": 0
        }
    
    active_count = sum(1 for user in users if user.get('active') == True)
    inactive_count = sum(1 for user in users if user.get('active') == False)
    with_login = sum(1 for user in users if user.get('lastLogin'))
    without_login = len(users) - with_login
    
    return {
        "total": len(users),
        "active": active_count,
        "inactive": inactive_count,
        "with_login": with_login,
        "without_login": without_login
    }


def process_user_pipeline(
    input_file: str,
    output_dir: str = "backend/resolutions",
    days_threshold: int = 1095
) -> Dict:
    """
    Complete user processing pipeline:
    1. Split by active/inactive status
    2. Filter active users by login date
    
    Args:
        input_file: Path to resolved users JSON file
        output_dir: Directory to save output files
        days_threshold: Days threshold for login filtering (default: 1095 = 3 years)
        
    Returns:
        Dictionary with results and file paths
        
    Raises:
        UserFilterError: If operation fails
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Step 1: Split by active status
    active_file, inactive_file, active_count, inactive_count = split_by_active_status(
        input_file, output_dir, timestamp
    )
    
    # Step 2: Filter active users by login date
    old_login_file, recent_login_file, old_count, recent_count = filter_by_login_date(
        active_file, days_threshold, output_dir, timestamp
    )
    
    return {
        "timestamp": timestamp,
        "input_file": input_file,
        "files_created": {
            "active_users": active_file,
            "inactive_users": inactive_file,
            "old_login_users": old_login_file,
            "recent_login_users": recent_login_file
        },
        "counts": {
            "total_active": active_count,
            "total_inactive": inactive_count,
            "old_login": old_count,
            "recent_login": recent_count
        }
    }


def list_resolution_files(resolution_dir: str = "backend/resolutions") -> List[Dict]:
    """
    List all resolution files with metadata.
    
    Args:
        resolution_dir: Directory containing resolution files
        
    Returns:
        List of file information dictionaries
    """
    files = []
    resolution_path = Path(resolution_dir)
    
    if not resolution_path.exists():
        return files
    
    for file_path in resolution_path.glob("*.json"):
        try:
            stats = file_path.stat()
            files.append({
                "filename": file_path.name,
                "path": str(file_path),
                "size": stats.st_size,
                "modified": datetime.fromtimestamp(stats.st_mtime).isoformat(),
                "type": _classify_file_type(file_path.name)
            })
        except Exception:
            continue
    
    return sorted(files, key=lambda x: x['modified'], reverse=True)


def _classify_file_type(filename: str) -> str:
    """Classify file type based on filename pattern."""
    if "resolved_users" in filename:
        return "resolved"
    elif "isv_active_users" in filename:
        return "active"
    elif "isv_inactive_users" in filename:
        return "inactive"
    elif "isv_last_login" in filename:
        return "old_login"
    elif "not_to_be_deleted" in filename:
        return "keep"
    elif "failed_ids" in filename:
        return "failed"
    else:
        return "other"

# Made with Bob
