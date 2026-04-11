"""
Test script for the Dormant ID Decision Engine

This script creates sample data and tests the decision engine to ensure
it correctly consolidates all validation results into a single JSON output.
"""

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta


def create_test_data():
    """Create sample test data for each validation stage"""
    
    # Create test directories
    Path("backend/resolutions").mkdir(parents=True, exist_ok=True)
    Path("backend/outputs").mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. ISV Failed IDs (not found in ISV)
    isv_failed = ["user001", "user002", "user003"]
    isv_failed_file = f"backend/outputs/isv_failed_ids_{timestamp}.json"
    with open(isv_failed_file, 'w') as f:
        json.dump(isv_failed, f, indent=2)
    
    # 2. ISV Inactive Users (found in ISV but inactive)
    isv_inactive = [
        {
            "user_id": "user004",
            "username": "inactive.user@ibm.com",
            "email": "inactive.user@ibm.com",
            "active": False,
            "lastLogin": "2020-01-15T10:30:00Z"
        },
        {
            "user_id": "user005",
            "username": "another.inactive@ibm.com",
            "email": "another.inactive@ibm.com",
            "active": False,
            "lastLogin": "2019-06-20T14:45:00Z"
        }
    ]
    isv_inactive_file = f"backend/outputs/isv_inactive_users_{timestamp}.json"
    with open(isv_inactive_file, 'w') as f:
        json.dump(isv_inactive, f, indent=2)
    
    # 3. Users to be deleted (not found in BluPages)
    to_be_deleted = [
        {
            "user_id": "user006",
            "username": "old.user@ibm.com",
            "email": "old.user@ibm.com",
            "active": True,
            "lastLogin": "2020-03-10T08:15:00Z"
        },
        {
            "user_id": "user007",
            "username": "dormant.account@ibm.com",
            "email": "dormant.account@ibm.com",
            "active": True,
            "lastLogin": "2019-12-25T16:20:00Z"
        }
    ]
    to_be_deleted_file = f"backend/outputs/to_be_deleted_{timestamp}.json"
    with open(to_be_deleted_file, 'w') as f:
        json.dump(to_be_deleted, f, indent=2)
    
    # 4. Users not to be deleted (found in BluPages or recent login)
    not_to_be_deleted = [
        {
            "user_id": "user008",
            "username": "active.user@ibm.com",
            "email": "active.user@ibm.com",
            "active": True,
            "lastLogin": (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        },
        {
            "user_id": "user009",
            "username": "recent.login@ibm.com",
            "email": "recent.login@ibm.com",
            "active": True,
            "lastLogin": (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        },
        {
            "user_id": "user010",
            "username": "bluepages.verified@ibm.com",
            "email": "bluepages.verified@ibm.com",
            "active": True,
            "lastLogin": "2020-05-15T12:00:00Z"
        }
    ]
    not_to_be_deleted_file = "backend/outputs/not_to_be_deleted.json"
    with open(not_to_be_deleted_file, 'w') as f:
        json.dump(not_to_be_deleted, f, indent=2)
    
    # Create mock pipeline results
    pipeline_results = {
        "success": True,
        "pipeline": "validation_pipeline",
        "timestamp": datetime.now().isoformat(),
        "input_file": "test_extraction.json",
        "checks_run": ["isv_validation", "active_status", "last_login", "bluepages"],
        "results": {
            "isv_validation": {
                "success": True,
                "validator": "isv_validation",
                "input_count": 10,
                "output": {
                    "found_in_isv": 7,
                    "not_found_in_isv": 3
                },
                "files_created": {
                    "resolved": f"backend/resolutions/isv_resolved_users_{timestamp}.json",
                    "failed": isv_failed_file
                }
            },
            "active_status": {
                "success": True,
                "validator": "active_status",
                "input_count": 7,
                "output": {
                    "active": 5,
                    "inactive": 2
                },
                "files_created": {
                    "active": f"backend/resolutions/isv_active_users_{timestamp}.json",
                    "inactive": isv_inactive_file
                }
            },
            "last_login": {
                "success": True,
                "validator": "last_login",
                "input_count": 5,
                "output": {
                    "old_login": 2,
                    "recent_login": 3
                },
                "files_created": {
                    "old_login": f"backend/resolutions/isv_last_login_>3_{timestamp}.json",
                    "recent_login": not_to_be_deleted_file
                }
            },
            "bluepages": {
                "success": True,
                "validator": "bluepages",
                "input_count": 2,
                "output": {
                    "found_in_bluepages": 1,
                    "not_found_in_bluepages": 2
                },
                "files_created": {
                    "to_delete": to_be_deleted_file,
                    "not_to_delete": not_to_be_deleted_file
                }
            }
        }
    }
    
    return pipeline_results


def test_decision_engine():
    """Test the decision engine with sample data"""
    
    print("="*70)
    print("TESTING DORMANT ID DECISION ENGINE")
    print("="*70)
    print()
    
    # Create test data
    print("Creating test data...")
    pipeline_results = create_test_data()
    print("✓ Test data created\n")
    
    # Import and run decision engine
    from backend.validators.decision_engine import consolidate_decisions
    
    print("Running decision engine...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result = consolidate_decisions(
        pipeline_results=pipeline_results,
        timestamp=timestamp
    )
    
    # Verify output
    print("\n" + "="*70)
    print("VERIFICATION")
    print("="*70)
    
    output_file = Path(result["output_file"])
    if output_file.exists():
        print(f"✓ Output file created: {output_file}")
        
        with open(output_file, 'r') as f:
            decisions = json.load(f)
        
        print(f"\nDecision Categories:")
        print(f"  - to_be_deleted: {len(decisions['to_be_deleted'])} users")
        print(f"  - not_to_be_deleted: {len(decisions['not_to_be_deleted'])} users")
        print(f"  - isv_inactive_users: {len(decisions['isv_inactive_users'])} users")
        print(f"  - isv_failed_ids: {len(decisions['isv_failed_ids'])} users")
        
        print(f"\nSample entries:")
        
        if decisions['to_be_deleted']:
            print(f"\n  to_be_deleted[0]:")
            print(f"    {json.dumps(decisions['to_be_deleted'][0], indent=6)}")
        
        if decisions['not_to_be_deleted']:
            print(f"\n  not_to_be_deleted[0]:")
            print(f"    {json.dumps(decisions['not_to_be_deleted'][0], indent=6)}")
        
        if decisions['isv_inactive_users']:
            print(f"\n  isv_inactive_users[0]:")
            print(f"    {json.dumps(decisions['isv_inactive_users'][0], indent=6)}")
        
        if decisions['isv_failed_ids']:
            print(f"\n  isv_failed_ids[0]:")
            print(f"    {json.dumps(decisions['isv_failed_ids'][0], indent=6)}")
        
        print(f"\n{'='*70}")
        print("✓ TEST PASSED - Decision engine working correctly!")
        print("="*70)
        
    else:
        print(f"✗ Output file not created!")
        return False
    
    return True


if __name__ == "__main__":
    try:
        success = test_decision_engine()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

# Made with Bob