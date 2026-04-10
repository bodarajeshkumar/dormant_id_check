"""
Dormant ID Decision Engine

This module consolidates all validation results into a single unified JSON output
with four categories:
- to_be_deleted: Users who failed BluPages validation (not found in BluPages)
- not_to_be_deleted: Users who passed all checks or have recent login
- isv_inactive: Users who are inactive in ISV
- isv_failed: Users who were not found in ISV

Each user entry includes the reason mentioning the FINAL step where the decision was made.
"""

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional


class DecisionEngineError(Exception):
    """Custom exception for decision engine errors"""
    pass


def consolidate_decisions(
    pipeline_results: Dict,
    output_file: Optional[str] = None,
    timestamp: Optional[str] = None
) -> Dict:
    """
    Consolidate all validation results into a single unified decision file.
    
    Args:
        pipeline_results: Results dictionary from run_validation_pipeline
        output_file: Path to save the unified decision JSON (auto-generated if None)
        timestamp: Timestamp string for filename (auto-generated if None)
        
    Returns:
        Dictionary with consolidated decisions:
        {
            "to_be_deleted": [...],
            "not_to_be_deleted": [...],
            "isv_inactive_users": [...],
            "isv_failed_ids": [...]
        }
    """
    try:
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate output file path if not provided
        if output_file is None:
            output_file = f"backend/outputs/dormant_id_decisions_{timestamp}.json"
        
        # Initialize decision categories
        decisions = {
            "to_be_deleted": [],
            "not_to_be_deleted": [],
            "isv_inactive_users": [],
            "isv_failed_ids": []
        }
        
        # Track processed user IDs to avoid duplicates
        processed_ids = set()
        
        # Process ISV failed users (not found in ISV)
        if "isv_validation" in pipeline_results.get("results", {}):
            isv_result = pipeline_results["results"]["isv_validation"]
            failed_file = isv_result.get("files_created", {}).get("failed")
            
            if failed_file and Path(failed_file).exists():
                with open(failed_file, 'r') as f:
                    failed_ids = json.load(f)
                
                for user_id in failed_ids:
                    if user_id not in processed_ids:
                        decisions["isv_failed_ids"].append({
                            "id": user_id,
                            "username": user_id,
                            "lastLogin": None,
                            "activeStatus": None,
                            "reasons": ["User not found in ISV (IBM Users API) - FINAL DECISION: ISV Validation Failed"]
                        })
                        processed_ids.add(user_id)
        
        # Process ISV inactive users
        if "active_status" in pipeline_results.get("results", {}):
            active_result = pipeline_results["results"]["active_status"]
            inactive_file = active_result.get("files_created", {}).get("inactive")
            
            if inactive_file and Path(inactive_file).exists():
                with open(inactive_file, 'r') as f:
                    inactive_users = json.load(f)
                
                for user in inactive_users:
                    user_id = user.get("user_id", user.get("id", ""))
                    if user_id not in processed_ids:
                        decisions["isv_inactive_users"].append({
                            "id": user_id,
                            "username": user.get("username", user.get("email", user_id)),
                            "lastLogin": user.get("lastLogin"),
                            "activeStatus": False,
                            "reasons": ["User marked as inactive in ISV - FINAL DECISION: Active Status Check Failed"]
                        })
                        processed_ids.add(user_id)
        
        # Process BluPages results (final decision for active users with old login)
        if "bluepages" in pipeline_results.get("results", {}):
            bluepages_result = pipeline_results["results"]["bluepages"]
            
            # Users not found in BluPages → to_be_deleted
            to_delete_file = bluepages_result.get("files_created", {}).get("to_delete")
            if to_delete_file and Path(to_delete_file).exists():
                with open(to_delete_file, 'r') as f:
                    to_delete_users = json.load(f)
                
                for user in to_delete_users:
                    user_id = user.get("user_id", user.get("id", ""))
                    if user_id not in processed_ids:
                        decisions["to_be_deleted"].append({
                            "id": user_id,
                            "username": user.get("username", user.get("email", user_id)),
                            "lastLogin": user.get("lastLogin"),
                            "activeStatus": user.get("active", True),
                            "reasons": [
                                "User has old login (>1065 days)",
                                "User not found in IBM BluPages - FINAL DECISION: BluPages Validation Failed"
                            ]
                        })
                        processed_ids.add(user_id)
            
            # Users found in BluPages → not_to_be_deleted
            not_delete_file = bluepages_result.get("files_created", {}).get("not_to_delete")
            if not_delete_file and Path(not_delete_file).exists():
                with open(not_delete_file, 'r') as f:
                    not_delete_users = json.load(f)
                
                for user in not_delete_users:
                    user_id = user.get("user_id", user.get("id", ""))
                    if user_id not in processed_ids:
                        # Determine the reason based on what checks passed
                        reasons = []
                        last_login = user.get("lastLogin")
                        
                        # Check if this user came from BluPages validation or recent login
                        if last_login:
                            try:
                                last_login_date = datetime.fromisoformat(last_login.replace('Z', '+00:00'))
                                current_time = datetime.now(timezone.utc)
                                days_since_login = (current_time - last_login_date).days
                                
                                if days_since_login <= 1065:
                                    reasons.append(f"User has recent login (≤1065 days, actual: {days_since_login} days) - FINAL DECISION: Last Login Check Passed")
                                else:
                                    reasons.append(f"User has old login (>{days_since_login} days)")
                                    reasons.append("User found in IBM BluPages - FINAL DECISION: BluPages Validation Passed")
                            except:
                                reasons.append("User found in IBM BluPages - FINAL DECISION: BluPages Validation Passed")
                        else:
                            reasons.append("User found in IBM BluPages - FINAL DECISION: BluPages Validation Passed")
                        
                        decisions["not_to_be_deleted"].append({
                            "id": user_id,
                            "username": user.get("username", user.get("email", user_id)),
                            "lastLogin": last_login,
                            "activeStatus": user.get("active", True),
                            "reasons": reasons
                        })
                        processed_ids.add(user_id)
        
        # If BluPages wasn't run, check for recent login users
        elif "last_login" in pipeline_results.get("results", {}):
            login_result = pipeline_results["results"]["last_login"]
            recent_file = login_result.get("files_created", {}).get("recent_login")
            
            if recent_file and Path(recent_file).exists():
                with open(recent_file, 'r') as f:
                    recent_users = json.load(f)
                
                for user in recent_users:
                    user_id = user.get("user_id", user.get("id", ""))
                    if user_id not in processed_ids:
                        last_login = user.get("lastLogin")
                        days_since = "unknown"
                        
                        if last_login:
                            try:
                                last_login_date = datetime.fromisoformat(last_login.replace('Z', '+00:00'))
                                current_time = datetime.now(timezone.utc)
                                days_since = (current_time - last_login_date).days
                            except:
                                pass
                        
                        decisions["not_to_be_deleted"].append({
                            "id": user_id,
                            "username": user.get("username", user.get("email", user_id)),
                            "lastLogin": last_login,
                            "activeStatus": user.get("active", True),
                            "reasons": [f"User has recent login (≤1065 days, actual: {days_since} days) - FINAL DECISION: Last Login Check Passed"]
                        })
                        processed_ids.add(user_id)
        
        # Create output directory
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save consolidated decisions
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(decisions, f, indent=2, ensure_ascii=False)
        
        # Print summary
        print(f"\n{'='*70}")
        print(f"DECISION ENGINE - Consolidated Results")
        print(f"{'='*70}")
        print(f"Output file: {output_file}")
        print(f"\nDecision Summary:")
        print(f"  To be deleted: {len(decisions['to_be_deleted'])}")
        print(f"  Not to be deleted: {len(decisions['not_to_be_deleted'])}")
        print(f"  ISV inactive users: {len(decisions['isv_inactive_users'])}")
        print(f"  ISV failed IDs: {len(decisions['isv_failed_ids'])}")
        print(f"  Total processed: {len(processed_ids)}")
        print(f"{'='*70}\n")
        
        return {
            "success": True,
            "output_file": output_file,
            "decisions": decisions,
            "summary": {
                "to_be_deleted": len(decisions['to_be_deleted']),
                "not_to_be_deleted": len(decisions['not_to_be_deleted']),
                "isv_inactive_users": len(decisions['isv_inactive_users']),
                "isv_failed_ids": len(decisions['isv_failed_ids']),
                "total_processed": len(processed_ids)
            }
        }
        
    except Exception as e:
        raise DecisionEngineError(f"Decision consolidation failed: {str(e)}")


# For direct usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python decision_engine.py <pipeline_results.json>")
        print("\nThis script consolidates pipeline results into a single decision file.")
        sys.exit(1)
    
    results_file = sys.argv[1]
    
    try:
        with open(results_file, 'r') as f:
            pipeline_results = json.load(f)
        
        result = consolidate_decisions(pipeline_results)
        print(f"\n✓ Decision consolidation complete!")
        print(f"  Output: {result['output_file']}")
        print(f"  Total decisions: {result['summary']['total_processed']}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

# Made with Bob