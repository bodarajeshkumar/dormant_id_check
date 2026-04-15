"""
Validation Pipeline Orchestrator

This module orchestrates the complete validation pipeline, running selected
validators in sequence based on user configuration (UI checkboxes).

Pipeline Flow:
1. ISV Validation (if selected)
2. Active Status Check (if selected)
3. Last Login Check (if selected)
4. BluPages Validation (if selected)

Each step uses the output from the previous step.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Callable
import asyncio

from .isv_validator import validate_isv
from .active_status_validator import validate_active_status
from .login_validator import validate_last_login
from .bluepages_validator import validate_bluepages
from .decision_engine import consolidate_decisions


class PipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass


async def run_validation_pipeline(
    input_file: str,
    output_dir: str = "backend/resolutions",
    checks: Optional[Dict[str, bool]] = None,
    days_threshold: int = 1065,
    max_concurrent: int = 10,
    batch_size: int = 100,
    status_callback: Optional[Callable] = None
) -> Dict:
    """
    Run the complete validation pipeline with selected checks.
    
    Args:
        input_file: Path to extraction JSON file
        output_dir: Directory to save output files
        checks: Dictionary of checks to run:
            {
                "isv_validation": True/False,
                "active_status": True/False,
                "last_login": True/False,
                "bluepages": True/False
            }
        days_threshold: Days threshold for login check (default: 1065 = ~3 years)
        max_concurrent: Maximum concurrent requests for async operations
        batch_size: Batch size for processing
        
    Returns:
        Dictionary with complete pipeline results:
        {
            "success": True,
            "pipeline": "validation_pipeline",
            "timestamp": "2026-04-06T10:00:00",
            "input_file": "extraction_*.json",
            "checks_run": ["isv_validation", "active_status", ...],
            "results": {
                "isv_validation": {...},
                "active_status": {...},
                "last_login": {...},
                "bluepages": {...}
            },
            "final_outputs": {
                "to_delete": "path/to/to_be_deleted.json",
                "not_to_delete": "path/to/not_to_be_deleted.json",
                "inactive": "path/to/isv_inactive_users.json"
            },
            "summary": {
                "total_input": 1000,
                "found_in_isv": 950,
                "active": 900,
                "recent_login": 700,
                "old_login": 200,
                "to_delete": 50,
                "not_to_delete": 850
            },
            "duration_seconds": 180
        }
        
    Raises:
        PipelineError: If pipeline execution fails
    """
    try:
        start_time = datetime.now()
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")
        
        # Default checks: run all if not specified
        if checks is None:
            checks = {
                "isv_validation": True,
                "active_status": True,
                "last_login": True,
                "bluepages": True
            }
        
        # Validate input file
        if not Path(input_file).exists():
            raise PipelineError(f"Input file not found: {input_file}")
        
        # Track results from each step
        results = {}
        checks_run = []
        current_file = input_file
        
        # Summary statistics
        summary = {
            "total_input": 0,
            "found_in_isv": 0,
            "not_found_in_isv": 0,
            "active": 0,
            "inactive": 0,
            "recent_login": 0,
            "old_login": 0,
            "found_in_bluepages": 0,
            "not_found_in_bluepages": 0,
            "to_delete": 0,
            "not_to_delete": 0
        }
        
        # Get initial count
        with open(input_file, 'r') as f:
            initial_data = json.load(f)
            summary["total_input"] = len(initial_data)
        
        print(f"\n{'='*70}")
        print(f"VALIDATION PIPELINE - Starting")
        print(f"{'='*70}")
        print(f"Input file: {input_file}")
        print(f"Total records: {summary['total_input']}")
        print(f"Checks to run: {[k for k, v in checks.items() if v]}")
        print(f"{'='*70}\n")
        
        # Step 1: ISV Validation
        if checks.get("isv_validation", False):
            print(f"[1/4] Running ISV Validation...")
            if status_callback:
                status_callback("ISV Validation", "running")
            result = await validate_isv(
                input_file=current_file,
                output_dir=output_dir,
                batch_size=batch_size,
                max_concurrent=max_concurrent
            )
            results["isv_validation"] = result
            checks_run.append("isv_validation")
            
            summary["found_in_isv"] = result["output"]["found_in_isv"]
            summary["not_found_in_isv"] = result["output"]["not_found_in_isv"]
            
            # Use resolved file for next step
            current_file = result["files_created"]["resolved"]
            print(f"✓ ISV Validation complete: {result['output']['found_in_isv']} found, {result['output']['not_found_in_isv']} not found\n")
            if status_callback:
                status_callback("ISV Validation", "completed")
        
        # Step 2: Active Status Check
        if checks.get("active_status", False):
            print(f"[2/4] Running Active Status Check...")
            if status_callback:
                status_callback("Dormancy Check", "running")
            result = validate_active_status(
                input_file=current_file,
                output_dir=output_dir,
                timestamp=timestamp
            )
            results["active_status"] = result
            checks_run.append("active_status")
            
            summary["active"] = result["output"]["active"]
            summary["inactive"] = result["output"]["inactive"]
            
            # Use active file for next step
            current_file = result["files_created"]["active"]
            print(f"✓ Active Status Check complete: {result['output']['active']} active, {result['output']['inactive']} inactive\n")
            if status_callback:
                status_callback("Dormancy Check", "completed")
        
        # Step 3: Last Login Check
        if checks.get("last_login", False):
            print(f"[3/4] Running Last Login Check...")
            if status_callback:
                status_callback("Last Login Check", "running")
            result = validate_last_login(
                input_file=current_file,
                days_threshold=days_threshold,
                output_dir=output_dir,
                timestamp=timestamp,
                append_recent=True
            )
            results["last_login"] = result
            checks_run.append("last_login")
            
            summary["old_login"] = result["output"]["old_login"]
            summary["recent_login"] = result["output"]["recent_login"]
            
            # Use old login file for BluPages check
            current_file = result["files_created"]["old_login"]
            print(f"✓ Last Login Check complete: {result['output']['old_login']} old (>{days_threshold} days), {result['output']['recent_login']} recent\n")
            if status_callback:
                status_callback("Last Login Check", "completed")
        
        # Step 4: BluPages Validation
        if checks.get("bluepages", False):
            print(f"[4/4] Filtering IBM emails and running BluPages Validation...")
            if status_callback:
                status_callback("BluPages Validation", "running")
            
            # Filter to only @ibm.com or *.ibm.com emails before BluPages
            with open(current_file, 'r') as f:
                users = json.load(f)
            
            ibm_users = []
            non_ibm_users = []
            
            for user in users:
                email = user.get('email', '')
                # Only IBM emails (ending with @ibm.com or .ibm.com) go to BluPages
                # Exclude mail.test.*.ibm.com and malinator.com (already filtered in bluepages_validator_async.py)
                if email.endswith('@ibm.com') or '.ibm.com' in email:
                    ibm_users.append(user)
                else:
                    non_ibm_users.append(user)
            
            print(f"  IBM emails: {len(ibm_users)}, Non-IBM emails: {len(non_ibm_users)}")
            
            # Add non-IBM users directly to to_be_deleted (per flowchart: remaining mails go to "To be deleted")
            if non_ibm_users:
                # Use the same outputs directory structure
                outputs_dir = Path("backend/backend/outputs")
                outputs_dir.mkdir(parents=True, exist_ok=True)
                to_delete_file = outputs_dir / f"to_be_deleted_{timestamp}.json"
                
                if to_delete_file.exists():
                    with open(to_delete_file, 'r') as f:
                        existing = json.load(f)
                    existing.extend(non_ibm_users)
                    with open(to_delete_file, 'w') as f:
                        json.dump(existing, f, indent=2)
                else:
                    with open(to_delete_file, 'w') as f:
                        json.dump(non_ibm_users, f, indent=2)
            
            # Run BluPages validation only on IBM users - pass data directly
            if ibm_users:
                result = await validate_bluepages(
                    users_data=ibm_users,
                    output_dir=output_dir,
                    timestamp=timestamp,
                    max_concurrent=max_concurrent,
                    batch_size=batch_size
                )
                results["bluepages"] = result
                checks_run.append("bluepages")
                
                summary["found_in_bluepages"] = result["output"]["found_in_bluepages"]
                summary["not_found_in_bluepages"] = result["output"]["not_found_in_bluepages"]
                # Non-IBM users + users not found in BluPages = to_delete
                summary["to_delete"] = result["output"]["not_found_in_bluepages"] + len(non_ibm_users)
                # Users found in BluPages + recent login users = not_to_delete
                summary["not_to_delete"] = result["output"]["found_in_bluepages"] + summary.get("recent_login", 0)
                
                print(f"✓ BluPages Validation complete: {result['output']['found_in_bluepages']} found, {result['output']['not_found_in_bluepages']} not found")
                print(f"  Non-IBM emails marked for deletion: {len(non_ibm_users)}")
                if status_callback:
                    status_callback("BluPages Validation", "completed")
            else:
                print(f"✓ No IBM users to validate via BluPages")
                if status_callback:
                    status_callback("BluPages Validation", "completed")
                # All non-IBM users go to to_delete
                summary["to_delete"] = len(non_ibm_users)
                summary["not_to_delete"] = summary.get("recent_login", 0)
                
                # Non-IBM users already added to to_be_deleted file above
                outputs_dir = Path("backend/backend/outputs")
                outputs_dir.mkdir(parents=True, exist_ok=True)
                to_delete_file = outputs_dir / f"to_be_deleted_{timestamp}.json"
                
                # Store result for final outputs
                results["bluepages"] = {
                    "success": True,
                    "validator": "bluepages",
                    "input_count": 0,
                    "output": {
                        "found_in_bluepages": 0,
                        "not_found_in_bluepages": 0
                    },
                    "files_created": {
                        "to_delete": str(to_delete_file),
                        "not_to_delete": str(outputs_dir / "not_to_be_deleted.json")
                    }
                }
                checks_run.append("bluepages")
            
            print()
        
        # Calculate total duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Collect final output files
        final_outputs = {}
        if "bluepages" in results:
            final_outputs["to_delete"] = results["bluepages"]["files_created"]["to_delete"]
            final_outputs["not_to_delete"] = results["bluepages"]["files_created"]["not_to_delete"]
        elif "last_login" in results:
            final_outputs["old_login"] = results["last_login"]["files_created"]["old_login"]
            final_outputs["recent_login"] = results["last_login"]["files_created"]["recent_login"]
        
        if "active_status" in results:
            final_outputs["inactive"] = results["active_status"]["files_created"]["inactive"]
        
        if "isv_validation" in results:
            final_outputs["failed_isv"] = results["isv_validation"]["files_created"]["failed"]
        
        print(f"{'='*70}")
        print(f"PIPELINE COMPLETE")
        print(f"{'='*70}")
        print(f"Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        print(f"Checks run: {len(checks_run)}")
        print(f"\nSummary:")
        for key, value in summary.items():
            if value > 0:
                print(f"  {key}: {value}")
        print(f"{'='*70}\n")
        
        # Build pipeline results for decision engine
        pipeline_result = {
            "success": True,
            "pipeline": "validation_pipeline",
            "timestamp": end_time.isoformat(),
            "input_file": input_file,
            "checks_run": checks_run,
            "results": results,
            "final_outputs": final_outputs,
            "summary": summary,
            "duration_seconds": int(duration)
        }
        
        # Run decision engine to consolidate all results into single JSON with timestamp
        print(f"Running Decision Engine to consolidate results...")
        decision_result = consolidate_decisions(
            pipeline_results=pipeline_result,
            timestamp=timestamp
        )
        
        # Add decision engine output to pipeline result
        pipeline_result["decision_output"] = decision_result["output_file"]
        pipeline_result["decision_summary"] = decision_result["summary"]
        
        # Clean up intermediate files - keep only the final decision file
        print(f"\nCleaning up intermediate files...")
        files_to_remove = []
        
        # Collect all intermediate files from validation steps
        for step_name, step_result in results.items():
            if "files_created" in step_result:
                for file_type, file_path in step_result["files_created"].items():
                    if file_path and Path(file_path).exists():
                        # Don't delete the final decision file
                        if file_path != decision_result["output_file"]:
                            files_to_remove.append(file_path)
        
        # Remove intermediate files
        removed_count = 0
        for file_path in files_to_remove:
            try:
                Path(file_path).unlink()
                removed_count += 1
            except Exception as e:
                print(f"  Warning: Could not remove {file_path}: {e}")
        
        print(f"✓ Removed {removed_count} intermediate files")
        print(f"✓ Final output: {decision_result['output_file']}\n")
        
        return pipeline_result
        
    except Exception as e:
        raise PipelineError(f"Pipeline execution failed: {str(e)}")


def run_validation_pipeline_sync(
    input_file: str,
    output_dir: str = "backend/resolutions",
    checks: Optional[Dict[str, bool]] = None,
    days_threshold: int = 1065,
    max_concurrent: int = 10,
    batch_size: int = 100
) -> Dict:
    """
    Synchronous wrapper for run_validation_pipeline.
    
    Use this when calling from non-async code.
    """
    return asyncio.run(run_validation_pipeline(
        input_file, output_dir, checks, days_threshold, max_concurrent, batch_size
    ))


# For direct usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python pipeline.py <extraction_file> [checks]")
        print("\nExample:")
        print("  python pipeline.py extraction.json")
        print("  python pipeline.py extraction.json isv,active,login,bluepages")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    # Parse checks from command line
    checks = None
    if len(sys.argv) > 2:
        check_names = sys.argv[2].split(',')
        checks = {
            "isv_validation": "isv" in check_names,
            "active_status": "active" in check_names,
            "last_login": "login" in check_names,
            "bluepages": "bluepages" in check_names
        }
    
    try:
        result = run_validation_pipeline_sync(input_file, checks=checks)
        print(f"\n✓ Pipeline Complete!")
        print(f"  Duration: {result['duration_seconds']}s")
        print(f"  Checks run: {', '.join(result['checks_run'])}")
        print(f"\nFinal outputs:")
        for name, path in result['final_outputs'].items():
            print(f"  - {name}: {path}")
    except PipelineError as e:
        print(f"Error: {e}")
        sys.exit(1)

# Made with Bob
