#!/usr/bin/env python3
"""
Bluepages Validator Script (Async Version)
High-performance async validation of user emails against IBM Bluepages API.
Uses concurrent requests for significantly faster processing.
"""

import json
import asyncio
import aiohttp
import time
import sys
from typing import List, Dict, Tuple
from datetime import datetime
import os


async def check_bluepages_email(email: str, session: aiohttp.ClientSession,
                                semaphore: asyncio.Semaphore) -> Tuple[bool, str]:
    """
    Check if an email exists in IBM Bluepages (async).
    
    Args:
        email: Email address to check
        session: Aiohttp session for connection pooling
        semaphore: Semaphore to limit concurrent requests
        
    Returns:
        Tuple of (exists: bool, status_message: str)
    """
    async with semaphore:
        # Use email as-is (don't force @ibm.com since we have various domains)
        api_url = f"https://bluepages.ibm.com/BpHttpApisv3/slaphapi?ibmperson/(mail={email}).list/bytext?*"
        
        try:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    response_text = await response.text()
                    response_text = response_text.strip()
                    
                    # Parse the response format: "# rc=0, count=N, message=Success"
                    # count=0 means not found, count>0 means found
                    import re
                    count_match = re.search(r'count=(\d+)', response_text)
                    
                    if count_match:
                        count = int(count_match.group(1))
                        if count > 0:
                            return True, f"Found in Bluepages (count={count})"
                        else:
                            return False, "Not found in Bluepages (count=0)"
                    else:
                        # Fallback: if we can't parse count, check response length
                        if len(response_text) < 50:
                            return False, "Not found (empty response)"
                        return True, "Found in Bluepages (unparsed)"
                
                elif response.status == 404:
                    return False, "Not found (404)"
                
                else:
                    return False, f"API error (status {response.status})"
                    
        except asyncio.TimeoutError:
            return False, "Timeout"
        except aiohttp.ClientError as e:
            return False, f"Request error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"


async def process_batch(users: List[Dict], session: aiohttp.ClientSession,
                       semaphore: asyncio.Semaphore) -> Tuple[List[Dict], List[Dict]]:
    """
    Process a batch of users concurrently.
    
    Returns:
        Tuple of (to_be_deleted, not_to_delete)
    """
    tasks = []
    for user in users:
        email = user.get('email', '')
        
        # Skip mail.test.*.ibm.com emails - treat them as "to be deleted"
        if email and 'mail.test.' in email and email.endswith('.ibm.com'):
            # Don't check bluepages, automatically mark for deletion
            tasks.append((user, None))
        else:
            task = check_bluepages_email(email, session, semaphore)
            tasks.append((user, task))
    
    # Gather only the non-None tasks
    tasks_to_run = [task for _, task in tasks if task is not None]
    results = await asyncio.gather(*tasks_to_run) if tasks_to_run else []
    
    to_be_deleted = []
    not_to_delete = []
    
    result_idx = 0
    for user, task in tasks:
        if task is None:
            # This was a mail.test.*.ibm.com email - mark for deletion
            to_be_deleted.append(user)
        else:
            # Get the result from the gathered results
            exists, status = results[result_idx]
            result_idx += 1
            
            if exists:
                not_to_delete.append(user)
            else:
                to_be_deleted.append(user)
    
    return to_be_deleted, not_to_delete


def save_checkpoint(to_be_deleted: List[Dict], not_to_delete: List[Dict], 
                    processed_count: int, checkpoint_file: str = "validation_checkpoint_async.json"):
    """Save progress checkpoint to resume if interrupted."""
    checkpoint_data = {
        "processed_count": processed_count,
        "timestamp": datetime.now().isoformat(),
        "to_be_deleted": to_be_deleted,
        "not_to_delete": not_to_delete
    }
    
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)


def load_checkpoint(checkpoint_file: str = "validation_checkpoint_async.json") -> Tuple[List[Dict], List[Dict], int]:
    """Load checkpoint if exists."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
            return data['to_be_deleted'], data['not_to_delete'], data['processed_count']
        except Exception as e:
            print(f"Warning: Could not load checkpoint: {e}")
    
    return [], [], 0


async def validate_users_async(input_file: str, 
                               to_delete_file: str = "to_be_deleted.json",
                               not_to_delete_file: str = "not_to_delete.json",
                               test_mode: bool = False,
                               test_limit: int = 10,
                               resume: bool = False,
                               max_concurrent: int = 50,
                               batch_size: int = 100):
    """
    Validate users against Bluepages API using async/concurrent requests.
    
    Args:
        input_file: Path to input JSON file with user records
        to_delete_file: Output file for users not found in Bluepages
        not_to_delete_file: Output file for users found in Bluepages
        test_mode: If True, only process first test_limit users
        test_limit: Number of users to process in test mode
        resume: If True, resume from checkpoint
        max_concurrent: Maximum number of concurrent requests
        batch_size: Number of users to process before saving checkpoint
    """
    print("=" * 70)
    print("IBM Bluepages Email Validator (ASYNC)")
    print("=" * 70)
    
    # Read input file
    print(f"\n📂 Reading input file: {input_file}")
    with open(input_file, 'r') as f:
        users = json.load(f)
    
    total = len(users)
    print(f"✓ Loaded {total:,} user records")
    
    # Load checkpoint if resuming
    to_be_deleted = []
    not_to_delete = []
    start_index = 0
    
    if resume:
        print("\n🔄 Checking for checkpoint...")
        to_be_deleted, not_to_delete, start_index = load_checkpoint()
        if start_index > 0:
            print(f"✓ Resuming from record {start_index:,}")
            print(f"  - Already processed: {start_index:,} users")
            print(f"  - To be deleted so far: {len(to_be_deleted):,}")
            print(f"  - Not to delete so far: {len(not_to_delete):,}")
    
    # Test mode
    if test_mode:
        users = users[start_index:start_index + test_limit]
        total = len(users)
        print(f"\n⚠️  TEST MODE: Processing only {total} users")
    else:
        users = users[start_index:]
        total = len(users)
    
    print(f"\n🔍 Starting async validation of {total:,} users...")
    print(f"⚡ Concurrency: {max_concurrent} simultaneous requests")
    print(f"📦 Batch size: {batch_size} users per checkpoint")
    print(f"⏱️  Estimated time: ~{(total / max_concurrent * 0.5) / 60:.1f} minutes")
    print("-" * 70)
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Create aiohttp session
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)
    
    start_time = time.time()
    processed = 0
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process in batches
        for i in range(0, len(users), batch_size):
            batch = users[i:i + batch_size]
            batch_start = time.time()
            
            # Process batch
            batch_to_delete, batch_not_delete = await process_batch(batch, session, semaphore)
            
            # Update results
            to_be_deleted.extend(batch_to_delete)
            not_to_delete.extend(batch_not_delete)
            processed += len(batch)
            
            # Calculate stats
            batch_time = time.time() - batch_start
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (total - processed) / rate if rate > 0 else 0
            
            # Progress output
            print(f"[{start_index + processed:,}/{start_index + total:,}] "
                  f"Batch: {len(batch)} users in {batch_time:.1f}s | "
                  f"Rate: {rate:.1f}/s | "
                  f"To delete: {len(to_be_deleted):,} | "
                  f"To keep: {len(not_to_delete):,} | "
                  f"ETA: {remaining/60:.1f}m")
            
            # Save checkpoint
            save_checkpoint(to_be_deleted, not_to_delete, start_index + processed)
    
    # Write final results
    print("\n" + "=" * 70)
    print("💾 Writing results...")
    
    with open(to_delete_file, 'w') as f:
        json.dump(to_be_deleted, f, indent=2)
    print(f"✓ Created: {to_delete_file}")
    
    with open(not_to_delete_file, 'w') as f:
        json.dump(not_to_delete, f, indent=2)
    print(f"✓ Created: {not_to_delete_file}")
    
    # Clean up checkpoint
    checkpoint_file = "validation_checkpoint_async.json"
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        print("✓ Removed checkpoint file")
    
    # Summary
    elapsed_time = time.time() - start_time
    print("\n" + "=" * 70)
    print("📊 VALIDATION SUMMARY")
    print("=" * 70)
    print(f"Total processed:     {len(to_be_deleted) + len(not_to_delete):,} users")
    print(f"To be deleted:       {len(to_be_deleted):,} users ({len(to_be_deleted)/(len(to_be_deleted)+len(not_to_delete))*100:.1f}%)")
    print(f"Not to delete:       {len(not_to_delete):,} users ({len(not_to_delete)/(len(to_be_deleted)+len(not_to_delete))*100:.1f}%)")
    print(f"Time elapsed:        {elapsed_time/60:.1f} minutes")
    print(f"Processing rate:     {(len(to_be_deleted) + len(not_to_delete))/elapsed_time:.2f} users/second")
    print(f"Speedup vs sync:     ~{((len(to_be_deleted) + len(not_to_delete))/elapsed_time) / 0.75:.1f}x faster")
    print("=" * 70)
    print("\n✅ Validation complete!")


def main():
    """Main entry point with command line argument handling."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Validate IBM user emails against Bluepages API (Async Version)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test mode - validate first 10 users
  python3 bluepages_validator_async.py --test
  
  # Test mode - validate first 50 users
  python3 bluepages_validator_async.py --test --limit 50
  
  # Full validation with 50 concurrent requests (default)
  python3 bluepages_validator_async.py
  
  # Full validation with 20 concurrent requests (safer)
  python3 bluepages_validator_async.py --concurrent 20
  
  # Resume from checkpoint
  python3 bluepages_validator_async.py --resume
  
  # Custom input file
  python3 bluepages_validator_async.py --input my_users.json
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        default=None,
        help='Input JSON file with user records (default: auto-selects latest resolved_users file)'
    )
    
    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='Test mode - only process a limited number of users'
    )
    
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=10,
        help='Number of users to process in test mode (default: 10)'
    )
    
    parser.add_argument(
        '--resume', '-r',
        action='store_true',
        help='Resume from checkpoint if available'
    )
    
    parser.add_argument(
        '--concurrent', '-c',
        type=int,
        default=50,
        help='Maximum number of concurrent requests (default: 50)'
    )
    
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=100,
        help='Number of users to process before saving checkpoint (default: 100)'
    )
    
    parser.add_argument(
        '--output-delete',
        default='to_be_deleted.json',
        help='Output file for users to be deleted (default: to_be_deleted.json)'
    )
    
    parser.add_argument(
        '--output-keep',
        default='not_to_delete.json',
        help='Output file for users to keep (default: not_to_delete.json)'
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    # Auto-pick latest resolved file if not specified
    if not args.input:
        resolution_dir = 'backend/resolutions'
        files = [
            os.path.join(resolution_dir, f)
            for f in os.listdir(resolution_dir)
            if f.startswith('ibm_only_') and f.endswith('.json')
        ]
        if not files:
            # Fall back to resolved_users if no ibm_only file exists yet
            files = [
                os.path.join(resolution_dir, f)
                for f in os.listdir(resolution_dir)
                if f.startswith('resolved_users_') and f.endswith('.json')
            ]
        if not files:
            print(f"❌ Error: No resolved users file found in {resolution_dir}")
            sys.exit(1)
        args.input = max(files, key=os.path.getctime)
        print(f"Auto-selected latest input file: {args.input}")

    if not os.path.exists(args.input):
        print(f"❌ Error: Input file not found: {args.input}")
        sys.exit(1)
    
    try:
        asyncio.run(validate_users_async(
            input_file=args.input,
            to_delete_file=args.output_delete,
            not_to_delete_file=args.output_keep,
            test_mode=args.test,
            test_limit=args.limit,
            resume=args.resume,
            max_concurrent=args.concurrent,
            batch_size=args.batch_size
        ))
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Progress saved to checkpoint.")
        print("   Run with --resume to continue from where you left off.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

# Made with Bob
