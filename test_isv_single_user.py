#!/usr/bin/env python3
"""
Test script to verify ISV API with a single user ID
"""

import asyncio
import sys
from ibm_users_resolver_async import IBMUsersResolverAsync

async def test_single_user(user_id):
    """Test resolving a single user ID"""
    print(f"\n{'='*60}")
    print(f"Testing ISV API with user ID: {user_id}")
    print(f"{'='*60}\n")
    
    resolver = IBMUsersResolverAsync(
        batch_size=1,
        max_concurrent=1
    )
    
    try:
        # Try to resolve the user
        results = await resolver.resolve_all([user_id])
        
        if user_id in results:
            print(f"✅ SUCCESS: User found in ISV")
            print(f"   User data: {results[user_id]}")
        else:
            print(f"❌ FAILED: User NOT found in ISV")
            print(f"   This user ID does not exist in the IBM Users API")
        
        # Print statistics
        print(f"\nStatistics:")
        print(f"  API calls made: {resolver.stats['api_calls']}")
        print(f"  Resolved: {resolver.stats['resolved_ids']}")
        print(f"  Failed: {resolver.stats['failed_ids']}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python test_isv_single_user.py <user_id>")
        print("\nExample:")
        print("  python test_isv_single_user.py 55000A9AQN")
        print("  python test_isv_single_user.py your.email@ibm.com")
        sys.exit(1)
    
    user_id = sys.argv[1]
    asyncio.run(test_single_user(user_id))

# Made with Bob
