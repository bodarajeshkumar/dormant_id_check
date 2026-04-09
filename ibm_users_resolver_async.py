#!/usr/bin/env python3
"""
IBM Users API Resolver (Async Version)
High-performance async resolution of user IDs to email addresses using IBM Users API.
Uses concurrent requests for significantly faster processing.
"""

import json
import os
import logging
import asyncio
import aiohttp
from datetime import datetime
from typing import List, Dict, Set
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ibm_users_resolution_async.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IBMUsersResolverAsync:
    """Async resolver for SCIM user IDs to email addresses using IBM Users API"""
    
    def __init__(
        self,
        api_url: str = None,
        token_url: str = None,
        client_id: str = None,
        client_secret: str = None,
        access_token: str = None,
        batch_size: int = 100,
        max_concurrent: int = 10, # 1-50 is recommended for high performance
        max_retries: int = 3,
        retry_delay: int = 2
    ):
        """
        Initialize the async IBM Users API resolver
        
        Args:
            api_url: IBM Users API base URL (default from env)
            token_url: IBM Access Token API URL (default from env)
            client_id: OAuth client ID for token request (default from env)
            client_secret: OAuth client secret for token request (default from env)
            access_token: Pre-obtained access token (optional, will fetch if not provided)
            batch_size: Number of IDs to process per checkpoint batch (default: 100)
            max_concurrent: Maximum number of concurrent requests (default: 50)
            max_retries: Maximum retry attempts for failed requests
            retry_delay: Delay between retries in seconds
        """
        self.api_url = api_url or os.getenv('IBM_USERS_API_URL', 'https://login.ibm.com/v2.0/Users')
        self.token_url = token_url or os.getenv('IBM_TOKEN_API_URL')
        self.client_id = client_id or os.getenv('IBM_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('IBM_CLIENT_SECRET')
        self.access_token = access_token
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Initialize statistics
        self.stats = {
            'total_ids': 0,
            'resolved_ids': 0,
            'failed_ids': 0,
            'batches_processed': 0,
            'api_calls': 0,
            'start_time': None,
            'end_time': None
        }
    
    async def get_access_token(self) -> str:
        """
        Fetch access token from the token API endpoint (async)
        
        Returns:
            Access token string
        """
        try:
            logger.info(f"Requesting access token from: {self.token_url}")
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'client_credentials'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.token_url,
                    headers=headers,
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        token = data.get('access_token')
                        if token:
                            logger.info("Access token obtained successfully")
                            return token
                        else:
                            logger.error("No access_token field in response")
                            logger.error(f"Response: {data}")
                            return None
                    else:
                        text = await response.text()
                        logger.error(f"Failed to get access token. Status: {response.status}")
                        logger.error(f"Response: {text}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error fetching access token: {e}")
            return None
    
    def extract_user_ids(self, extraction_file: str) -> List[str]:
        """
        Extract unique user IDs from extraction.json file
        
        Args:
            extraction_file: Path to extraction JSON file
            
        Returns:
            List of unique user IDs
        """
        logger.info(f"Reading extraction file: {extraction_file}")
        
        try:
            user_ids = set()
            
            with open(extraction_file, 'r') as f:
                try:
                    data = json.load(f)
                    for record in data:
                        user_id = record.get('value')
                        if user_id:
                            user_ids.add(user_id)
                    
                    user_ids_list = sorted(list(user_ids))
                    logger.info(f"Extracted {len(user_ids_list)} unique user IDs")
                    return user_ids_list
                    
                except json.JSONDecodeError as json_err:
                    logger.warning(f"JSON parsing failed: {json_err}")
                    logger.warning("Attempting line-by-line parsing...")
                    
                    f.seek(0)
                    
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        
                        if not line or line == '[' or line == ']':
                            continue
                        
                        if line.endswith(','):
                            line = line[:-1]
                        
                        try:
                            record = json.loads(line)
                            user_id = record.get('value')
                            if user_id:
                                user_ids.add(user_id)
                        except json.JSONDecodeError:
                            logger.debug(f"Skipping malformed line {line_num}")
                            continue
            
            user_ids_list = sorted(list(user_ids))
            logger.info(f"Extracted {len(user_ids_list)} unique user IDs (line-by-line parsing)")
            
            return user_ids_list
            
        except FileNotFoundError:
            logger.error(f"Extraction file not found: {extraction_file}")
            raise
        except Exception as e:
            logger.error(f"Error reading extraction file: {e}")
            raise
    
    async def resolve_single_id(self, user_id: str, session: aiohttp.ClientSession,
                                semaphore: asyncio.Semaphore) -> Dict[str, Dict]:
        """
        Resolve a single user ID to user details (email, lastLogin, active status) (async)
        
        Args:
            user_id: User ID to resolve
            session: Aiohttp session for connection pooling
            semaphore: Semaphore to limit concurrent requests
            
        Returns:
            Dictionary with user_id -> user_details mapping (empty if failed)
            user_details contains: email, lastLogin, active
        """
        params = {
            'filter': f'id eq "{user_id}"'
            # Don't specify attributes - get all fields to access IBM extension
        }
        
        async with semaphore:
            for attempt in range(self.max_retries):
                try:
                    async with session.get(
                        self.api_url,
                        params=params
                    ) as response:
                        self.stats['api_calls'] += 1
                        
                        if response.status == 200:
                            content_type = response.headers.get('Content-Type', '')
                            
                            # Check if response is HTML (service unavailable page)
                            if 'text/html' in content_type:
                                # Exponential backoff for HTML errors (rate limiting)
                                wait_time = self.retry_delay * (2 ** attempt)
                                logger.warning(f"Received HTML error page for {user_id} (attempt {attempt + 1}/{self.max_retries})")
                                if attempt < self.max_retries - 1:
                                    logger.info(f"Retrying after {wait_time:.1f}s with exponential backoff...")
                                    await asyncio.sleep(wait_time)
                                    continue
                                else:
                                    logger.error(f"Service unavailable for {user_id} after {self.max_retries} retries")
                                    return {}
                            
                            # Try to parse JSON response
                            try:
                                data = await response.json()
                            except (json.JSONDecodeError, aiohttp.ContentTypeError) as e:
                                # Response is not valid JSON (likely HTML error page)
                                wait_time = self.retry_delay * (2 ** attempt)
                                logger.warning(f"Invalid JSON response for {user_id}: {str(e)[:100]}")
                                if attempt < self.max_retries - 1:
                                    logger.info(f"Retrying after {wait_time:.1f}s with exponential backoff...")
                                    await asyncio.sleep(wait_time)
                                    continue
                                else:
                                    logger.error(f"Failed to parse response for {user_id} after {self.max_retries} retries")
                                    return {}
                            
                            # Normal JSON processing
                            resources = data.get('Resources', [])
                            
                            if resources and len(resources) > 0:
                                resource = resources[0]
                                email = resource.get('userName')
                                active = resource.get('active', True)
                                
                                # Extract lastLogin from IBM extension schema
                                ibm_extension = resource.get('urn:ietf:params:scim:schemas:extension:ibm:2.0:User', {})
                                last_login = ibm_extension.get('lastLogin')
                                
                                if email:
                                    return {
                                        user_id: {
                                            'email': email,
                                            'lastLogin': last_login,
                                            'active': active
                                        }
                                    }
                            
                            # Return empty for not found, but don't retry
                            return {}
                        
                        elif response.status == 401:
                            logger.error("Authentication failed. Check access token")
                            return {}
                        
                        elif response.status == 429:
                            # Rate limit - exponential backoff
                            wait_time = self.retry_delay * (2 ** attempt)
                            logger.warning(f"Rate limited on {user_id} (429). Waiting {wait_time:.1f}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        elif response.status >= 500:
                            # Server error - retry with exponential backoff
                            if attempt < self.max_retries - 1:
                                wait_time = self.retry_delay * (2 ** attempt)
                                logger.warning(f"Server error {response.status} for {user_id}. Retrying after {wait_time:.1f}s...")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                logger.warning(f"Server error for {user_id} after {self.max_retries} attempts")
                                return {}
                        
                        else:
                            # Other errors - log and return empty
                            logger.debug(f"Status {response.status} for {user_id}")
                            return {}
                
                except asyncio.TimeoutError:
                    if attempt < self.max_retries - 1:
                        logger.debug(f"Timeout for {user_id}, attempt {attempt + 1}/{self.max_retries}")
                        await asyncio.sleep(self.retry_delay)
                        continue
                    else:
                        logger.warning(f"Timeout for {user_id} after {self.max_retries} attempts")
                        return {}
                
                except aiohttp.ClientError as e:
                    if attempt < self.max_retries - 1:
                        logger.debug(f"Client error for {user_id}: {e}, attempt {attempt + 1}/{self.max_retries}")
                        await asyncio.sleep(self.retry_delay)
                        continue
                    else:
                        logger.warning(f"Client error for {user_id} after {self.max_retries} attempts: {e}")
                        return {}
                
                except Exception as e:
                    logger.error(f"Unexpected error resolving {user_id}: {e}")
                    return {}
            
            return {}
    
    async def resolve_batch(self, user_ids: List[str], session: aiohttp.ClientSession,
                           semaphore: asyncio.Semaphore) -> Dict[str, Dict]:
        """
        Resolve a batch of user IDs concurrently
        
        Args:
            user_ids: List of user IDs to resolve
            session: Aiohttp session
            semaphore: Semaphore to limit concurrency
            
        Returns:
            Dictionary mapping user_id -> user_details (email, lastLogin, active)
        """
        tasks = [self.resolve_single_id(user_id, session, semaphore) for user_id in user_ids]
        results = await asyncio.gather(*tasks)
        
        # Merge all results
        merged = {}
        for result in results:
            if result:
                merged.update(result)
        
        return merged
    
    def save_checkpoint(self, results: Dict[str, Dict], processed_count: int,
                       checkpoint_file: str = "resolution_checkpoint_async.json"):
        """Save progress checkpoint"""
        checkpoint_data = {
            "processed_count": processed_count,
            "timestamp": datetime.now().isoformat(),
            "results": results
        }
        
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def load_checkpoint(self, checkpoint_file: str = "resolution_checkpoint_async.json") -> tuple:
        """Load checkpoint if exists"""
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    data = json.load(f)
                return data['results'], data['processed_count']
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
        
        return {}, 0
    
    async def resolve_all(self, user_ids: List[str], resume: bool = False) -> Dict[str, Dict]:
        """
        Resolve all user IDs to user details (email, lastLogin, active) using async/concurrent requests
        
        Args:
            user_ids: List of all user IDs to resolve
            resume: If True, resume from checkpoint
            
        Returns:
            Dictionary mapping user_id -> user_details (email, lastLogin, active)
        """
        self.stats['total_ids'] = len(user_ids)
        self.stats['start_time'] = datetime.now()
        
        # Load checkpoint if resuming
        all_results = {}
        start_index = 0
        
        if resume:
            logger.info("Checking for checkpoint...")
            all_results, start_index = self.load_checkpoint()
            if start_index > 0:
                logger.info(f"Resuming from record {start_index}")
                logger.info(f"Already resolved: {len(all_results)} users")
                user_ids = user_ids[start_index:]
        
        # Get access token if not provided
        if not self.access_token:
            if self.token_url:
                logger.info("Fetching access token...")
                self.access_token = await self.get_access_token()
            else:
                logger.error("No access token or token URL provided")
                return {}
        
        if not self.access_token:
            logger.error("Failed to obtain access token")
            return {}
        
        logger.info(f"Starting async resolution of {len(user_ids)} user IDs")
        logger.info(f"Concurrency: {self.max_concurrent} simultaneous requests")
        logger.info(f"Checkpoint batch size: {self.batch_size}")
        
        # Create semaphore and session
        semaphore = asyncio.Semaphore(self.max_concurrent)
        headers = {
            'Content-Type': 'application/scim+json',
            'Accept': 'application/scim+json',
            'Authorization': f'Bearer {self.access_token}'
        }
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
            # Process in checkpoint batches
            total_batches = (len(user_ids) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(user_ids), self.batch_size):
                batch = user_ids[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} IDs)")
                
                batch_start = datetime.now()
                batch_results = await self.resolve_batch(batch, session, semaphore)
                batch_time = (datetime.now() - batch_start).total_seconds()
                
                all_results.update(batch_results)
                
                self.stats['batches_processed'] += 1
                self.stats['resolved_ids'] = len(all_results)
                self.stats['failed_ids'] = self.stats['total_ids'] - self.stats['resolved_ids']
                
                # Calculate stats
                elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
                rate = len(all_results) / elapsed if elapsed > 0 else 0
                remaining = (self.stats['total_ids'] - len(all_results)) / rate if rate > 0 else 0
                
                logger.info(f"Batch completed in {batch_time:.1f}s | "
                          f"Resolved: {len(batch_results)}/{len(batch)} | "
                          f"Total: {len(all_results)}/{self.stats['total_ids']} | "
                          f"Rate: {rate:.1f}/s | "
                          f"ETA: {remaining/60:.1f}m")
                
                # Save checkpoint
                self.save_checkpoint(all_results, start_index + i + len(batch))
                
                # Add 1 second delay between batches to avoid overwhelming the API
                if batch_num < total_batches:
                    logger.debug(f"Waiting 1s before next batch to avoid rate limiting...")
                    await asyncio.sleep(1)
        
        self.stats['end_time'] = datetime.now()
        
        return all_results
    
    def save_results(self, results: Dict[str, Dict], output_file: str):
        """
        Save resolution results to JSON file
        
        Args:
            results: Dictionary mapping user_id -> user_details (email, lastLogin, active)
            output_file: Path to output JSON file
        """
        logger.info(f"Saving results to: {output_file}")
        
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        
        output_data = [
            {
                'user_id': user_id,
                'email': user_details.get('email'),
                'lastLogin': user_details.get('lastLogin'),
                'active': user_details.get('active', True)
            }
            for user_id, user_details in sorted(results.items())
        ]
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        logger.info(f"Saved {len(output_data)} resolved users to {output_file}")
    
    def save_failed_ids(self, all_ids: List[str], resolved_ids: Set[str], output_file: str):
        """
        Save list of IDs that failed to resolve
        
        Args:
            all_ids: List of all user IDs
            resolved_ids: Set of successfully resolved IDs
            output_file: Path to output file
        """
        failed_ids = [uid for uid in all_ids if uid not in resolved_ids]
        
        # Always create the file, even if empty
        logger.info(f"Saving {len(failed_ids)} failed IDs to: {output_file}")
        
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(failed_ids, f, indent=2)
        
        if failed_ids:
            logger.warning(f"{len(failed_ids)} IDs could not be resolved")
        else:
            logger.info("All IDs were successfully resolved!")
    
    def print_statistics(self):
        """Print resolution statistics"""
        logger.info("=" * 60)
        logger.info("RESOLUTION STATISTICS (ASYNC)")
        logger.info("=" * 60)
        logger.info(f"Total User IDs:      {self.stats['total_ids']}")
        logger.info(f"Resolved:            {self.stats['resolved_ids']}")
        logger.info(f"Failed:              {self.stats['failed_ids']}")
        
        if self.stats['total_ids'] > 0:
            logger.info(f"Success Rate:        {(self.stats['resolved_ids']/self.stats['total_ids']*100):.1f}%")
        
        logger.info(f"Batches Processed:   {self.stats['batches_processed']}")
        logger.info(f"API Calls Made:      {self.stats['api_calls']}")
        
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            logger.info(f"Duration:            {duration:.1f} seconds ({duration/60:.1f} minutes)")
            if duration > 0:
                rate = self.stats['resolved_ids'] / duration
                logger.info(f"Resolution Rate:     {rate:.1f} IDs/second")
                logger.info(f"Speedup vs sync:     ~{rate / 10:.1f}x faster")
        
        logger.info("=" * 60)


async def main_async():
    """Main async execution function"""
    
    if user_ids_input:
        user_ids = user_ids_input
        logger.info(f"Using {len(user_ids)} IDs passed from command line")
    else:
        # Configuration
        extraction_dir = os.getenv('EXTRACTION_DIR', 'backend/extractions')
        extraction_file = os.getenv('EXTRACTION_FILE', None)

        if not extraction_file:
            files = [
                os.path.join(extraction_dir, f)
                for f in os.listdir(extraction_dir)
                if f.startswith('extraction_') and f.endswith('.json')
            ]
            if not files:
                logger.error(f"No extraction files found in {extraction_dir}")
                return 1
            extraction_file = max(files, key=os.path.getctime)
            logger.info(f"Auto-selected latest extraction file: {extraction_file}")
    
    # Output configuration
    output_dir = os.getenv('OUTPUT_DIR', 'backend/resolutions')
    batch_size = int(os.getenv('RESOLUTION_BATCH_SIZE', '100'))
    max_concurrent = int(os.getenv('MAX_CONCURRENT', '1'))
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f'isv_resolved_users_{timestamp}.json')
    failed_file = os.path.join(output_dir, f'isv_failed_ids_{timestamp}.json')
    
    logger.info("=" * 60)
    logger.info("IBM USERS API RESOLVER (ASYNC)")
    logger.info("=" * 60)
    logger.info(f"Extraction file: {extraction_file}")
    logger.info(f"Output file:     {output_file}")
    logger.info(f"Batch size:      {batch_size}")
    logger.info(f"Max concurrent:  {max_concurrent}")
    logger.info("=" * 60)
    
    try:
        # Initialize resolver
        resolver = IBMUsersResolverAsync(
            batch_size=batch_size,
            max_concurrent=max_concurrent
        )
        
        # Extract user IDs from extraction file
        if not user_ids_input:
            user_ids = resolver.extract_user_ids(extraction_file)
        
        if not user_ids:
            logger.warning("No user IDs found in extraction file")
            return 1
        
        # Resolve user IDs to emails
        results = await resolver.resolve_all(user_ids, resume=False)
        
        # Save results
        resolver.save_results(results, output_file)
        
        # Save failed IDs
        resolver.save_failed_ids(user_ids, set(results.keys()), failed_file)
        
        # Print statistics
        resolver.print_statistics()
        
        # Clean up checkpoint
        checkpoint_file = "resolution_checkpoint_async.json"
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            logger.info("Removed checkpoint file")
        
        logger.info("Resolution completed successfully!")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='IBM Users API Resolver')
    parser.add_argument(
        '--ids', '-i',
        nargs='+',
        help='One or more user IDs to resolve directly. Example: --ids abc123 def456 ghi789'
    )
    args = parser.parse_args()

    try:
        return asyncio.run(main_async(user_ids_input=args.ids))
    except KeyboardInterrupt:
        logger.warning("\n\nInterrupted by user. Progress saved to checkpoint.")
        logger.warning("Run again to resume from checkpoint.")
        return 0

if __name__ == '__main__':
    exit(main())

# Made with Bob
