"""
Async Production-ready Cloudant Data Extraction Pipeline
Implements monthly partitioning with key-based pagination using aiohttp for 21M+ records
"""

import os
import sys
import time
import logging
import calendar
import json
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple, AsyncGenerator
from urllib.parse import urlencode
import aiohttp
from aiohttp import BasicAuth, ClientTimeout, TCPConnector
from dotenv import load_dotenv


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cloudant_extraction.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CloudantExtractorAsync:
    """
    Async scalable Cloudant data extractor with monthly partitioning and key-based pagination.
    
    Key Design Decisions:
    1. Monthly Partitioning: Reduces query scope, enables parallel processing
    2. Key-based Pagination: Avoids skip parameter (O(n) complexity)
    3. startkey_docid: Ensures no duplicates when keys are identical
    4. Streaming: Processes batches immediately, low memory footprint
    5. Async I/O: Non-blocking HTTP requests for better throughput
    """
    
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        batch_size: int = 3000,
        max_retries: int = 3,
        retry_delay: int = 5,
        timeout: int = 30
    ):
        """
        Initialize the async Cloudant extractor.
        
        Args:
            base_url: Cloudant database URL (without query params)
            username: Basic auth username
            password: Basic auth password
            batch_size: Number of records per batch (default: 3000)
            max_retries: Maximum retry attempts for failed requests
            retry_delay: Delay between retries in seconds
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.auth = BasicAuth(username, password)
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = ClientTimeout(total=timeout)
        
        # Session will be created in async context
        self.session = None
        
        # Statistics
        self.total_records_processed = 0
        self.total_batches_processed = 0
        self.months_processed = 0
        
        # Stop flag for graceful shutdown
        self.stop_requested = False
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.create_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
        
    async def create_session(self):
        """Create an aiohttp session with connection pooling"""
        connector = TCPConnector(
            limit=50,  # Max connections
            limit_per_host=50,
            ttl_dns_cache=300
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            auth=self.auth
        )
        
    def request_stop(self):
        """Request graceful stop of the extraction process"""
        logger.warning("Stop requested - extraction will stop after current batch")
        self.stop_requested = True
    
    def _get_last_day_of_month(self, year: int, month: int) -> int:
        """Get the last day of a given month"""
        return calendar.monthrange(year, month)[1]
    
    def _build_query_params(
        self,
        startkey: List,
        endkey: List,
        startkey_docid: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict:
        """Build query parameters for Cloudant view request"""
        params = {
            'startkey': json.dumps(startkey),
            'endkey': json.dumps(endkey),
            'reduce': 'false',
            'include_docs': 'false',
            'inclusive_end': 'true'
        }
        
        if startkey_docid:
            params['startkey_docid'] = startkey_docid
        
        if limit:
            params['limit'] = str(limit)
            
        return params
    
    async def _fetch_batch(
        self,
        startkey: List,
        endkey: List,
        startkey_docid: Optional[str] = None
    ) -> Tuple[List[Dict], bool]:
        """
        Fetch a single batch of records with retry logic (async).
        
        Returns:
            Tuple of (rows, has_more) where has_more indicates if more data exists
        """
        # Request batch_size records (Cloudant has a hard limit of ~3000)
        limit = self.batch_size
        
        params = self._build_query_params(
            startkey=startkey,
            endkey=endkey,
            startkey_docid=startkey_docid,
            limit=limit
        )
        
        logger.debug(f"Fetching batch with params: startkey={startkey}, endkey={endkey}, startkey_docid={startkey_docid}, limit={limit}")
        
        for attempt in range(self.max_retries):
            try:
                async with self.session.get(self.base_url, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    rows = data.get('rows', [])
                    
                    logger.debug(f"Received {len(rows)} rows from API")
                    
                    # Check if there are more records BEFORE skipping duplicates
                    # If we got exactly batch_size rows from API, there might be more
                    has_more = len(rows) == self.batch_size
                    
                    # When using startkey_docid, skip the first document to avoid duplicates
                    if startkey_docid and rows and rows[0].get('id') == startkey_docid:
                        logger.debug(f"Skipping duplicate document: {startkey_docid}")
                        rows = rows[1:]
                    
                    logger.debug(f"After processing: {len(rows)} rows, has_more={has_more}")
                    
                    logger.info(f"Batch fetch complete: {len(rows)} rows returned, has_more={has_more}")
                    return rows, has_more
                    
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Max retries exceeded. Last error: {e}")
                    raise
        
        return [], False
    
    def process_batch(self, rows: List[Dict]) -> int:
        """
        Process a batch of records (synchronous processing).
        
        Override this method to implement your business logic.
        """
        processed_count = 0
        
        for row in rows:
            try:
                key = row.get('key', [])
                value = row.get('value', {})
                doc_id = row.get('id', '')
                
                if len(key) >= 7:
                    is_active, year, month, day, hour, minute, second = key[:7]
                    timestamp = datetime(year, month, day, hour, minute, second)
                    processed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing row {row.get('id', 'unknown')}: {e}")
                continue
        
        return processed_count
    
    async def _extract_month_data(
        self,
        year: int,
        month: int,
        start_day: int = 1,
        start_hour: int = 0,
        start_minute: int = 0,
        end_day: int = None,
        end_hour: int = 23,
        end_minute: int = 59
    ) -> AsyncGenerator[List[Dict], None]:
        """
        Extract data for a specific date/time range within a month using key-based pagination (async).
        
        Yields:
            Batches of rows for the specified date/time range
        """
        if end_day is None:
            end_day = self._get_last_day_of_month(year, month)
        
        startkey = [True, year, month, start_day, start_hour, start_minute, 0]
        endkey = [True, year, month, end_day, end_hour, end_minute, 59]
        
        logger.info(f"Starting extraction for {year}-{month:02d}")
        logger.info(f"  Start key: {startkey}")
        logger.info(f"  End key: {endkey}")
        
        current_startkey = startkey
        current_startkey_docid = None
        month_batch_count = 0
        month_record_count = 0
        
        while True:
            if self.stop_requested:
                logger.warning(f"Stop requested during extraction of {year}-{month:02d}")
                raise InterruptedError("Extraction stopped by user")
            
            rows, has_more = await self._fetch_batch(
                startkey=current_startkey,
                endkey=endkey,
                startkey_docid=current_startkey_docid
            )
            
            if not rows:
                logger.info(f"No more data for {year}-{month:02d}")
                break
            
            month_batch_count += 1
            month_record_count += len(rows)
            
            logger.info(
                f"  Batch {month_batch_count}: Fetched {len(rows)} records "
                f"(Total: {month_record_count})"
            )
            
            yield rows
            
            if not has_more:
                logger.info(f"Reached end of data for {year}-{month:02d}")
                break
            
            last_row = rows[-1]
            current_startkey = last_row['key']
            current_startkey_docid = last_row['id']
            
            logger.debug(
                f"  Next pagination: key={current_startkey}, "
                f"docid={current_startkey_docid}"
            )
        
        logger.info(
            f"Completed {year}-{month:02d}: "
            f"{month_batch_count} batches, {month_record_count} records"
        )
    
    async def extract_year(self, year: int, start_month: int = 1, end_month: int = 12,
                          start_day: int = 1, start_hour: int = 0, start_minute: int = 0,
                          end_day: int = None, end_hour: int = 23, end_minute: int = 59):
        """Extract data for a date/time range within a year (async)"""
        logger.info(f"=" * 80)
        logger.info(f"Starting extraction for year {year}")
        logger.info(f"Months: {start_month} to {end_month}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"=" * 80)
        
        start_time = time.time()
        
        for month in range(start_month, end_month + 1):
            if self.stop_requested:
                logger.warning(f"Stop requested before processing {year}-{month:02d}")
                raise InterruptedError("Extraction stopped by user")
            
            month_start_time = time.time()
            
            # Determine day/time range for this specific month
            # For the first month in range, use the specified start day/time
            if month == start_month:
                month_start_day = start_day
                month_start_hour = start_hour
                month_start_minute = start_minute
            else:
                # For subsequent months, start from beginning
                month_start_day = 1
                month_start_hour = 0
                month_start_minute = 0
            
            # For the last month in range, use the specified end day/time
            if month == end_month:
                month_end_day = end_day
                month_end_hour = end_hour
                month_end_minute = end_minute
            else:
                # For earlier months, go to end of month
                month_end_day = None  # Will be set to last day of month
                month_end_hour = 23
                month_end_minute = 59
            
            logger.info(f"Processing {year}-{month:02d}: day {month_start_day} {month_start_hour}:{month_start_minute} to day {month_end_day or 'last'} {month_end_hour}:{month_end_minute}")
            
            try:
                async for batch in self._extract_month_data(
                    year, month,
                    start_day=month_start_day,
                    start_hour=month_start_hour,
                    start_minute=month_start_minute,
                    end_day=month_end_day,
                    end_hour=month_end_hour,
                    end_minute=month_end_minute
                ):
                    processed = self.process_batch(batch)
                    
                    self.total_batches_processed += 1
                    self.total_records_processed += processed
                
                self.months_processed += 1
                
                month_duration = time.time() - month_start_time
                logger.info(
                    f"Month {year}-{month:02d} completed in "
                    f"{month_duration:.2f} seconds"
                )
                
            except InterruptedError:
                raise
            except Exception as e:
                logger.error(f"Failed to process {year}-{month:02d}: {e}")
                continue
        
        total_duration = time.time() - start_time
        
        logger.info(f"=" * 80)
        logger.info(f"Extraction completed for year {year}")
        logger.info(f"Total months processed: {self.months_processed}")
        logger.info(f"Total batches processed: {self.total_batches_processed}")
        logger.info(f"Total records processed: {self.total_records_processed}")
        logger.info(f"Total duration: {total_duration:.2f} seconds")
        if total_duration > 0:
            logger.info(f"Average records/second: {self.total_records_processed / total_duration:.2f}")
        logger.info(f"=" * 80)
    
    async def extract_date_range(
        self,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        start_day: int = 1,
        start_hour: int = 0,
        start_minute: int = 0,
        end_day: int = None,
        end_hour: int = 23,
        end_minute: int = 59
    ):
        """Extract data for a specific date/time range (async)"""
        logger.info(f"Extracting data from {start_year}-{start_month:02d}-{start_day:02d} {start_hour:02d}:{start_minute:02d} "
                   f"to {end_year}-{end_month:02d}-{end_day or 'last'}  {end_hour:02d}:{end_minute:02d}")
        
        for year in range(start_year, end_year + 1):
            first_month = start_month if year == start_year else 1
            last_month = end_month if year == end_year else 12

            await self.extract_year(
                year, first_month, last_month,
                start_day=start_day if year == start_year else 1,
                start_hour=start_hour if year == start_year else 0,
                start_minute=start_minute if year == start_year else 0,
                end_day=end_day if year == end_year else None,
                end_hour=end_hour if year == end_year else 23,
                end_minute=end_minute if year == end_year else 59
            )
    async def close(self):
        """Close the session and cleanup resources"""
        if self.session:
            await self.session.close()
            logger.info("Extractor session closed")


async def main():
    """Main entry point for the async extraction pipeline"""
    load_dotenv()
    
    username = os.getenv('CLOUDANT_USERNAME')
    password = os.getenv('CLOUDANT_PASSWORD')
    base_url = os.getenv('CLOUDANT_URL')
    
    if not username or not password or not base_url:
        logger.error("Missing required environment variables")
        sys.exit(1)
    
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '3000'))
    START_YEAR = int(os.getenv('START_YEAR', '2024'))
    START_MONTH = int(os.getenv('START_MONTH', '1'))
    END_YEAR = int(os.getenv('END_YEAR', '2026'))
    END_MONTH = int(os.getenv('END_MONTH', '12'))
    
    logger.info("Configuration:")
    logger.info(f"  Base URL: {base_url}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  Date Range: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    
    async with CloudantExtractorAsync(
        base_url=base_url,
        username=username,
        password=password,
        batch_size=BATCH_SIZE,
        max_retries=3,
        retry_delay=5
    ) as extractor:
        try:
            await extractor.extract_date_range(
                start_year=START_YEAR,
                start_month=START_MONTH,
                end_year=END_YEAR,
                end_month=END_MONTH
            )
        except KeyboardInterrupt:
            logger.info("Extraction interrupted by user")
        except Exception as e:
            logger.error(f"Extraction failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())

# Made with Bob