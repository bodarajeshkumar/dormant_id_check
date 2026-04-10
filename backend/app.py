"""
Flask Backend for Cloudant Data Extraction Control System
Provides REST APIs for job management and status tracking
"""

import os
import json
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv
import sys
import subprocess

# Add parent directory to path to import cloudant_extractor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cloudant_extractor_async import CloudantExtractorAsync
from backend import user_filters
from backend import validators
from backend.filters import FilterManager
import asyncio

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Status file path
STATUS_FILE = 'status.json'
HISTORY_FILE = 'extraction_history.json'

# Global lock for thread-safe status updates
status_lock = threading.Lock()

# Global reference to current extractor (for stop functionality)
current_extractor = None
current_extractor_lock = threading.Lock()


class StatusManager:
    """Manages job status persistence and updates"""
    
    @staticmethod
    def load_status():
        """Load status from file"""
        try:
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading status: {e}")
        
        # Return default status
        return {
            'status': 'not_started',
            'current_month': None,
            'records_processed': 0,
            'progress_percent': 0,
            'start_date': None,
            'end_date': None,
            'total_months': 0,
            'completed_months': 0,
            'error': None,
            'last_updated': None
        }
    
    @staticmethod
    def save_status(status_data):
        """Save status to file"""
        with status_lock:
            try:
                status_data['last_updated'] = datetime.now().isoformat()
                with open(STATUS_FILE, 'w') as f:
                    json.dump(status_data, f, indent=2)
            except Exception as e:
                print(f"Error saving status: {e}")
    
    @staticmethod
    def update_status(updates):
        """Update specific fields in status"""
        status = StatusManager.load_status()
        status.update(updates)
        StatusManager.save_status(status)
        return status


class HistoryManager:
    """Manages extraction history persistence"""
    
    @staticmethod
    def load_history():
        """Load history from file"""
        try:
            if os.path.exists(HISTORY_FILE):
                with open(HISTORY_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading history: {e}")
        
        return []
    
    @staticmethod
    def save_history(history_data):
        """Save history to file"""
        with status_lock:
            try:
                with open(HISTORY_FILE, 'w') as f:
                    json.dump(history_data, f, indent=2)
            except Exception as e:
                print(f"Error saving history: {e}")
    
    @staticmethod
    def add_history_entry(entry):
        """Add a new history entry"""
        history = HistoryManager.load_history()
        history.insert(0, entry)  # Add to beginning (most recent first)
        
        # Keep only last 100 entries
        if len(history) > 100:
            history = history[:100]
        
        HistoryManager.save_history(history)
        return history


class ExtractorWrapper:
    """Wrapper for CloudantExtractor with status tracking"""
    
    def __init__(self, start_date, end_date, filter_config=None, batch_size=3000, user_ids=None, extraction_mode='date_range'):
        self.start_date = start_date
        self.end_date = end_date
        self.extractor = None
        self.filter_config = filter_config or {}
        self.filter_manager = None
        self.batch_size = batch_size
        self.stop_requested = False
        self.user_ids = user_ids or []
        self.extraction_mode = extraction_mode
        
    def calculate_total_months(self):
        """Calculate total months in date range"""
        # Parse datetime with or without timestamp
        try:
            start = datetime.strptime(self.start_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                start = datetime.strptime(self.start_date, '%Y-%m-%d %H:%M')
            except ValueError:
                start = datetime.strptime(self.start_date, '%Y-%m-%d')
        
        try:
            end = datetime.strptime(self.end_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                end = datetime.strptime(self.end_date, '%Y-%m-%d %H:%M')
            except ValueError:
                end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        months = (end.year - start.year) * 12 + (end.month - start.month) + 1
        return months
    
    def run(self):
        """Run extraction with status updates (wraps async execution)"""
        # Run async extraction in a new event loop
        asyncio.run(self._run_async())
    
    async def _run_async(self):
        """Async extraction logic"""
        start_time = time.time()
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_filename = f"extraction_{timestamp}.json"
        output_path = os.path.join('backend', 'extractions', self.output_filename)
        
        # Create extractions directory if it doesn't exist
        os.makedirs(os.path.join('backend', 'extractions'), exist_ok=True)
        
        # Initialize output file
        self.output_file = output_path
        self.output_path = output_path  # Store for later use
        self.extracted_data = []
        self.total_records_extracted = 0  # Track total records for specific_ids mode
        
        try:
            if self.extraction_mode == 'specific_ids':
                # Handle specific IDs extraction
                total_ids = len(self.user_ids)
                
                # Update status to under_processing
                StatusManager.update_status({
                    'status': 'under_processing',
                    'extraction_mode': 'specific_ids',
                    'total_ids': total_ids,
                    'records_processed': 0,
                    'progress_percent': 0,
                    'error': None,
                    'start_time': start_time,
                    'output_file': self.output_filename,
                    'filters': self.filter_config
                })
                
                # Initialize filter manager
                self.filter_manager = FilterManager(self.filter_config)
                logger.info(f"Filter configuration: {self.filter_config}")
                logger.info(f"Enabled filters: {self.filter_manager.get_stats()['enabled_filters']}")
                
                # For specific IDs, we'll fetch them directly from Cloudant
                # Store the IDs as extracted data
                for idx, user_id in enumerate(self.user_ids):
                    if self.stop_requested:
                        raise InterruptedError("Extraction stopped by user")
                    
                    # Create a simple record with the user ID
                    record = {'id': user_id, 'email': user_id}
                    self.extracted_data.append(record)
                    self.total_records_extracted += 1
                    
                    # Update progress
                    progress = int(((idx + 1) / total_ids) * 100)
                    StatusManager.update_status({
                        'records_processed': idx + 1,
                        'progress_percent': progress
                    })
                
                # Set extraction successful flag
                self.duration_seconds = int(time.time() - start_time)
                self.extraction_successful = True
                
            else:
                # Handle date range extraction (existing logic)
                # Parse dates with or without timestamp
                try:
                    start = datetime.strptime(self.start_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        start = datetime.strptime(self.start_date, '%Y-%m-%d %H:%M')
                    except ValueError:
                        start = datetime.strptime(self.start_date, '%Y-%m-%d')
                
                try:
                    end = datetime.strptime(self.end_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        end = datetime.strptime(self.end_date, '%Y-%m-%d %H:%M')
                    except ValueError:
                        end = datetime.strptime(self.end_date, '%Y-%m-%d')
                
                # Calculate total months
                total_months = self.calculate_total_months()
                
                # Update status to under_processing
                StatusManager.update_status({
                    'status': 'under_processing',
                    'extraction_mode': 'date_range',
                    'start_date': self.start_date,
                    'end_date': self.end_date,
                    'total_months': total_months,
                    'completed_months': 0,
                    'records_processed': 0,
                    'progress_percent': 0,
                    'current_month': f"{start.year}-{start.month:02d}",
                    'error': None,
                    'start_time': start_time,
                    'output_file': self.output_filename,
                    'filters': self.filter_config
                })
            
                # Initialize filter manager
                self.filter_manager = FilterManager(self.filter_config)
                logger.info(f"Filter configuration: {self.filter_config}")
                logger.info(f"Enabled filters: {self.filter_manager.get_stats()['enabled_filters']}")
                
                # Check if stop was requested before initialization
                if self.stop_requested:
                    logger.warning("Stop requested before extractor initialization")
                    raise InterruptedError("Extraction stopped by user before starting")
                
                # Initialize extractor
                username = os.getenv('CLOUDANT_USERNAME')
                password = os.getenv('CLOUDANT_PASSWORD')
                base_url = os.getenv('CLOUDANT_URL')
                
                if not all([username, password, base_url]):
                    raise Exception("Missing Cloudant credentials in environment variables")
                
                self.extractor = CloudantExtractorWithCallback(
                    base_url=base_url,
                    username=username,
                    password=password,
                    batch_size=self.batch_size,
                    status_callback=self.update_progress,
                    data_storage_callback=self.store_batch_data,
                    total_months=total_months
                )
                
                # Check again after initialization
                if self.stop_requested:
                    logger.warning("Stop requested after extractor initialization")
                    self.extractor.request_stop()
                
                # Create session and run extraction
                await self.extractor.create_session()
                
                # Run extraction with full date/time range
                await self.extractor.extract_date_range(
                    start_year=start.year,
                    start_month=start.month,
                    end_year=end.year,
                    end_month=end.month,
                    start_day=start.day,
                    start_hour=start.hour,
                    start_minute=start.minute,
                    end_day=end.day,
                    end_hour=end.hour,
                    end_minute=end.minute
                )

                print(f"DEBUG: extraction done. records={self.extractor.total_records_processed}, months={self.extractor.months_processed}, batches={self.extractor.total_batches_processed}", flush=True)
                
                # Calculate duration
                end_time = time.time()
                duration_seconds = int(end_time - start_time)
                
                # Store duration for later use
                self.duration_seconds = duration_seconds
                self.extraction_successful = True
            
        except InterruptedError as e:
            # Handle user-requested stop
            end_time = time.time()
            duration_seconds = int(end_time - start_time)
            
            # Mark as stopped
            StatusManager.update_status({
                'status': 'stopped',
                'error': 'Stopped by user',
                'duration_seconds': duration_seconds,
                'filters': self.filter_config
            })
            
            # Add stopped entry to history
            HistoryManager.add_history_entry({
                'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                'start_date': self.start_date,
                'end_date': self.end_date,
                'records_processed': self.extractor.total_records_processed if self.extractor else 0,
                'months_processed': self.extractor.months_processed if self.extractor else 0,
                'status': 'stopped',
                'error': 'Stopped by user',
                'timestamp': datetime.now().isoformat(),
                'duration_seconds': duration_seconds,
                'filename': self.output_filename,
                'filters': self.filter_config
            })
            
            logger.info("Extraction stopped by user")
            
        except Exception as e:
            # Calculate duration even for failed jobs
            end_time = time.time()
            duration_seconds = int(end_time - start_time)
            
            print(f"PIPELINE ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()

            # Mark as finished with error
            StatusManager.update_status({
                'status': 'failed',
                'error': str(e),
                'duration_seconds': duration_seconds,
                'filters': self.filter_config
            })
            
            # Add failed entry to history
            HistoryManager.add_history_entry({
                'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                'start_date': self.start_date,
                'end_date': self.end_date,
                'records_processed': self.extractor.total_records_processed if self.extractor else 0,
                'months_processed': self.extractor.months_processed if self.extractor else 0,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'duration_seconds': duration_seconds,
                'filename': None,
                'filters': self.filter_config
            })
            
            print(f"Extraction error: {e}")
        
        finally:
            # Finalize the output file
            if hasattr(self, 'output_file') and hasattr(self, 'extracted_data'):
                self.finalize_output_file()
            
            # Run validation pipeline AFTER file is finalized
            logger.info(f"Finally block: extraction_successful={hasattr(self, 'extraction_successful')}, value={getattr(self, 'extraction_successful', None)}, stop_requested={self.stop_requested}")
            if hasattr(self, 'extraction_successful') and self.extraction_successful and not self.stop_requested:
                logger.info("Running validation pipeline...")
                try:
                    await self._run_validation_pipeline(self.output_path)
                    logger.info("Validation pipeline completed successfully")
                except Exception as e:
                    logger.error(f"Validation pipeline failed: {e}", exc_info=True)
                    # Continue to mark as finished even if validation fails
            elif self.stop_requested:
                logger.info("Skipping validation pipeline because extraction was stopped")
                
                # Mark as finished (successful completion) - AFTER validation completes
                logger.info("Updating status to finished...")
                StatusManager.update_status({
                    'status': 'finished',
                    'error': None,
                    'duration_seconds': self.duration_seconds,
                    'filters': self.filter_config
                })
                
                # Add to history
                logger.info("Adding to history...")
                HistoryManager.add_history_entry({
                    'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                    'start_date': self.start_date,
                    'end_date': self.end_date,
                    'records_processed': self.extractor.total_records_processed if self.extractor else self.total_records_extracted,
                    'months_processed': self.extractor.months_processed if self.extractor else 0,
                    'status': 'completed',
                    'error': None,
                    'timestamp': datetime.now().isoformat(),
                    'duration_seconds': self.duration_seconds,
                    'filename': self.output_filename,
                    'filters': self.filter_config,
                    'extraction_mode': self.extraction_mode
                })
                logger.info("Status update and history entry complete!")
            
            if self.extractor:
                await self.extractor.close()
            
            # Clear the global current_extractor reference
            global current_extractor
            with current_extractor_lock:
                if current_extractor == self:
                    current_extractor = None
                    logger.info("Cleared current_extractor reference")
    
    def store_batch_data(self, batch):
        """Store batch data to file incrementally with date and plugin filtering"""
        try:
            # Filter records to ensure they're within the requested date range
            # Parse with or without timestamp
            try:
                start = datetime.strptime(self.start_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    start = datetime.strptime(self.start_date, '%Y-%m-%d %H:%M')
                except ValueError:
                    start = datetime.strptime(self.start_date, '%Y-%m-%d')
            
            try:
                end = datetime.strptime(self.end_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    end = datetime.strptime(self.end_date, '%Y-%m-%d %H:%M')
                except ValueError:
                    end = datetime.strptime(self.end_date, '%Y-%m-%d')
            
            filtered_batch = []
            date_filtered_out = 0
            plugin_filtered_out = 0
            
            for record in batch:
                key = record.get('key', [])
                if len(key) >= 7:
                    # Extract datetime from key: [boolean, year, month, day, hour, minute, second]
                    try:
                        record_datetime = datetime(key[1], key[2], key[3], key[4], key[5], key[6])
                        # Only include if within datetime range
                        if start <= record_datetime <= end:
                            filtered_batch.append(record)
                        else:
                            date_filtered_out += 1
                            logger.debug(f"Filtered out record with datetime {record_datetime} (outside range {start} to {end})")
                    except (ValueError, IndexError) as e:
                        # Skip invalid dates
                        logger.warning(f"Invalid date in record key {key}: {e}")
                        continue
            
            if date_filtered_out > 0:
                logger.info(f"Date filter: {date_filtered_out} records outside date range from batch of {len(batch)}")
            
            if plugin_filtered_out > 0:
                logger.info(f"Plugin filters: {plugin_filtered_out} records filtered from batch")
            
            # Append filtered batch to extracted data
            self.extracted_data.extend(filtered_batch)
            
            # Write to file every 10,000 records to avoid memory issues
            if len(self.extracted_data) >= 10000:
                self.flush_to_file()
        except Exception as e:
            logger.error(f"Error storing batch data: {e}")
    
    def flush_to_file(self):
        """Flush accumulated data to file"""
        if not self.extracted_data:
            return
        
        try:
            # Check if file exists to determine if we need to add opening bracket
            file_exists = os.path.exists(self.output_file)
            
            with open(self.output_file, 'a') as f:
                if not file_exists:
                    # Start JSON array
                    f.write('[\n')
                
                # Write records - one per line, compact format
                for i, record in enumerate(self.extracted_data):
                    # Compact JSON (no indentation)
                    json_str = json.dumps(record, separators=(',', ':'))
                    # Add comma if not the first record in file
                    if file_exists or i > 0:
                        f.write(',\n')
                    f.write(json_str)
            
            # Clear the buffer
            self.extracted_data = []
            
        except Exception as e:
            logger.error(f"Error flushing data to file: {e}")
    
    def finalize_output_file(self):
        """Finalize the output file by closing JSON array"""
        try:
            # Flush any remaining data
            self.flush_to_file()
            
            # Close JSON array
            with open(self.output_file, 'a') as f:
                f.write('\n]')
            
            logger.info(f"Data saved to: {self.output_file}")
            
        except Exception as e:
            logger.error(f"Error finalizing output file: {e}")
    
    def update_progress(self, year, month, records_processed, completed_months, total_months):
        """Callback to update progress"""
        progress_percent = int((completed_months / total_months) * 100) if total_months > 0 else 0
        
        StatusManager.update_status({
            'current_month': f"{year}-{month:02d}",
            'records_processed': records_processed,
            'completed_months': completed_months,
            'progress_percent': progress_percent
        })

    async def _run_resolution(self, extraction_file: str) -> str:
        """Run IBM user resolution on extraction output, return resolved file path"""
        try:
            from ibm_users_resolution_async import IBMUsersResolverAsync

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = os.path.join('backend', 'resolutions')
            os.makedirs(output_dir, exist_ok=True)
            resolved_file = os.path.join(output_dir, f'resolved_users_{timestamp}.json')
            failed_file = os.path.join(output_dir, f'failed_ids_{timestamp}.json')

            resolver = IBMUsersResolverAsync(
                batch_size=int(os.getenv('RESOLUTION_BATCH_SIZE', '100')),
                max_concurrent=int(os.getenv('MAX_CONCURRENT', '1'))
            )

            user_ids = resolver.extract_user_ids(extraction_file)
            if not user_ids:
                logger.warning("No user IDs found in extraction file, skipping resolution")
                return None

            results = await resolver.resolve_all(user_ids, resume=False)
            resolver.save_results(results, resolved_file)
            resolver.save_failed_ids(user_ids, set(results.keys()), failed_file)
            resolver.print_statistics()

            logger.info(f"Resolution complete: {resolved_file}")
            return resolved_file

        except Exception as e:
            logger.error(f"Resolution pipeline error: {e}", exc_info=True)
            return None

    def _filter_ibm_emails(self, resolved_file: str) -> str:
        """Filter resolved users to only @ibm.com emails, return filtered file path"""
        try:
            with open(resolved_file, 'r') as f:
                users = json.load(f)

            ibm_users = [u for u in users if u.get('email', '').endswith('@ibm.com')]
            filtered_out = len(users) - len(ibm_users)

            logger.info(f"Email filter: {len(ibm_users)} @ibm.com kept, {filtered_out} non-IBM removed")

            # Save filtered file alongside resolved file
            filtered_file = resolved_file.replace('resolved_users_', 'ibm_only_')
            with open(filtered_file, 'w') as f:
                json.dump(ibm_users, f, indent=2)

            logger.info(f"Filtered file saved: {filtered_file}")
            return filtered_file

        except Exception as e:
            logger.error(f"Email filter error: {e}", exc_info=True)
            return None

    async def _run_bluepages(self, filtered_file: str):
        """Run Bluepages validation on filtered @ibm.com users"""
        try:
            from bluepages_validator_async import validate_users_async

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = os.path.join('backend', 'resolutions')
            to_delete_file = os.path.join(output_dir, f'to_be_deleted_{timestamp}.json')
            not_to_delete_file = os.path.join(output_dir, f'not_to_delete_{timestamp}.json')

            await validate_users_async(
                input_file=filtered_file,
                to_delete_file=to_delete_file,
                not_to_delete_file=not_to_delete_file,
                resume=False,
                max_concurrent=int(os.getenv('BLUEPAGES_CONCURRENT', '50')),
                batch_size=int(os.getenv('BLUEPAGES_BATCH_SIZE', '100'))
            )

            logger.info(f"Bluepages validation complete. To delete: {to_delete_file}")

        except Exception as e:
            logger.error(f"Bluepages pipeline error: {e}", exc_info=True)
    async def _run_validation_pipeline(self, extraction_file: str):
        """
        Run validation pipeline based on filter configuration from UI checkboxes.
        Maps UI filter names to validation checks.
        """
        try:
            # Map filter IDs to validation checks
            # UI sends filters like: {"isv_validation": true, "dormancy_check": true, ...}
            checks = {}
            
            if self.filter_config.get('isv_validation'):
                checks['isv_validation'] = True
                checks['active_status'] = True  # Always check active status after ISV
            
            if self.filter_config.get('dormancy_check'):
                checks['last_login'] = True
                checks['bluepages'] = True  # Always run BluPages after login check
            
            # If no checks selected, skip validation
            if not checks:
                logger.info("No validation checks selected, skipping validation pipeline")
                return
            
            # Update status
            StatusManager.update_status({'status': 'validating', 'error': None})
            logger.info(f"Starting validation pipeline with checks: {list(checks.keys())}")
            
            # Run the validation pipeline
            output_dir = os.path.join('backend', 'resolutions')
            os.makedirs(output_dir, exist_ok=True)
            
            result = await validators.run_validation_pipeline(
                input_file=extraction_file,
                output_dir=output_dir,
                checks=checks,
                days_threshold=1095,  # 3 years
                max_concurrent=int(os.getenv('MAX_CONCURRENT', '50')),
                batch_size=int(os.getenv('RESOLUTION_BATCH_SIZE', '100'))
            )
            
            if result.get('success'):
                logger.info(f"Validation pipeline completed successfully")
                logger.info(f"Summary: {result.get('summary', {})}")
            else:
                logger.error(f"Validation pipeline failed: {result.get('error')}")
                
        except Exception as e:
            logger.error(f"Validation pipeline error: {e}", exc_info=True)



class CloudantExtractorWithCallback(CloudantExtractorAsync):
    """Extended CloudantExtractorAsync with progress callbacks and data storage"""
    
    def __init__(self, *args, status_callback=None, data_storage_callback=None, total_months=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.status_callback = status_callback
        self.data_storage_callback = data_storage_callback
        self.total_months_expected = total_months
    
    async def extract_year(self, year, start_month=1, end_month=12,
                           start_day=1, start_hour=0, start_minute=0,
                           end_day=None, end_hour=23, end_minute=59):
        """Override to add progress tracking (async)"""
        logger.info(f"=" * 80)
        logger.info(f"Starting extraction for year {year}")
        logger.info(f"Months: {start_month} to {end_month}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"=" * 80)
        
        start_time = time.time()
        
        for month in range(start_month, end_month + 1):
            month_start_time = time.time()
            
            try:
                # Process month data in batches (async)
                async for batch in self._extract_month_data(
                    year, month,
                    start_day=start_day if month == start_month else 1,
                    start_hour=start_hour if month == start_month else 0,
                    start_minute=start_minute if month == start_month else 0,
                    end_day=end_day if month == end_month else None,
                    end_hour=end_hour if month == end_month else 23,
                    end_minute=end_minute if month == end_month else 59
                ):
                    # Store data if callback provided
                    if self.data_storage_callback:
                        self.data_storage_callback(batch)
                    
                    # Process the batch immediately (streaming approach)
                    processed = self.process_batch(batch)
                    
                    self.total_batches_processed += 1
                    self.total_records_processed += processed
                
                self.months_processed += 1
                
                # Callback for progress update
                if self.status_callback:
                    self.status_callback(
                        year=year,
                        month=month,
                        records_processed=self.total_records_processed,
                        completed_months=self.months_processed,
                        total_months=self.total_months_expected
                    )
                
                month_duration = time.time() - month_start_time
                logger.info(
                    f"Month {year}-{month:02d} completed in "
                    f"{month_duration:.2f} seconds"
                )
                
            except InterruptedError:
                # Re-raise to stop the extraction
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


# Import required modules for the extended class
import time
import logging
logger = logging.getLogger(__name__)


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current job status"""
    status = StatusManager.load_status()
    return jsonify(status)



@app.route('/api/filters', methods=['GET'])
def get_filters():
    """Get list of available filters"""
    try:
        # Create a temporary filter manager to get available filters
        temp_manager = FilterManager()
        filters = temp_manager.get_available_filters()
        
        return jsonify({
            'success': True,
            'filters': filters
        })
        
    except Exception as e:
        logger.error(f"Error getting filters: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/retrieve', methods=['POST'])
def start_retrieval():
    """Start data retrieval job"""
    global current_extractor
    
    try:
        # Check current status
        current_status = StatusManager.load_status()
        
        if current_status['status'] == 'under_processing':
            return jsonify({
                'success': False,
                'error': 'A job is already running. Please wait for it to complete.'
            }), 400
        
        # Get request data
        data = request.get_json()
        extraction_mode = data.get('extraction_mode', 'date_range')
        filters = data.get('filters', {})  # Get filter configuration
        batch_size = data.get('batch_size', 3000)  # Get batch size, default 3000
        
        # Validate batch size
        try:
            batch_size = int(batch_size)
            if batch_size < 100 or batch_size > 10000:
                return jsonify({
                    'success': False,
                    'error': 'Batch size must be between 100 and 10000'
                }), 400
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid batch size'
            }), 400
        
        # Handle different extraction modes
        if extraction_mode == 'date_range':
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            
            # Validate input
            if not start_date or not end_date:
                return jsonify({
                    'success': False,
                    'error': 'start_date and end_date are required for date range extraction'
                }), 400
            
            # Validate date format (supports YYYY-MM-DD, YYYY-MM-DD HH:MM, and YYYY-MM-DD HH:MM:SS)
            try:
                # Try parsing with full timestamp first
                try:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        # Try HH:MM format (append :00 for seconds)
                        start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M')
                        end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M')
                        # Update the date strings to include seconds for consistency
                        start_date = start_dt.strftime('%Y-%m-%d %H:%M:%S')
                        end_date = end_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # Fall back to date-only format
                        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid date format. Use YYYY-MM-DD, YYYY-MM-DD HH:MM, or YYYY-MM-DD HH:MM:SS'
                }), 400
            
            # Create extractor wrapper with filter configuration and batch size
            wrapper = ExtractorWrapper(start_date, end_date, filter_config=filters, batch_size=batch_size)
            
            # Store reference to wrapper for stop functionality
            with current_extractor_lock:
                current_extractor = wrapper
            
            # Start extraction in background thread
            thread = threading.Thread(target=wrapper.run, daemon=True)
            thread.start()
            
            return jsonify({
                'success': True,
                'message': 'Data retrieval started successfully',
                'extraction_mode': 'date_range',
                'start_date': start_date,
                'end_date': end_date
            })
            
        elif extraction_mode == 'specific_ids':
            user_ids = data.get('user_ids', [])
            
            # Validate input
            if not user_ids or not isinstance(user_ids, list) or len(user_ids) == 0:
                return jsonify({
                    'success': False,
                    'error': 'user_ids array is required and must contain at least one ID'
                }), 400
            
            # Create extractor wrapper for specific IDs
            wrapper = ExtractorWrapper(
                start_date=None,
                end_date=None,
                filter_config=filters,
                batch_size=batch_size,
                user_ids=user_ids,
                extraction_mode='specific_ids'
            )
            
            # Store reference to wrapper for stop functionality
            with current_extractor_lock:
                current_extractor = wrapper
            
            # Start extraction in background thread
            thread = threading.Thread(target=wrapper.run, daemon=True)
            thread.start()
            
            return jsonify({
                'success': True,
                'message': 'Data retrieval started successfully',
                'extraction_mode': 'specific_ids',
                'user_count': len(user_ids)
            })
        
        else:
            return jsonify({
                'success': False,
                'error': f'Invalid extraction_mode: {extraction_mode}. Must be "date_range" or "specific_ids"'
            }), 400
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/reset', methods=['POST'])
def reset_status():
    """Reset job status to not_started"""
    try:
        current_status = StatusManager.load_status()
        
        if current_status['status'] == 'under_processing':
            return jsonify({
                'success': False,
                'error': 'Cannot reset while a job is running'
            }), 400
        
        # Reset status
        StatusManager.save_status({
            'status': 'not_started',
            'current_month': None,
            'records_processed': 0,
            'progress_percent': 0,
            'start_date': None,
            'end_date': None,
            'total_months': 0,
            'completed_months': 0,
            'error': None,
            'last_updated': datetime.now().isoformat()
        })
        
        return jsonify({
            'success': True,
            'message': 'Status reset successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/stop', methods=['POST'])
def stop_extraction():
    """Stop the currently running extraction"""
    try:
        global current_extractor
        
        # Check if there's actually a job running by checking status
        current_status = StatusManager.load_status()
        if current_status.get('status') != 'under_processing':
            return jsonify({
                'success': False,
                'error': 'No extraction is currently running (status check)'
            }), 400
        
        with current_extractor_lock:
            if current_extractor is None:
                # Status says running but no extractor reference
                # This can happen after server reload - update status to stopped
                logger.warning("Extraction running but no extractor reference (likely after server reload)")
                StatusManager.update_status({
                    'status': 'stopped',
                    'error': 'Stopped (server reloaded during extraction)'
                })
                return jsonify({
                    'success': True,
                    'message': 'Extraction marked as stopped (server was reloaded)'
                })
            
            if current_extractor.extractor is None:
                # Extractor not initialized yet, but we can still set the stop flag
                logger.warning("Extractor not initialized yet, setting stop flag on wrapper")
                current_extractor.stop_requested = True
                return jsonify({
                    'success': True,
                    'message': 'Stop requested. Extraction will stop before starting.'
                })
            
            # Request stop on the extractor
            current_extractor.extractor.request_stop()
        
        return jsonify({
            'success': True,
            'message': 'Stop requested. Extraction will stop after current batch.'
        })
        
    except Exception as e:
        logger.error(f"Error stopping extraction: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def _get_extraction_file_path(filename):
    """Helper function to get extraction file path with validation"""
    # Security: Only allow JSON files
    if not filename.endswith('.json'):
        return None, 'Invalid file type'
    
    # Check extraction directory
    file_path = os.path.join('backend', 'extractions', filename)
    
    if not os.path.exists(file_path):
        return None, 'File not found'
    
    return file_path, None


@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """Download extraction file"""
    try:
        file_path, error = _get_extraction_file_path(filename)
        
        if error or not file_path:
            return jsonify({
                'success': False,
                'error': error or 'File not found'
            }), 404 if error == 'File not found' else 400
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/json'
        )
        
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/view/<filename>', methods=['GET'])
def view_file(filename):
    """View extraction file with pagination"""
    try:
        file_path, error = _get_extraction_file_path(filename)
        
        if error or not file_path:
            return jsonify({
                'success': False,
                'error': error or 'File not found'
            }), 404 if error == 'File not found' else 400
        
        # Get pagination parameters
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 100, type=int)
        
        # Validate pagination parameters
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 1000:
            page_size = 100
        
        # Read and parse JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Calculate pagination
        total_records = len(data)
        total_pages = (total_records + page_size - 1) // page_size
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        # Get page data
        page_data = data[start_idx:end_idx]
        
        return jsonify({
            'success': True,
            'data': page_data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_records': total_records,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        })
        
    except Exception as e:
        logger.error(f"Error viewing file: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/extractions', methods=['GET'])
def list_extractions():
    """List all available extraction files"""
    try:
        extractions = []
        extraction_dir = os.path.join('backend', 'extractions')
        
        if os.path.exists(extraction_dir):
            for filename in os.listdir(extraction_dir):
                if filename.endswith('.json') and filename.startswith('extraction_'):
                    file_path = os.path.join(extraction_dir, filename)
                    file_stats = os.stat(file_path)
                    
                    extractions.append({
                        'filename': filename,
                        'size': file_stats.st_size,
                        'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                        'created': datetime.fromtimestamp(file_stats.st_ctime).isoformat(),
                        'modified': datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                    })
        
        # Sort by creation time (newest first)
        extractions.sort(key=lambda x: x['created'], reverse=True)
        
        return jsonify({
            'success': True,
            'extractions': extractions,
            'count': len(extractions)
        })
        
    except Exception as e:
        logger.error(f"Error listing extractions: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/', methods=['GET'])
def root():
    """Root endpoint - API information"""
    return jsonify({
        'name': 'Cloudant Extractor API',
        'version': '1.0.0',
        'status': 'running',
        'endpoints': {
            'status': '/api/status',
            'retrieve': '/api/retrieve',
            'reset': '/api/reset',
            'history': '/api/history',
            'health': '/api/health'
        },
        'documentation': 'See SETUP.md for API documentation'
    })


@app.route('/api/history', methods=['GET'])
def get_history():
    """Get extraction history"""
    history = HistoryManager.load_history()
    return jsonify({
        'success': True,
        'history': history,
        'count': len(history)
    })


@app.route('/api/history/<history_id>', methods=['DELETE'])
def delete_history_entry(history_id):
    """
    Delete a history entry and all its associated files.
    
    Deletes files from:
    - backend/backend/extractions/ (extraction file)
    - backend/backend/resolutions/ (all files with matching timestamp)
    - backend/backend/outputs/ (all files with matching timestamp)
    """
    try:
        # Load history
        history = HistoryManager.load_history()
        
        # Find the entry
        entry = None
        entry_index = None
        for i, h in enumerate(history):
            if h.get('id') == history_id:
                entry = h
                entry_index = i
                break
        
        if not entry:
            return jsonify({
                'success': False,
                'error': f'History entry {history_id} not found'
            }), 404
        
        deleted_files = []
        failed_files = []
        
        # Extract timestamp from filename (format: extraction_YYYYMMDD_HHMMSS.json -> YYYYMMDD_HHMMSS)
        extraction_filename = entry.get('filename', '')
        if extraction_filename:
            # e.g. extraction_20260407_162814.json -> 20260407_162814
            full_timestamp = extraction_filename.replace('extraction_', '').replace('.json', '')
        else:
            full_timestamp = history_id  # fallback to history_id
        
        # Use a partial timestamp match (YYYYMMDD_HHMM) to catch files created within the same minute
        # This handles cases where validation pipeline creates files a few seconds after extraction
        timestamp_prefix = full_timestamp[:13] if len(full_timestamp) >= 13 else full_timestamp  # YYYYMMDD_HHMM
        
        print(f"=== DELETE OPERATION START ===")
        print(f"History ID: {history_id}")
        print(f"Full timestamp: {full_timestamp}")
        print(f"Timestamp prefix for matching: {timestamp_prefix}")
        print(f"Entry: {entry}")
        logger.info(f"=== DELETE OPERATION START ===")
        logger.info(f"History ID: {history_id}")
        logger.info(f"Full timestamp: {full_timestamp}")
        logger.info(f"Timestamp prefix for matching: {timestamp_prefix}")
        logger.info(f"Entry: {entry}")
        
        # Define all directories to check (relative to backend directory where Flask runs)
        directories_to_check = [
            ('backend/extractions', 'extractions'),
            ('backend/resolutions', 'resolutions'),
            ('backend/outputs', 'outputs')
        ]
        
        # Delete files from all directories
        for dir_path, dir_name in directories_to_check:
            if os.path.exists(dir_path):
                print(f"Checking {dir_name} directory: {dir_path}")
                logger.info(f"Checking {dir_name} directory: {dir_path}")
                try:
                    files_in_dir = os.listdir(dir_path)
                    print(f"Files in {dir_name}: {files_in_dir}")
                    for filename in files_in_dir:
                        # Match files containing the timestamp prefix (matches files within same minute)
                        if timestamp_prefix in filename:
                            file_path = os.path.join(dir_path, filename)
                            print(f"Found matching file in {dir_name}: {filename}")
                            logger.info(f"Found matching file in {dir_name}: {filename}")
                            try:
                                if os.path.isfile(file_path):
                                    print(f"Attempting to delete: {file_path}")
                                    os.remove(file_path)
                                    deleted_files.append(file_path)
                                    print(f"✓ Deleted: {file_path}")
                                    logger.info(f"✓ Deleted: {file_path}")
                            except Exception as e:
                                print(f"✗ Failed to delete {file_path}: {e}")
                                logger.error(f"✗ Failed to delete {file_path}: {e}")
                                failed_files.append({'file': file_path, 'error': str(e)})
                except Exception as e:
                    print(f"Error scanning {dir_name} directory: {e}")
                    logger.error(f"Error scanning {dir_name} directory: {e}")
                    failed_files.append({'directory': dir_path, 'error': str(e)})
            else:
                print(f"{dir_name} directory does not exist: {dir_path}")
                logger.warning(f"{dir_name} directory does not exist: {dir_path}")
        
        print(f"=== DELETE OPERATION COMPLETE ===")
        print(f"Total files deleted: {len(deleted_files)}")
        print(f"Total files failed: {len(failed_files)}")
        print(f"Deleted files: {deleted_files}")
        print(f"Failed files: {failed_files}")
        logger.info(f"=== DELETE OPERATION COMPLETE ===")
        logger.info(f"Total files deleted: {len(deleted_files)}")
        logger.info(f"Total files failed: {len(failed_files)}")
        
        # Remove entry from history
        if entry_index is not None:
            history.pop(entry_index)
            HistoryManager.save_history(history)
        
        return jsonify({
            'success': True,
            'message': f'History entry {history_id} deleted',
            'deleted_files': deleted_files,
            'deleted_count': len(deleted_files),
            'failed_files': failed_files,
            'failed_count': len(failed_files)
        })
        
    except Exception as e:
        logger.error(f"Error in delete_history_entry: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================================================
# User Filtering API Endpoints
# ============================================================================

@app.route('/api/users/split-by-status', methods=['POST'])
def split_users_by_status():
    """
    Split users into active and inactive files.
    
    Request body:
    {
        "input_file": "backend/resolutions/resolved_users_20260403_155345.json",
        "output_dir": "backend/resolutions"  // optional
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        output_dir = data.get('output_dir', 'backend/resolutions')
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable function
        active_file, inactive_file, active_count, inactive_count = user_filters.split_by_active_status(
            input_file, output_dir
        )
        
        return jsonify({
            'success': True,
            'files': {
                'active': active_file,
                'inactive': inactive_file
            },
            'counts': {
                'active': active_count,
                'inactive': inactive_count
            }
        })
        
    except user_filters.UserFilterError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/users/filter-by-login', methods=['POST'])
def filter_users_by_login():
    """
    Filter users by last login date.
    
    Request body:
    {
        "input_file": "backend/resolutions/isv_active_users_20260403_160408.json",
        "days_threshold": 1095,  // optional, default 1095 (3 years)
        "output_dir": "backend/resolutions",  // optional
        "append_recent": true  // optional, default true
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        days_threshold = data.get('days_threshold', 1095)
        output_dir = data.get('output_dir', 'backend/resolutions')
        append_recent = data.get('append_recent', True)
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable function
        old_file, recent_file, old_count, recent_count = user_filters.filter_by_login_date(
            input_file, days_threshold, output_dir, append_recent=append_recent
        )
        
        return jsonify({
            'success': True,
            'files': {
                'old_login': old_file,
                'recent_login': recent_file
            },
            'counts': {
                'old_login': old_count,
                'recent_login': recent_count
            },
            'threshold_days': days_threshold
        })
        
    except user_filters.UserFilterError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/users/process-pipeline', methods=['POST'])
def process_user_pipeline():
    """
    Run complete user processing pipeline:
    1. Split by active/inactive
    2. Filter active users by login date
    
    Request body:
    {
        "input_file": "backend/resolutions/resolved_users_20260403_155345.json",
        "days_threshold": 1095,  // optional, default 1095 (3 years)
        "output_dir": "backend/resolutions"  // optional
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        days_threshold = data.get('days_threshold', 1095)
        output_dir = data.get('output_dir', 'backend/resolutions')
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable pipeline function
        result = user_filters.process_user_pipeline(
            input_file, output_dir, days_threshold
        )
        
        return jsonify({
            'success': True,
            **result
        })
        
    except user_filters.UserFilterError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/users/statistics', methods=['POST'])
def get_user_statistics():
    """
    Get statistics for a user file.
    
    Request body:
    {
        "file_path": "backend/resolutions/resolved_users_20260403_155345.json"
    }
    """
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        
        if not file_path:
            return jsonify({'error': 'file_path is required'}), 400
        
        # Load users and get statistics
        users = user_filters.load_users_from_file(file_path)
        stats = user_filters.get_user_statistics(users)
        
        return jsonify({
            'success': True,
            'file': file_path,
            'statistics': stats
        })
        
    except user_filters.UserFilterError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/users/list-files', methods=['GET'])
def list_user_files():
    """
    List all resolution files with metadata.
    
    Query params:
    - resolution_dir: optional, default "backend/resolutions"
    """
    try:
        resolution_dir = request.args.get('resolution_dir', 'backend/resolutions')
        
        files = user_filters.list_resolution_files(resolution_dir)
        
        return jsonify({
            'success': True,
            'files': files,
            'count': len(files)
        })
        
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


# ============================================================================
# Validation Pipeline API Endpoints
# ============================================================================

@app.route('/api/validate/isv', methods=['POST'])
async def validate_isv_endpoint():
    """
    Validate users against ISV (IBM Users API).
    
    Request body:
    {
        "input_file": "backend/backend/extractions/extraction_*.json",
        "output_dir": "backend/resolutions",
        "batch_size": 100,
        "max_concurrent": 50
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        output_dir = data.get('output_dir', 'backend/resolutions')
        batch_size = data.get('batch_size', 100)
        max_concurrent = data.get('max_concurrent', 50)
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable validator
        result = await validators.validate_isv(
            input_file=input_file,
            output_dir=output_dir,
            batch_size=batch_size,
            max_concurrent=max_concurrent
        )
        
        return jsonify(result)
        
    except validators.ISVValidationError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/validate/active-status', methods=['POST'])
def validate_active_status_endpoint():
    """
    Split users by active/inactive status.
    
    Request body:
    {
        "input_file": "backend/resolutions/isv_resolved_users_*.json",
        "output_dir": "backend/resolutions",
        "timestamp": "20260406_100000"
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        output_dir = data.get('output_dir', 'backend/resolutions')
        timestamp = data.get('timestamp')
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable validator
        result = validators.validate_active_status(
            input_file=input_file,
            output_dir=output_dir,
            timestamp=timestamp
        )
        
        return jsonify(result)
        
    except validators.ActiveStatusError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/validate/last-login', methods=['POST'])
def validate_last_login_endpoint():
    """
    Filter users by last login date.
    
    Request body:
    {
        "input_file": "backend/resolutions/isv_active_users_*.json",
        "days_threshold": 1095,
        "output_dir": "backend/resolutions",
        "timestamp": "20260406_100000",
        "append_recent": true
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        days_threshold = data.get('days_threshold', 1095)
        output_dir = data.get('output_dir', 'backend/resolutions')
        timestamp = data.get('timestamp')
        append_recent = data.get('append_recent', True)
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable validator
        result = validators.validate_last_login(
            input_file=input_file,
            days_threshold=days_threshold,
            output_dir=output_dir,
            timestamp=timestamp,
            append_recent=append_recent
        )
        
        return jsonify(result)
        
    except validators.LoginValidationError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/validate/bluepages', methods=['POST'])
async def validate_bluepages_endpoint():
    """
    Validate users against IBM BluPages.
    
    Request body:
    {
        "input_file": "backend/resolutions/isv_last_login_>3_*.json",
        "output_dir": "backend/resolutions",
        "timestamp": "20260406_100000",
        "max_concurrent": 50,
        "batch_size": 100
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        output_dir = data.get('output_dir', 'backend/resolutions')
        timestamp = data.get('timestamp')
        max_concurrent = data.get('max_concurrent', 50)
        batch_size = data.get('batch_size', 100)
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable validator
        result = await validators.validate_bluepages(
            input_file=input_file,
            output_dir=output_dir,
            timestamp=timestamp,
            max_concurrent=max_concurrent,
            batch_size=batch_size
        )
        
        return jsonify(result)
        
    except validators.BluePagesError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@app.route('/api/validate/pipeline', methods=['POST'])
async def validate_pipeline_endpoint():
    """
    Run complete validation pipeline with selected checks.
    
    Request body:
    {
        "input_file": "backend/backend/extractions/extraction_*.json",
        "output_dir": "backend/resolutions",
        "checks": {
            "isv_validation": true,
            "active_status": true,
            "last_login": true,
            "bluepages": true
        },
        "days_threshold": 1095,
        "max_concurrent": 50,
        "batch_size": 100
    }
    """
    try:
        data = request.get_json()
        input_file = data.get('input_file')
        output_dir = data.get('output_dir', 'backend/resolutions')
        checks = data.get('checks')
        days_threshold = data.get('days_threshold', 1095)
        max_concurrent = data.get('max_concurrent', 50)
        batch_size = data.get('batch_size', 100)
        
        if not input_file:
            return jsonify({'error': 'input_file is required'}), 400
        
        # Call the pluggable pipeline
        result = await validators.run_validation_pipeline(
            input_file=input_file,
            output_dir=output_dir,
            checks=checks,
            days_threshold=days_threshold,
            max_concurrent=max_concurrent,
            batch_size=batch_size
        )
        
        return jsonify(result)
        
    except validators.PipelineError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500




@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })


if __name__ == '__main__':
    # Initialize status file if it doesn't exist
    if not os.path.exists(STATUS_FILE):
        StatusManager.save_status({
            'status': 'not_started',
            'current_month': None,
            'records_processed': 0,
            'progress_percent': 0,
            'start_date': None,
            'end_date': None,
            'total_months': 0,
            'completed_months': 0,
            'error': None,
            'last_updated': None
        })
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)

# Made with Bob
