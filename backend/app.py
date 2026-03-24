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

# Add parent directory to path to import cloudant_extractor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cloudant_extractor import CloudantExtractor

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
    
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.extractor = None
        
    def calculate_total_months(self):
        """Calculate total months in date range"""
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        months = (end.year - start.year) * 12 + (end.month - start.month) + 1
        return months
    
    def run(self):
        """Run extraction with status updates"""
        start_time = time.time()
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"extraction_{timestamp}.json"
        output_path = os.path.join('backend', 'extractions', output_filename)
        
        # Create extractions directory if it doesn't exist
        os.makedirs(os.path.join('backend', 'extractions'), exist_ok=True)
        
        # Initialize output file
        self.output_file = output_path
        self.extracted_data = []
        
        try:
            # Parse dates
            start = datetime.strptime(self.start_date, '%Y-%m-%d')
            end = datetime.strptime(self.end_date, '%Y-%m-%d')
            
            # Calculate total months
            total_months = self.calculate_total_months()
            
            # Update status to under_processing
            StatusManager.update_status({
                'status': 'under_processing',
                'start_date': self.start_date,
                'end_date': self.end_date,
                'total_months': total_months,
                'completed_months': 0,
                'records_processed': 0,
                'progress_percent': 0,
                'current_month': f"{start.year}-{start.month:02d}",
                'error': None,
                'start_time': start_time,
                'output_file': output_filename
            })
            
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
                batch_size=1000,
                status_callback=self.update_progress,
                data_storage_callback=self.store_batch_data,
                total_months=total_months
            )
            
            # Run extraction
            self.extractor.extract_date_range(
                start_year=start.year,
                start_month=start.month,
                end_year=end.year,
                end_month=end.month
            )
            
            # Calculate duration
            end_time = time.time()
            duration_seconds = int(end_time - start_time)
            
            # Mark as finished
            StatusManager.update_status({
                'status': 'finished',
                'progress_percent': 100,
                'error': None,
                'duration_seconds': duration_seconds
            })
            
            # Add to history
            HistoryManager.add_history_entry({
                'id': datetime.now().strftime('%Y%m%d_%H%M%S'),
                'start_date': self.start_date,
                'end_date': self.end_date,
                'records_processed': self.extractor.total_records_processed if self.extractor else 0,
                'months_processed': self.extractor.months_processed if self.extractor else 0,
                'status': 'completed',
                'error': None,
                'timestamp': datetime.now().isoformat(),
                'duration_seconds': duration_seconds,
                'filename': output_filename
            })
            
        except InterruptedError as e:
            # Handle user-requested stop
            end_time = time.time()
            duration_seconds = int(end_time - start_time)
            
            # Mark as stopped
            StatusManager.update_status({
                'status': 'stopped',
                'error': 'Stopped by user',
                'duration_seconds': duration_seconds
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
                'filename': output_filename if hasattr(self, 'output_file') else None
            })
            
            print(f"Extraction stopped by user")
            
        except Exception as e:
            # Calculate duration even for failed jobs
            end_time = time.time()
            duration_seconds = int(end_time - start_time)
            
            # Mark as finished with error
            StatusManager.update_status({
                'status': 'finished',
                'error': str(e),
                'duration_seconds': duration_seconds
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
                'filename': None
            })
            
            print(f"Extraction error: {e}")
        
        finally:
            # Finalize the output file
            if hasattr(self, 'output_file') and hasattr(self, 'extracted_data'):
                self.finalize_output_file()
            
            if self.extractor:
                self.extractor.close()
    
    def store_batch_data(self, batch):
        """Store batch data to file incrementally with date filtering"""
        try:
            # Filter records to ensure they're within the requested date range
            start = datetime.strptime(self.start_date, '%Y-%m-%d')
            end = datetime.strptime(self.end_date, '%Y-%m-%d')
            
            filtered_batch = []
            filtered_out = 0
            
            for record in batch:
                key = record.get('key', [])
                if len(key) >= 7:
                    # Extract date from key: [boolean, year, month, day, hour, minute, second]
                    try:
                        record_date = datetime(key[1], key[2], key[3])
                        # Only include if within date range
                        if start <= record_date <= end:
                            filtered_batch.append(record)
                        else:
                            filtered_out += 1
                            logger.debug(f"Filtered out record with date {record_date.date()} (outside range {start.date()} to {end.date()})")
                    except (ValueError, IndexError) as e:
                        # Skip invalid dates
                        logger.warning(f"Invalid date in record key {key}: {e}")
                        continue
            
            if filtered_out > 0:
                logger.info(f"Filtered out {filtered_out} records outside date range from batch of {len(batch)}")
            
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


class CloudantExtractorWithCallback(CloudantExtractor):
    """Extended CloudantExtractor with progress callbacks and data storage"""
    
    def __init__(self, *args, status_callback=None, data_storage_callback=None, total_months=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.status_callback = status_callback
        self.data_storage_callback = data_storage_callback
        self.total_months_expected = total_months
    
    def extract_year(self, year, start_month=1, end_month=12):
        """Override to add progress tracking"""
        logger.info(f"=" * 80)
        logger.info(f"Starting extraction for year {year}")
        logger.info(f"Months: {start_month} to {end_month}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"=" * 80)
        
        start_time = time.time()
        
        for month in range(start_month, end_month + 1):
            month_start_time = time.time()
            
            try:
                # Process month data in batches
                for batch in self._extract_month_data(year, month):
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


@app.route('/api/retrieve', methods=['POST'])
def start_retrieval():
    """Start data retrieval job"""
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
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Validate input
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start_date and end_date are required'
            }), 400
        
        # Validate date format
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Invalid date format. Use YYYY-MM-DD'
            }), 400
        
        # Create extractor wrapper
        wrapper = ExtractorWrapper(start_date, end_date)
        
        # Store reference to wrapper for stop functionality
        global current_extractor
        with current_extractor_lock:
            current_extractor = wrapper
        
        # Start extraction in background thread
        thread = threading.Thread(target=wrapper.run, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Data retrieval started successfully',
            'start_date': start_date,
            'end_date': end_date
        })
        
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
        
        with current_extractor_lock:
            if current_extractor is None:
                return jsonify({
                    'success': False,
                    'error': 'No extraction is currently running'
                }), 400
            
            if current_extractor.extractor is None:
                return jsonify({
                    'success': False,
                    'error': 'Extractor not initialized yet'
                }), 400
            
            # Request stop
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


@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """Download extraction file"""
    try:
        # Security: Only allow downloading from extractions directory
        # and only JSON files
        if not filename.endswith('.json'):
            return jsonify({
                'success': False,
                'error': 'Invalid file type'
            }), 400
        
        # Check both possible extraction directories
        file_paths = [
            os.path.join('backend', 'extractions', filename),
            os.path.join('backend', 'backend', 'extractions', filename)
        ]
        
        file_path = None
        for path in file_paths:
            if os.path.exists(path):
                file_path = path
                break
        
        if not file_path:
            return jsonify({
                'success': False,
                'error': 'File not found'
            }), 404
        
        # Send file for download
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


@app.route('/api/extractions', methods=['GET'])
def list_extractions():
    """List all available extraction files"""
    try:
        extractions = []
        
        # Check both possible extraction directories
        extraction_dirs = [
            os.path.join('backend', 'extractions'),
            os.path.join('backend', 'backend', 'extractions')
        ]
        
        for extraction_dir in extraction_dirs:
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
