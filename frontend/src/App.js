import React, { useState, useEffect, useCallback } from 'react';
import {
  Accordion,
  AccordionItem,
  Button,
  DatePicker,
  DatePickerInput,
  ProgressBar,
  InlineNotification,
  InlineLoading,
  Loading,
  Tag,
  Checkbox,
  TimePicker,
  NumberInput,
  Modal,
  DataTable,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  Pagination,
  RadioButtonGroup,
  RadioButton,
  TextArea
} from '@carbon/react';
import { Renew, Download, StopFilled, TrashCan, View } from '@carbon/icons-react';
import axios from 'axios';
import './App.scss';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000/api';

function App() {
  const [extractionMode, setExtractionMode] = useState('date_range');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [startTime, setStartTime] = useState('00:00');
  const [endTime, setEndTime] = useState('23:59');
  const [specificIds, setSpecificIds] = useState('');
  const [batchSize, setBatchSize] = useState(3000);
  const [status, setStatus] = useState(null);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [notification, setNotification] = useState(null);
  const [polling, setPolling] = useState(false);
  const [availableFilters, setAvailableFilters] = useState([]);
  const [selectedFilters, setSelectedFilters] = useState({});
  const [viewModalOpen, setViewModalOpen] = useState(false);
  const [viewData, setViewData] = useState([]);
  const [viewPagination, setViewPagination] = useState(null);
  const [viewFilename, setViewFilename] = useState('');
  const [viewLoading, setViewLoading] = useState(false);
  const [finalDuration, setFinalDuration] = useState(null);

  // Fetch status from API
  const fetchStatus = useCallback(async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/status`);
      console.log('Fetched status:', response.data); // Debug log
      const newStatus = response.data;
      
      // Update status state
      setStatus(prevStatus => {
        // If transitioning from processing/validating to finished/stopped/failed,
        // stop polling immediately and capture final duration
        if ((prevStatus?.status === 'under_processing' || prevStatus?.status === 'validating') &&
            (newStatus.status === 'finished' || newStatus.status === 'stopped' || newStatus.status === 'failed')) {
          console.log('Status transition detected, stopping polling');
          setPolling(false);
          
          // Capture final duration when transitioning to finished
          if (newStatus.status === 'finished') {
            if (newStatus.duration_seconds) {
              setFinalDuration(newStatus.duration_seconds);
            } else if (newStatus.end_time && newStatus.start_time) {
              setFinalDuration(newStatus.end_time - newStatus.start_time);
            } else if (newStatus.start_time) {
              setFinalDuration(Math.floor(Date.now() / 1000 - newStatus.start_time));
            }
          }
        }
        return newStatus;
      });
      
      // Start polling if under processing or validating, stop if finished/stopped/failed
      if (newStatus.status === 'under_processing' || newStatus.status === 'validating') {
        setPolling(true);
      } else if (newStatus.status === 'finished' || newStatus.status === 'stopped' || newStatus.status === 'failed') {
        setPolling(false);
      }
    } catch (error) {
      console.error('Error fetching status:', error);
      setNotification({
        kind: 'error',
        title: 'Error',
        subtitle: 'Failed to fetch status from server'
      });
    }
  }, []); // Remove polling dependency to avoid stale closure

  // Fetch history from API
  const fetchHistory = useCallback(async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/history`);
      if (response.data.success) {
        setHistory(response.data.history);
      }
    } catch (error) {
      console.error('Error fetching history:', error);
    }
  }, []);

  // Handle file download
  const handleDownload = async (filename) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/download/${filename}`, {
        responseType: 'blob'
      });
      
      // Create download link
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      
      setNotification({
        kind: 'success',
        title: 'Download Started',
        subtitle: `Downloading ${filename}`
      });
    } catch (error) {
      setNotification({
        kind: 'error',
        title: 'Download Failed',
        subtitle: error.response?.data?.error || 'Failed to download file'
      });
    }
  };

  // Handle delete history
  const handleDeleteHistory = async (historyId) => {
    if (!window.confirm('Are you sure you want to delete this extraction and all its associated files?')) {
      return;
    }

    try {
      const response = await fetch(`${API_BASE_URL}/history/${historyId}`, {
        method: 'DELETE',
      });

      const data = await response.json();

      if (data.success) {
        setNotification({
          kind: 'success',
          title: 'Deletion Successful',
          subtitle: `Deleted ${data.deleted_count} file(s). ${data.failed_count > 0 ? `Failed to delete ${data.failed_count} file(s).` : ''}`,
        });
        // Refresh history
        fetchHistory();
      } else {
        setNotification({
          kind: 'error',
          title: 'Deletion Failed',
          subtitle: data.error || 'Failed to delete history entry',
        });
      }
    } catch (error) {
      console.error('Error deleting history:', error);
      setNotification({
        kind: 'error',
        title: 'Deletion Error',
        subtitle: error.message,
      });
    }
  };

  // Handle clear all history
  const handleClearAllHistory = async () => {
    if (!window.confirm('Are you sure you want to delete ALL extraction history and associated files? This action cannot be undone.')) {
      return;
    }

    try {
      const response = await fetch(`${API_BASE_URL}/history/clear-all`, {
        method: 'DELETE',
      });

      const data = await response.json();

      if (data.success) {
        setNotification({
          kind: 'success',
          title: 'History Cleared',
          subtitle: data.message || `Cleared ${data.entries_cleared || 0} history entries and ${data.files_deleted || 0} files`,
        });
        // Refresh history
        fetchHistory();
      } else {
        setNotification({
          kind: 'error',
          title: 'Clear Failed',
          subtitle: data.error || 'Failed to clear history',
        });
      }
    } catch (error) {
      console.error('Error clearing history:', error);
      setNotification({
        kind: 'error',
        title: 'Clear Error',
        subtitle: error.message,
      });
    }
  };

  // Fetch available filters
  const fetchFilters = useCallback(async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/filters`);
      if (response.data.success) {
        setAvailableFilters(response.data.filters);
        
        // Set default selected filters (ISV Validation, Dormancy Check, Federated ID removal)
        const defaultFilters = {};
        response.data.filters.forEach(filter => {
          // Check if filter ID matches the default ones
          if (filter.id === 'isv_validation' ||
              filter.id === 'dormancy_check' ||
              filter.id === 'federated_id_removal') {
            defaultFilters[filter.id] = true;
          }
        });
        setSelectedFilters(defaultFilters);
      }
    } catch (error) {
      console.error('Error fetching filters:', error);
    }
  }, []);

  // Handle filter checkbox change
  const handleFilterChange = (filterId, checked) => {
    setSelectedFilters(prev => ({
      ...prev,
      [filterId]: checked
    }));
  };

  // Handle view data
  const handleViewData = async (filename) => {
    setViewFilename(filename);
    setViewModalOpen(true);
    setViewLoading(true);
    
    try {
      const response = await axios.get(`${API_BASE_URL}/view/${filename}`, {
        params: { page: 1, page_size: 100 }
      });
      
      if (response.data.success) {
        setViewData(response.data.data);
        setViewPagination(response.data.pagination);
      } else {
        setNotification({
          kind: 'error',
          title: 'Error',
          subtitle: 'Failed to load data'
        });
        setViewModalOpen(false);
      }
    } catch (error) {
      setNotification({
        kind: 'error',
        title: 'Error',
        subtitle: error.response?.data?.error || 'Failed to load data'
      });
      setViewModalOpen(false);
    } finally {
      setViewLoading(false);
    }
  };

  // Handle page change in data viewer
  const handlePageChange = async ({ page, pageSize }) => {
    setViewLoading(true);
    
    try {
      const response = await axios.get(`${API_BASE_URL}/view/${viewFilename}`, {
        params: { page, page_size: pageSize }
      });
      
      if (response.data.success) {
        setViewData(response.data.data);
        setViewPagination(response.data.pagination);
      }
    } catch (error) {
      setNotification({
        kind: 'error',
        title: 'Error',
        subtitle: 'Failed to load page'
      });
    } finally {
      setViewLoading(false);
    }
  };

  // Initial status, history, and filters fetch
  useEffect(() => {
    fetchStatus();
    fetchHistory();
    fetchFilters();
  }, [fetchStatus, fetchHistory, fetchFilters]);

  // Polling effect
  useEffect(() => {
    let interval;
    if (polling) {
      console.log('Starting polling...'); // Debug log
      interval = setInterval(() => {
        console.log('Polling status...'); // Debug log
        fetchStatus();
        // Also refresh history during polling to catch completed jobs
        fetchHistory();
      }, 3000); // Poll every 3 seconds (faster updates)
    } else {
      console.log('Polling stopped'); // Debug log
      // Fetch history one final time when polling stops
      fetchHistory();
    }
    return () => {
      if (interval) {
        console.log('Clearing polling interval'); // Debug log
        clearInterval(interval);
      }
    };
  }, [polling, fetchStatus, fetchHistory]);

  // Handle form submission
  const handleSubmit = async () => {
    // Check if at least one filter is selected
    const hasSelectedFilter = Object.values(selectedFilters).some(value => value === true);
    if (!hasSelectedFilter) {
      setNotification({
        kind: 'error',
        title: 'Validation Error',
        subtitle: 'Please select at least one filter option before starting extraction'
      });
      return;
    }

    // Validation based on extraction mode
    if (extractionMode === 'date_range') {
      if (!startDate || !endDate) {
        setNotification({
          kind: 'error',
          title: 'Validation Error',
          subtitle: 'Please select both start and end dates'
        });
        return;
      }
    } else if (extractionMode === 'specific_ids') {
      if (!specificIds.trim()) {
        setNotification({
          kind: 'error',
          title: 'Validation Error',
          subtitle: 'Please enter at least one user ID'
        });
        return;
      }
    }

    setLoading(true);
    setNotification(null);

    try {
      const requestData = {
        extraction_mode: extractionMode,
        batch_size: batchSize,
        filters: selectedFilters
      };

      if (extractionMode === 'date_range') {
        requestData.start_date = `${startDate} ${startTime}:00`;
        requestData.end_date = `${endDate} ${endTime}:00`;
      } else if (extractionMode === 'specific_ids') {
        // Parse IDs from textarea (comma, space, or newline separated)
        const ids = specificIds
          .split(/[\n,\s]+/)
          .map(id => id.trim())
          .filter(id => id.length > 0);
        requestData.user_ids = ids;
      }

      const response = await axios.post(`${API_BASE_URL}/retrieve`, requestData);

      if (response.data.success) {
        setNotification({
          kind: 'success',
          title: 'Success',
          subtitle: 'Data retrieval started successfully'
        });
        setFinalDuration(null); // Reset final duration for new extraction
        setPolling(true);
        fetchStatus();
        fetchHistory(); // Refresh history
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || 'Failed to start data retrieval';
      setNotification({
        kind: 'error',
        title: 'Error',
        subtitle: errorMessage
      });
    } finally {
      setLoading(false);
    }
  };

  // Handle stop
  const handleStop = async () => {
    try {
      const response = await axios.post(`${API_BASE_URL}/stop`);
      if (response.data.success) {
        setNotification({
          kind: 'info',
          title: 'Stop Requested',
          subtitle: 'Extraction will stop after current batch'
        });
        setPolling(false); // Stop polling immediately
        fetchStatus();
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || 'Failed to stop extraction';
      setNotification({
        kind: 'error',
        title: 'Error',
        subtitle: errorMessage
      });
    }
  };

  // Handle reset
  const handleReset = async () => {
    try {
      const response = await axios.post(`${API_BASE_URL}/reset`);
      if (response.data.success) {
        // Clear all form inputs
        setExtractionMode('date_range');
        setStartDate('');
        setEndDate('');
        setStartTime('00:00');
        setEndTime('23:59');
        setSpecificIds('');
        setBatchSize(3000);
        setSelectedFilters({});
        setPolling(false);
        
        setNotification({
          kind: 'success',
          title: 'Success',
          subtitle: 'Status reset successfully. All form inputs cleared.'
        });
        fetchStatus();
      }
    } catch (error) {
      const errorMessage = error.response?.data?.error || 'Failed to reset status';
      setNotification({
        kind: 'error',
        title: 'Error',
        subtitle: errorMessage
      });
    }
  };

  // Get status tag
  const getStatusTag = () => {
    if (!status) return null;

    const statusConfig = {
      not_started: { type: 'gray', text: 'Not Started' },
      under_processing: { type: 'blue', text: 'Processing' },
      validating: { type: 'blue', text: 'Validating' },
      stopped: { type: 'purple', text: 'Stopped' },
      finished: {
        type: status.error ? 'red' : 'green',
        text: status.error ? 'Failed' : 'Completed'
      }
    };

    const config = statusConfig[status.status] || statusConfig.not_started;

    return (
      <Tag type={config.type}>
        {config.text}
      </Tag>
    );
  };

  const isProcessing = status?.status === 'under_processing' || status?.status === 'validating';
  const isDisabled = isProcessing || loading;
  const canStop = isProcessing || loading || polling;

  return (
    <div className="app-container">
      <div className="app-header">
        <h1>Cloudant Data Extraction Control Panel</h1>
        <p>Manage and monitor data extraction jobs</p>
      </div>

      {notification && (
        <InlineNotification
          kind={notification.kind}
          title={notification.title}
          subtitle={notification.subtitle}
          onCloseButtonClick={() => setNotification(null)}
          style={{ marginBottom: '1rem', maxWidth: '100%' }}
        />
      )}

      <div className="main-layout">
        <div className="main-content">
          <Accordion>
            <AccordionItem title="Extraction Configuration" open>
              <div className="extraction-mode-section">
                <RadioButtonGroup
                  legendText="Extraction Mode"
                  name="extraction-mode"
                  valueSelected={extractionMode}
                  onChange={setExtractionMode}
                  orientation="horizontal"
                >
                  <RadioButton
                    labelText="Extract using Date Range"
                    value="date_range"
                    id="radio-date-range"
                    disabled={isDisabled}
                  />
                  <RadioButton
                    labelText="Extract using Specific IDs"
                    value="specific_ids"
                    id="radio-specific-ids"
                    disabled={isDisabled}
                  />
                </RadioButtonGroup>
              </div>

              {extractionMode === 'date_range' && (
                <div className="date-picker-container">
                  <div className="datetime-group">
                    <DatePicker
                      datePickerType="single"
                      onChange={(dates) => {
                        if (dates && dates.length > 0) {
                          const date = dates[0];
                          const year = date.getFullYear();
                          const month = String(date.getMonth() + 1).padStart(2, '0');
                          const day = String(date.getDate()).padStart(2, '0');
                          const formatted = `${year}-${month}-${day}`;
                          setStartDate(formatted);
                        }
                      }}
                    >
                      <DatePickerInput
                        id="start-date"
                        placeholder="yyyy-mm-dd"
                        labelText="Start Date"
                        disabled={isDisabled}
                      />
                    </DatePicker>
                    <TimePicker
                      id="start-time"
                      labelText="Start Time"
                      value={startTime}
                      onChange={(e) => setStartTime(e.target.value)}
                      disabled={isDisabled}
                      pattern="^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$"
                      placeholder="HH:MM"
                    />
                  </div>

                  <div className="datetime-group">
                    <DatePicker
                      datePickerType="single"
                      onChange={(dates) => {
                        if (dates && dates.length > 0) {
                          const date = dates[0];
                          const year = date.getFullYear();
                          const month = String(date.getMonth() + 1).padStart(2, '0');
                          const day = String(date.getDate()).padStart(2, '0');
                          const formatted = `${year}-${month}-${day}`;
                          setEndDate(formatted);
                        }
                      }}
                    >
                      <DatePickerInput
                        id="end-date"
                        placeholder="yyyy-mm-dd"
                        labelText="End Date"
                        disabled={isDisabled}
                      />
                    </DatePicker>
                    <TimePicker
                      id="end-time"
                      labelText="End Time"
                      value={endTime}
                      onChange={(e) => setEndTime(e.target.value)}
                      disabled={isDisabled}
                      pattern="^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$"
                      placeholder="HH:MM"
                    />
                  </div>
                </div>
              )}

              {extractionMode === 'specific_ids' && (
                <div className="specific-ids-container">
                  <TextArea
                    id="specific-ids"
                    labelText="User IDs"
                    helperText="Enter user IDs separated by commas, spaces, or new lines"
                    placeholder="user1@example.com, user2@example.com&#10;user3@example.com"
                    value={specificIds}
                    onChange={(e) => setSpecificIds(e.target.value)}
                    disabled={isDisabled}
                    rows={8}
                  />
                </div>
              )}
        </AccordionItem>

        <AccordionItem title="Extraction Settings" open>
          <div className="extraction-settings-batch">
            <NumberInput
              id="batch-size"
              label="Batch Size"
              helperText="Number of records per batch (1000-5000 recommended)"
              min={100}
              max={10000}
              step={100}
              value={batchSize}
              onChange={(e, { value }) => setBatchSize(value)}
              disabled={isDisabled}
              invalidText="Batch size must be between 100 and 10000"
            />
          </div>

          <div className="extraction-settings-filters">
            <h4 className="filters-subtitle">Data Filters</h4>
            <p className="filters-description">
              Select validation checks to apply during extraction
            </p>
            <div className="filters-container">
              {availableFilters.map((filter) => (
                <Checkbox
                  key={filter.id}
                  id={filter.id}
                  labelText={filter.name}
                  checked={selectedFilters[filter.id] || false}
                  onChange={(e) => handleFilterChange(filter.id, e.target.checked)}
                  disabled={isDisabled}
                  helperText={filter.description}
                />
              ))}
            </div>
            {availableFilters.length === 0 && (
              <p className="no-filters">Loading filters...</p>
            )}
          </div>

          <div className="button-container">
            <Button
              kind="primary"
              onClick={handleSubmit}
              disabled={isDisabled}
            >
              {loading ? 'Starting...' : 'Start Extraction'}
            </Button>

            <Button
              kind="danger"
              renderIcon={StopFilled}
              onClick={handleStop}
              disabled={!canStop}
            >
              Stop Extraction
            </Button>

            <Button
              kind="secondary"
              renderIcon={Renew}
              onClick={handleReset}
              disabled={isProcessing}
            >
              Reset
            </Button>
          </div>
        </AccordionItem>

        <AccordionItem title="Extraction Status" open>
          {status ? (
            <div className="status-container">
              <div className="status-row">
                <span className="status-label">Status:</span>
                {getStatusTag()}
                {polling && <InlineLoading description="Updating..." style={{ marginLeft: '1rem' }} />}
              </div>

              {(status.status === 'under_processing' || status.status === 'validating' || status.status === 'finished' || isProcessing) && (
                <>
                  {status.status === 'under_processing' && (
                    <>
                      <div className="status-row">
                        <span className="status-label">Current Month:</span>
                        <span className="status-value">{status.current_month || 'N/A'}</span>
                      </div>

                      <div className="status-row">
                        <span className="status-label">Records Processed:</span>
                        <span className="status-value">
                          {status.records_processed?.toLocaleString() || 0}
                        </span>
                      </div>

                      <div className="status-row">
                        <span className="status-label">Months Completed:</span>
                        <span className="status-value">
                          {status.completed_months || 0} / {status.total_months || 0}
                        </span>
                      </div>
                    </>
                  )}

                  <div className="status-row">
                    <span className="status-label">{status.status === 'finished' ? 'Total Duration:' : 'Elapsed Time:'}</span>
                    <span className="status-value">
                      {status.status === 'finished' && finalDuration !== null ? (
                        (() => {
                          const minutes = Math.floor(finalDuration / 60);
                          const seconds = finalDuration % 60;
                          return finalDuration >= 60 ? `${minutes}m ${seconds}s` : `${finalDuration}s`;
                        })()
                      ) : status.start_time ? (
                        (() => {
                          const elapsed = Math.floor(Date.now() / 1000 - status.start_time);
                          const minutes = Math.floor(elapsed / 60);
                          const seconds = elapsed % 60;
                          return elapsed >= 60 ? `${minutes}m ${seconds}s` : `${seconds}s`;
                        })()
                      ) : 'N/A'}
                    </span>
                  </div>

                  <div className="progress-stepper">
                    <h4>Pipeline Progress:</h4>
                    <div className="stepper-container">
                      {[
                        { key: 'extraction', label: 'Extraction' },
                        { key: 'isv_validation', label: 'ISV Validation' },
                        { key: 'dormancy_check', label: 'Dormancy Check' },
                        { key: 'last_login_check', label: 'Last Login' },
                        { key: 'bluepages_check', label: 'BluPages' }
                      ].map((step, index, array) => {
                        // Determine step status
                        let stepStatus = 'pending';
                        
                        if (step.key === 'extraction') {
                          if (status.status === 'under_processing') {
                            stepStatus = 'running';
                          } else if (status.status === 'validating' || status.status === 'finished') {
                            stepStatus = 'completed';
                          }
                        } else if (status.validation_progress) {
                          stepStatus = status.validation_progress[step.key] || 'pending';
                        } else if (status.status === 'finished') {
                          // If finished and no validation_progress, mark all as completed
                          stepStatus = 'completed';
                        }
                        
                        return (
                          <React.Fragment key={step.key}>
                            <div className={`stepper-step ${stepStatus === 'completed' ? 'completed' : stepStatus === 'running' ? 'active' : 'pending'}`}>
                              <div className="step-circle">
                                {stepStatus === 'completed' ? '✓' : index + 1}
                              </div>
                              <div className="step-label">{step.label}</div>
                            </div>
                            {index < array.length - 1 && (
                              <div className={`stepper-line ${stepStatus === 'completed' ? 'completed' : ''}`} />
                            )}
                          </React.Fragment>
                        );
                      })}
                    </div>
                  </div>
                </>
              )}

              {status.status === 'finished' && !status.error && (
                <>
                  <div className="status-row">
                    <span className="status-label">Total Records Processed:</span>
                    <span className="status-value">
                      {status.records_processed?.toLocaleString() || 0}
                    </span>
                  </div>
                  {status.batch_size && (
                    <div className="status-row">
                      <span className="status-label">Batch Size:</span>
                      <span className="status-value">{status.batch_size}</span>
                    </div>
                  )}
                  {status.filters && Object.keys(status.filters).some(k => status.filters[k]) && (
                    <div className="status-row">
                      <span className="status-label">Filters:</span>
                      <span className="status-value">
                        {Object.keys(status.filters)
                          .filter(k => status.filters[k])
                          .map(filterId => {
                            const filter = availableFilters.find(f => f.id === filterId);
                            return filter ? filter.name : filterId;
                          })
                          .join(', ')}
                      </span>
                    </div>
                  )}
                  
                  {status.output_file && (
                    <div className="status-row">
                      <Button
                        kind="tertiary"
                        size="sm"
                        renderIcon={Download}
                        onClick={() => handleDownload(status.output_file)}
                      >
                        Download
                      </Button>
                      <Button
                        kind="ghost"
                        size="sm"
                        onClick={() => handleViewData(status.output_file)}
                      >
                        View Data
                      </Button>
                    </div>
                  )}
                </>
              )}

              {status.error && (
                <InlineNotification
                  kind="error"
                  title="Extraction Error"
                  subtitle={status.error}
                  hideCloseButton
                  lowContrast
                />
              )}

              {status.start_date && status.end_date && (
                <div className="status-row">
                  <span className="status-label">Date Range:</span>
                  <span className="status-value">
                    {status.start_date} to {status.end_date}
                  </span>
                </div>
              )}

              {status.last_updated && (
                <div className="status-row">
                  <span className="status-label">Last Updated:</span>
                  <span className="status-value">
                    {new Date(status.last_updated).toLocaleString()}
                  </span>
                </div>
              )}
            </div>
          ) : (
            <Loading description="Loading status..." />
          )}
        </AccordionItem>
      </Accordion>
        </div>

        {/* Right sidebar with extraction history */}
        <div className="history-sidebar">
          <div className="history-sidebar-header">
            <h2>Extraction History</h2>
            {history.length > 0 && (
              <Button
                kind="danger--tertiary"
                size="sm"
                onClick={handleClearAllHistory}
              >
                Clear All
              </Button>
            )}
          </div>
          <div className="history-sidebar-content">
            {history.length > 0 ? (
              <div className="history-list">
                {history.map((entry, index) => (
                  <div key={entry.id} className="history-item">
                    <div className="history-job-id">EX{String(history.length - index).padStart(3, '0')}</div>
                    <div className="history-timestamp">
                      {new Date(entry.timestamp).toLocaleString()}
                    </div>
                    <div className="history-item-details">
                      <div className="history-detail">
                        <span className="history-label">Extraction Mode</span>
                        <span className="history-value">
                          {entry.extraction_mode === 'specific_ids' ? 'Specific IDs' : 'Date Range (ISV, Dormancy)'}
                        </span>
                      </div>
                      <div className="history-detail">
                        <span className="history-label">Records</span>
                        <span className="history-value">
                          {entry.records_processed?.toLocaleString() || 0}
                        </span>
                      </div>
                      <div className="history-detail">
                        <span className="history-label">Status</span>
                        <span className="history-value">
                          <Tag type={entry.status === 'completed' ? 'green' : 'red'}>
                            {entry.status === 'completed' ? 'Completed' : entry.error ? 'Failed (Network Error)' : 'Failed'}
                          </Tag>
                        </span>
                      </div>
                    </div>
                    <div className="history-item-actions">
                      <Button
                        kind="ghost"
                        size="sm"
                        renderIcon={View}
                        onClick={() => {
                          if (entry.filename) {
                            window.open(`/view?file=${encodeURIComponent(entry.filename)}`, '_blank');
                          }
                        }}
                        iconDescription="View"
                        disabled={!entry.filename}
                        hasIconOnly
                      />
                      <Button
                        kind="ghost"
                        size="sm"
                        renderIcon={Download}
                        onClick={() => entry.filename && handleDownload(entry.filename)}
                        iconDescription="Download"
                        disabled={!entry.filename}
                        hasIconOnly
                      />
                      <Button
                        kind="danger--ghost"
                        size="sm"
                        renderIcon={TrashCan}
                        onClick={() => handleDeleteHistory(entry.id)}
                        iconDescription="Delete"
                        hasIconOnly
                      />
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-history">
                <p>📋</p>
                <p>No extraction history yet</p>
                <p className="empty-history-subtitle">
                  Start your first extraction to see it here
                </p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Data Viewer Modal */}
      <Modal
        open={viewModalOpen}
        onRequestClose={() => setViewModalOpen(false)}
        modalHeading={`Viewing: ${viewFilename}`}
        passiveModal
        size="lg"
      >
        {viewLoading ? (
          <div style={{ padding: '2rem', textAlign: 'center' }}>
            <InlineLoading description="Loading data..." />
          </div>
        ) : viewData.length > 0 ? (
          <>
            <DataTable
              rows={viewData.map((item, idx) => ({
                id: `row-${viewPagination.page}-${idx}`,
                ...item
              }))}
              headers={
                viewData.length > 0
                  ? Object.keys(viewData[0]).map(key => ({
                      key,
                      header: key.charAt(0).toUpperCase() + key.slice(1).replace(/_/g, ' ')
                    }))
                  : []
              }
            >
              {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
                <TableContainer>
                  <Table {...getTableProps()}>
                    <TableHead>
                      <TableRow>
                        {headers.map(header => (
                          <TableHeader {...getHeaderProps({ header })}>
                            {header.header}
                          </TableHeader>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {rows.map(row => (
                        <TableRow {...getRowProps({ row })}>
                          {row.cells.map(cell => (
                            <TableCell key={cell.id}>
                              {typeof cell.value === 'object'
                                ? JSON.stringify(cell.value)
                                : cell.value}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </DataTable>
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                Showing {((viewPagination.page - 1) * viewPagination.page_size) + 1} - {Math.min(viewPagination.page * viewPagination.page_size, viewPagination.total_records)} of {viewPagination.total_records} records
              </div>
              <Pagination
                page={viewPagination.page}
                totalItems={viewPagination.total_records}
                pageSize={viewPagination.page_size}
                pageSizes={[100, 250, 500, 1000]}
                onChange={handlePageChange}
              />
            </div>
          </>
        ) : (
          <div style={{ padding: '2rem', textAlign: 'center' }}>
            <p>No data available</p>
          </div>
        )}
      </Modal>
    </div>
  );
}

export default App;
