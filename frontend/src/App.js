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
  Pagination
} from '@carbon/react';
import { Renew, Download, StopFilled, TrashCan } from '@carbon/icons-react';
import axios from 'axios';
import './App.scss';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000/api';

function App() {
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [startTime, setStartTime] = useState('00:00');
  const [endTime, setEndTime] = useState('23:59');
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
  const [extractions, setExtractions] = useState([]);
  const [loadingExtractions, setLoadingExtractions] = useState(false);

  // Fetch status from API
  const fetchStatus = useCallback(async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/status`);
      setStatus(response.data);
      
      // Start polling if under processing
      if (response.data.status === 'under_processing' && !polling) {
        setPolling(true);
      } else if (response.data.status !== 'under_processing' && polling) {
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
  }, [polling]);

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

  // Fetch extractions from API
  const fetchExtractions = useCallback(async () => {
    setLoadingExtractions(true);
    try {
      const response = await axios.get(`${API_BASE_URL}/extractions`);
      if (response.data.success) {
        setExtractions(response.data.extractions);
      }
    } catch (error) {
      console.error('Error fetching extractions:', error);
    } finally {
      setLoadingExtractions(false);
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

  // Fetch available filters
  const fetchFilters = useCallback(async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/filters`);
      if (response.data.success) {
        setAvailableFilters(response.data.filters);
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

  // Handle delete history entry
  const handleDeleteHistory = async (historyId) => {
    if (!window.confirm('Are you sure you want to delete this history entry and all its associated files?')) {
      return;
    }

    try {
      const response = await axios.delete(`${API_BASE_URL}/history/${historyId}`);
      
      if (response.data.success) {
        setNotification({
          kind: 'success',
          title: 'Deleted Successfully',
          subtitle: `Deleted ${response.data.deleted_count} file(s)`
        });
        
        // Refresh history list
        fetchHistory();
      } else {
        setNotification({
          kind: 'error',
          title: 'Delete Failed',
          subtitle: response.data.error || 'Failed to delete history entry'
        });
      }
    } catch (error) {
      setNotification({
        kind: 'error',
        title: 'Delete Failed',
        subtitle: error.response?.data?.error || 'Failed to delete history entry'
      });
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

  // Initial status, history, filters, and extractions fetch
  useEffect(() => {
    fetchStatus();
    fetchHistory();
    fetchFilters();
    fetchExtractions();
  }, [fetchStatus, fetchHistory, fetchFilters, fetchExtractions]);

  // Polling effect
  useEffect(() => {
    let interval;
    if (polling) {
      interval = setInterval(() => {
        fetchStatus();
      }, 5000); // Poll every 5 seconds
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [polling, fetchStatus]);

  // Handle form submission
  const handleSubmit = async () => {
    if (!startDate || !endDate) {
      setNotification({
        kind: 'error',
        title: 'Validation Error',
        subtitle: 'Please select both start and end dates'
      });
      return;
    }

    setLoading(true);
    setNotification(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/retrieve`, {
        start_date: `${startDate} ${startTime}:00`,
        end_date: `${endDate} ${endTime}:00`,
        batch_size: batchSize,
        filters: selectedFilters
      });

      if (response.data.success) {
        setNotification({
          kind: 'success',
          title: 'Success',
          subtitle: 'Data retrieval started successfully'
        });
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
        setNotification({
          kind: 'success',
          title: 'Success',
          subtitle: 'Status reset successfully'
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

  const isProcessing = status?.status === 'under_processing';
  const isDisabled = isProcessing || loading;

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
            <AccordionItem title="Date Range Configuration" open>
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
              disabled={!isProcessing}
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
              </div>

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

                  {status.start_time && (
                    <div className="status-row">
                      <span className="status-label">Elapsed Time:</span>
                      <span className="status-value">
                        {(() => {
                          const elapsed = Math.floor(Date.now() / 1000 - status.start_time);
                          const minutes = Math.floor(elapsed / 60);
                          const seconds = elapsed % 60;
                          return elapsed >= 60 ? `${minutes}m ${seconds}s` : `${seconds}s`;
                        })()}
                      </span>
                    </div>
                  )}

                  <div className="progress-container">
                    <ProgressBar
                      label="Progress"
                      value={status.progress_percent || 0}
                      max={100}
                      helperText={`${status.progress_percent || 0}% complete`}
                    />
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
                  {status.duration_seconds != null && status.duration_seconds > 0 && (
                    <div className="status-row">
                      <span className="status-label">Duration:</span>
                      <span className="status-value">
                        {status.duration_seconds >= 60
                          ? `${Math.floor(status.duration_seconds / 60)}m ${status.duration_seconds % 60}s`
                          : `${status.duration_seconds}s`
                        }
                      </span>
                    </div>
                  )}
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

        <AccordionItem title="Extraction History">
          {history.length > 0 ? (
            <div className="history-container">
              <p className="history-description">
                View past extraction jobs and download their data files.
              </p>
              <div className="history-list">
                {history.map((entry) => (
                  <div key={entry.id} className="history-item">
                    <div className="history-item-header">
                      <Tag type={entry.status === 'completed' ? 'green' : 'red'}>
                        {entry.status}
                      </Tag>
                      <span className="history-timestamp">
                        {new Date(entry.timestamp).toLocaleString()}
                      </span>
                      {entry.status === 'completed' && entry.filename && (
                        <>
                          <Button
                            kind="ghost"
                            size="sm"
                            renderIcon={Download}
                            onClick={() => handleDownload(entry.filename)}
                            className="history-download-btn"
                          >
                            Download
                          </Button>
                          <Button
                            kind="ghost"
                            size="sm"
                            onClick={() => handleViewData(entry.filename)}
                            className="history-download-btn"
                          >
                            View Data
                          </Button>
                        </>
                      )}
                      <Button
                        kind="danger--ghost"
                        size="sm"
                        renderIcon={TrashCan}
                        onClick={() => handleDeleteHistory(entry.id)}
                        className="history-delete-btn"
                        iconDescription="Delete"
                        hasIconOnly
                      />
                    </div>
                    <div className="history-item-details">
                      <div className="history-detail">
                        <span className="history-label">Date Range:</span>
                        <span className="history-value">
                          {entry.start_date} to {entry.end_date}
                        </span>
                      </div>
                      <div className="history-detail">
                        <span className="history-label">Records:</span>
                        <span className="history-value">
                          {entry.records_processed?.toLocaleString() || 0}
                        </span>
                      </div>
                      <div className="history-detail">
                        <span className="history-label">Months:</span>
                        <span className="history-value">
                          {entry.months_processed || 0}
                        </span>
                      </div>
                      {entry.duration_seconds != null && entry.duration_seconds > 0 && (
                        <div className="history-detail">
                          <span className="history-label">Duration:</span>
                          <span className="history-value">
                            {entry.duration_seconds >= 60
                              ? `${Math.floor(entry.duration_seconds / 60)}m ${entry.duration_seconds % 60}s`
                              : `${entry.duration_seconds}s`
                            }
                          </span>
                        </div>
                      )}
                      {entry.batch_size && (
                        <div className="history-detail">
                          <span className="history-label">Batch Size:</span>
                          <span className="history-value">{entry.batch_size}</span>
                        </div>
                      )}
                      {entry.filters && Object.keys(entry.filters).some(k => entry.filters[k]) && (
                        <div className="history-detail">
                          <span className="history-label">Filters:</span>
                          <span className="history-value">
                            {Object.keys(entry.filters)
                              .filter(k => entry.filters[k])
                              .map(filterId => {
                                const filter = availableFilters.find(f => f.id === filterId);
                                return filter ? filter.name : filterId;
                              })
                              .join(', ')}
                          </span>
                        </div>
                      )}
                      {entry.filename && (
                        <div className="history-detail">
                          <span className="history-label">File:</span>
                          <span className="history-value">{entry.filename}</span>
                        </div>
                      )}
                      {entry.error && (
                        <div className="history-detail error">
                          <span className="history-label">Error:</span>
                          <span className="history-value">{entry.error}</span>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="empty-history">
              <p>📋 No extraction history yet</p>
              <p className="empty-history-subtitle">
                Start your first extraction to see it here
              </p>
            </div>
          )}
        </AccordionItem>
      </Accordion>
        </div>

        {/* Right sidebar with configuration options */}
        <div className="config-sidebar">
          <div className="batch-size-section">
            <NumberInput
              id="batch-size"
              label="Batch Size"
              helperText="Number of records to fetch per batch (1000-5000 recommended)"
              min={100}
              max={10000}
              step={100}
              value={batchSize}
              onChange={(e, { value }) => setBatchSize(value)}
              disabled={isDisabled}
              invalidText="Batch size must be between 100 and 10000"
            />
          </div>

          <div className="filters-section">
            <h4 className="filters-title">Data Filtering Options</h4>
            <p className="filters-description">
              Select which filters to apply during extraction. Only records passing all selected filters will be included.
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
