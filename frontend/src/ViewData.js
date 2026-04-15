import React, { useState, useEffect } from 'react';
import {
  Button,
  DataTable,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  TableToolbar,
  TableToolbarContent,
  TableToolbarSearch,
  Pagination,
  Loading,
  InlineNotification,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
  Tag
} from '@carbon/react';
import { Download, ArrowLeft } from '@carbon/icons-react';
import axios from 'axios';
import './ViewData.scss';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000/api';

function ViewData() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filename, setFilename] = useState('');
  const [searchValue, setSearchValue] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const file = params.get('file');
    if (file) {
      setFilename(file);
      fetchData(file);
    } else {
      setError('No file specified');
      setLoading(false);
    }
  }, []);

  const fetchData = async (file) => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE_URL}/view/${file}`);
      console.log('Received data:', response.data);
      console.log('Data type:', Array.isArray(response.data) ? 'Array' : 'Object');
      console.log('Keys:', Object.keys(response.data));
      setData(response.data);
      setError(null);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/download/${filename}`, {
        responseType: 'blob'
      });
      
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Download failed:', error);
    }
  };

  const filterData = (items) => {
    if (!searchValue) return items;
    return items.filter(item => 
      item.id?.toLowerCase().includes(searchValue.toLowerCase()) ||
      item.username?.toLowerCase().includes(searchValue.toLowerCase())
    );
  };

  const paginateData = (items) => {
    const startIndex = (currentPage - 1) * pageSize;
    return items.slice(startIndex, startIndex + pageSize);
  };

  const renderTable = (items, title) => {
    const filteredItems = filterData(items);
    const paginatedItems = paginateData(filteredItems);

    const headers = [
      { key: 'userId', header: 'ID' },
      { key: 'username', header: 'Username' },
      { key: 'lastLogin', header: 'Last Login' },
      { key: 'activeStatus', header: 'Active Status' },
      { key: 'reasons', header: 'Reasons' }
    ];

    const rows = paginatedItems.map((item, index) => ({
      id: `row-${index}`,
      userId: item.id || 'N/A',
      username: item.username || 'N/A',
      lastLogin: item.lastLogin ? new Date(item.lastLogin).toLocaleString() : 'N/A',
      activeStatus: item.activeStatus ? 'Active' : 'Inactive',
      reasons: item.reasons?.join('; ') || 'N/A'
    }));

    return (
      <div className="table-container">
        <DataTable rows={rows} headers={headers}>
          {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
            <TableContainer title={title}>
              <TableToolbar>
                <TableToolbarContent>
                  <TableToolbarSearch
                    value={searchValue}
                    onChange={(e) => {
                      setSearchValue(e.target.value);
                      setCurrentPage(1);
                    }}
                    placeholder="Search by ID or Username"
                  />
                </TableToolbarContent>
              </TableToolbar>
              <Table {...getTableProps()}>
                <TableHead>
                  <TableRow>
                    {headers.map((header) => (
                      <TableHeader {...getHeaderProps({ header })}>
                        {header.header}
                      </TableHeader>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow {...getRowProps({ row })}>
                      {row.cells.map((cell) => (
                        <TableCell key={cell.id}>{cell.value}</TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <Pagination
                totalItems={filteredItems.length}
                pageSize={pageSize}
                pageSizes={[10, 20, 50, 100]}
                page={currentPage}
                onChange={({ page, pageSize }) => {
                  setCurrentPage(page);
                  setPageSize(pageSize);
                }}
              />
            </TableContainer>
          )}
        </DataTable>
      </div>
    );
  };

  if (loading) {
    return (
      <div className="view-data-container">
        <Loading description="Loading data..." />
      </div>
    );
  }

  if (error) {
    return (
      <div className="view-data-container">
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
        />
        <Button
          kind="tertiary"
          renderIcon={ArrowLeft}
          onClick={() => window.close()}
        >
          Close
        </Button>
      </div>
    );
  }

  return (
    <div className="view-data-container">
      <div className="view-data-header">
        <div className="header-left">
          <Button
            kind="ghost"
            renderIcon={ArrowLeft}
            onClick={() => window.close()}
          >
            Close
          </Button>
          <h1>Extraction Results: {filename}</h1>
        </div>
        <Button
          kind="primary"
          renderIcon={Download}
          onClick={handleDownload}
        >
          Download
        </Button>
      </div>

      <Tabs>
        <TabList aria-label="Data categories">
          <Tab>
            To Be Deleted
            <Tag type="red" size="sm">{data?.to_be_deleted?.length || 0}</Tag>
          </Tab>
          <Tab>
            Not To Be Deleted
            <Tag type="green" size="sm">{data?.not_to_be_deleted?.length || 0}</Tag>
          </Tab>
          <Tab>
            ISV Failed
            <Tag type="gray" size="sm">{data?.isv_failed_ids?.length || 0}</Tag>
          </Tab>
          <Tab>
            ISV Inactive
            <Tag type="gray" size="sm">{data?.isv_inactive_users?.length || 0}</Tag>
          </Tab>
        </TabList>

        <TabPanels>
          <TabPanel>
            {renderTable(data?.to_be_deleted || [], 'Users To Be Deleted')}
          </TabPanel>
          <TabPanel>
            {renderTable(data?.not_to_be_deleted || [], 'Users Not To Be Deleted')}
          </TabPanel>
          <TabPanel>
            {renderTable(data?.isv_failed_ids || [], 'ISV Failed IDs')}
          </TabPanel>
          <TabPanel>
            {renderTable(data?.isv_inactive_users || [], 'ISV Inactive Users')}
          </TabPanel>
        </TabPanels>
      </Tabs>
    </div>
  );
}

export default ViewData;

// Made with Bob
