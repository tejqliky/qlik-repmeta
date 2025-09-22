import React, { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8002";

interface RunSummary {
  run_id: number;
  uploaded_at: string;
  filename: string;
  customer_name: string;
  server_name: string;
  task_count: number;
  source_table_count: number;
  target_count: number;
  feature_flag_count: number;
}

interface RunDetails {
  run: any;
  tasks: any[];
  source_tables: any[];
  targets: any[];
  feature_flags: any[];
  endpoints: any[];
  unknown_fields: any[];
}

interface TableData {
  table_name: string;
  data: any[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    pages: number;
  };
}

interface DataBrowserProps {
  onClose?: () => void;
}

export const DataBrowser: React.FC<DataBrowserProps> = ({ onClose }) => {
  const [activeTab, setActiveTab] = useState<'summary' | 'runs' | 'tables'>('summary');
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [selectedTable, setSelectedTable] = useState<string>('task');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // Data states
  const [summary, setSummary] = useState<{runs: RunSummary[], table_counts: any, total_runs: number} | null>(null);
  const [runDetails, setRunDetails] = useState<RunDetails | null>(null);
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);

  // Load summary data
  const loadSummary = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/api/browse/summary`);
      if (!response.ok) throw new Error('Failed to load summary');
      const data = await response.json();
      setSummary(data);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Load run details
  const loadRunDetails = async (runId: number) => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/api/browse/run/${runId}`);
      if (!response.ok) throw new Error('Failed to load run details');
      const data = await response.json();
      setRunDetails(data);
      setSelectedRunId(runId);
      setActiveTab('runs');
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Load table data
  const loadTableData = async (tableName: string, page: number = 1, search: string = '') => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: '50'
      });
      if (search) params.append('search', search);
      if (selectedRunId) params.append('run_id', selectedRunId.toString());

      const response = await fetch(`${API_BASE}/api/browse/table/${tableName}?${params}`);
      if (!response.ok) throw new Error('Failed to load table data');
      const data = await response.json();
      setTableData(data);
      setSelectedTable(tableName);
      setCurrentPage(page);
      setActiveTab('tables');
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Export table data
  const exportTable = async (tableName: string) => {
    try {
      const params = selectedRunId ? `?run_id=${selectedRunId}` : '';
      const response = await fetch(`${API_BASE}/api/browse/export/${tableName}${params}`);
      if (!response.ok) throw new Error('Failed to export data');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${tableName}_export.csv`;
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (err: any) {
      setError(err.message);
    }
  };

  useEffect(() => {
    if (activeTab === 'summary') {
      loadSummary();
    }
  }, [activeTab]);

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  const formatJson = (obj: any) => {
    if (typeof obj === 'object' && obj !== null) {
      return JSON.stringify(obj, null, 2);
    }
    return obj;
  };

  const renderSummaryTab = () => (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-blue-50 rounded-lg p-4">
          <div className="text-2xl font-bold text-blue-700">{summary?.total_runs || 0}</div>
          <div className="text-sm text-blue-600">Total Runs</div>
        </div>
        <div className="bg-green-50 rounded-lg p-4">
          <div className="text-2xl font-bold text-green-700">{summary?.table_counts?.task || 0}</div>
          <div className="text-sm text-green-600">Tasks</div>
        </div>
        <div className="bg-purple-50 rounded-lg p-4">
          <div className="text-2xl font-bold text-purple-700">{summary?.table_counts?.source_table || 0}</div>
          <div className="text-sm text-purple-600">Source Tables</div>
        </div>
        <div className="bg-orange-50 rounded-lg p-4">
          <div className="text-2xl font-bold text-orange-700">{summary?.table_counts?.endpoint || 0}</div>
          <div className="text-sm text-orange-600">Endpoints</div>
        </div>
      </div>

      {/* Recent Runs */}
      <div className="bg-white rounded-lg border">
        <div className="px-6 py-4 border-b">
          <h3 className="text-lg font-semibold">Recent Runs</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Run ID</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Filename</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Uploaded At</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tasks</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tables</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {summary?.runs.map((run) => (
                <tr key={run.run_id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 text-sm font-medium text-gray-900">{run.run_id}</td>
                  <td className="px-6 py-4 text-sm text-gray-500">{run.filename || 'N/A'}</td>
                  <td className="px-6 py-4 text-sm text-gray-500">{formatDate(run.uploaded_at)}</td>
                  <td className="px-6 py-4 text-sm text-gray-500">{run.task_count}</td>
                  <td className="px-6 py-4 text-sm text-gray-500">{run.source_table_count}</td>
                  <td className="px-6 py-4 text-sm">
                    <button
                      onClick={() => loadRunDetails(run.run_id)}
                      className="text-blue-600 hover:text-blue-800 font-medium"
                    >
                      View Details
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  const renderRunDetailsTab = () => (
    <div className="space-y-6">
      {runDetails && (
        <>
          {/* Run Info */}
          <div className="bg-white rounded-lg border p-6">
            <div className="flex justify-between items-start mb-4">
              <h3 className="text-lg font-semibold">Run {runDetails.run.run_id} Details</h3>
              <button
                onClick={() => {setActiveTab('summary'); setSelectedRunId(null);}}
                className="text-gray-500 hover:text-gray-700"
              >
                ← Back to Summary
              </button>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              <div>
                <span className="font-medium">Filename:</span>
                <div className="text-gray-600">{runDetails.run.filename || 'N/A'}</div>
              </div>
              <div>
                <span className="font-medium">Uploaded:</span>
                <div className="text-gray-600">{formatDate(runDetails.run.uploaded_at)}</div>
              </div>
              <div>
                <span className="font-medium">Customer:</span>
                <div className="text-gray-600">{runDetails.run.customer_name || 'N/A'}</div>
              </div>
              <div>
                <span className="font-medium">Server:</span>
                <div className="text-gray-600">{runDetails.run.server_name || 'N/A'}</div>
              </div>
            </div>
          </div>

          {/* Data Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
            {[
              { name: 'Tasks', count: runDetails.tasks.length, color: 'blue' },
              { name: 'Source Tables', count: runDetails.source_tables.length, color: 'green' },
              { name: 'Targets', count: runDetails.targets.length, color: 'purple' },
              { name: 'Feature Flags', count: runDetails.feature_flags.length, color: 'orange' },
              { name: 'Endpoints', count: runDetails.endpoints.length, color: 'red' },
              { name: 'Unknown Fields', count: runDetails.unknown_fields.length, color: 'gray' }
            ].map((item) => (
              <div key={item.name} className={`bg-${item.color}-50 rounded-lg p-4 cursor-pointer hover:bg-${item.color}-100`}
                   onClick={() => loadTableData(item.name.toLowerCase().replace(' ', '_'))}>
                <div className={`text-2xl font-bold text-${item.color}-700`}>{item.count}</div>
                <div className={`text-sm text-${item.color}-600`}>{item.name}</div>
              </div>
            ))}
          </div>

          {/* Quick Data Preview */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Tasks Preview */}
            <div className="bg-white rounded-lg border">
              <div className="px-4 py-3 border-b flex justify-between items-center">
                <h4 className="font-medium">Tasks ({runDetails.tasks.length})</h4>
                <button
                  onClick={() => loadTableData('task')}
                  className="text-blue-600 hover:text-blue-800 text-sm"
                >
                  View All
                </button>
              </div>
              <div className="p-4 space-y-2 max-h-64 overflow-y-auto">
                {runDetails.tasks.slice(0, 5).map((task, idx) => (
                  <div key={idx} className="text-sm border-l-4 border-blue-200 pl-3">
                    <div className="font-medium">{task.name}</div>
                    <div className="text-gray-500">Source: {task.source_name}</div>
                  </div>
                ))}
                {runDetails.tasks.length > 5 && (
                  <div className="text-sm text-gray-500">... and {runDetails.tasks.length - 5} more</div>
                )}
              </div>
            </div>

            {/* Source Tables Preview */}
            <div className="bg-white rounded-lg border">
              <div className="px-4 py-3 border-b flex justify-between items-center">
                <h4 className="font-medium">Source Tables ({runDetails.source_tables.length})</h4>
                <button
                  onClick={() => loadTableData('source_table')}
                  className="text-green-600 hover:text-green-800 text-sm"
                >
                  View All
                </button>
              </div>
              <div className="p-4 space-y-2 max-h-64 overflow-y-auto">
                {runDetails.source_tables.slice(0, 5).map((table, idx) => (
                  <div key={idx} className="text-sm border-l-4 border-green-200 pl-3">
                    <div className="font-medium">{table.owner}.{table.table_name}</div>
                    <div className="text-gray-500">Task: {table.task_name} | Size: {table.estimated_size?.toLocaleString() || 'N/A'}</div>
                  </div>
                ))}
                {runDetails.source_tables.length > 5 && (
                  <div className="text-sm text-gray-500">... and {runDetails.source_tables.length - 5} more</div>
                )}
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );

  const renderTablesTab = () => (
    <div className="space-y-4">
      {/* Table Controls */}
      <div className="bg-white rounded-lg border p-4">
        <div className="flex flex-wrap gap-4 items-center justify-between">
          <div className="flex gap-2">
            {['task', 'source_table', 'task_target', 'feature_flag_value', 'endpoint', 'unknown_field'].map((table) => (
              <button
                key={table}
                onClick={() => loadTableData(table, 1, searchTerm)}
                className={`px-3 py-1 rounded text-sm font-medium ${
                  selectedTable === table
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {table.replace('_', ' ')}
              </button>
            ))}
          </div>
          <div className="flex gap-2 items-center">
            <input
              type="text"
              placeholder="Search..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="px-3 py-1 border rounded text-sm"
              onKeyPress={(e) => e.key === 'Enter' && loadTableData(selectedTable, 1, searchTerm)}
            />
            <button
              onClick={() => loadTableData(selectedTable, 1, searchTerm)}
              className="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
            >
              Search
            </button>
            <button
              onClick={() => exportTable(selectedTable)}
              className="px-3 py-1 bg-green-600 text-white rounded text-sm hover:bg-green-700"
            >
              Export CSV
            </button>
          </div>
        </div>
      </div>

      {/* Table Data */}
      {tableData && (
        <div className="bg-white rounded-lg border">
          <div className="px-6 py-4 border-b flex justify-between items-center">
            <h3 className="text-lg font-semibold">
              {tableData.table_name.replace('_', ' ')} 
              <span className="text-sm text-gray-500 ml-2">
                ({tableData.pagination.total} records)
              </span>
            </h3>
            {selectedRunId && (
              <button
                onClick={() => {setSelectedRunId(null); loadTableData(selectedTable, 1, searchTerm);}}
                className="text-sm text-gray-500 hover:text-gray-700"
              >
                Clear Run Filter
              </button>
            )}
          </div>
          
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  {tableData.data[0] && Object.keys(tableData.data[0]).map((key) => (
                    <th key={key} className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {tableData.data.map((row, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    {Object.values(row).map((value: any, cellIdx) => (
                      <td key={cellIdx} className="px-4 py-2 text-sm text-gray-900 max-w-xs">
                        <div className="truncate" title={formatJson(value)}>
                          {typeof value === 'object' && value !== null ? (
                            <span className="text-blue-600 cursor-pointer">
                              {JSON.stringify(value).substring(0, 50)}...
                            </span>
                          ) : (
                            String(value)
                          )}
                        </div>
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {tableData.pagination.pages > 1 && (
            <div className="px-6 py-4 border-t flex justify-between items-center">
              <div className="text-sm text-gray-500">
                Page {tableData.pagination.page} of {tableData.pagination.pages} 
                ({tableData.pagination.total} total records)
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => loadTableData(selectedTable, currentPage - 1, searchTerm)}
                  disabled={currentPage <= 1}
                  className="px-3 py-1 bg-gray-100 text-gray-700 rounded text-sm hover:bg-gray-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Previous
                </button>
                <span className="px-3 py-1 text-sm">
                  {currentPage} / {tableData.pagination.pages}
                </span>
                <button
                  onClick={() => loadTableData(selectedTable, currentPage + 1, searchTerm)}
                  disabled={currentPage >= tableData.pagination.pages}
                  className="px-3 py-1 bg-gray-100 text-gray-700 rounded text-sm hover:bg-gray-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-7xl h-full max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b flex justify-between items-center bg-gray-50 rounded-t-lg">
          <div className="flex items-center space-x-4">
            <h2 className="text-xl font-bold text-gray-900">Data Browser</h2>
            {selectedRunId && (
              <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-sm">
                Run {selectedRunId}
              </span>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700 text-xl font-bold"
          >
            ×
          </button>
        </div>

        {/* Navigation Tabs */}
        <div className="px-6 py-2 border-b bg-gray-50">
          <div className="flex space-x-4">
            {[
              { id: 'summary', label: 'Dashboard', icon: '📊' },
              { id: 'runs', label: 'Run Details', icon: '🔍' },
              { id: 'tables', label: 'Table Browser', icon: '📋' }
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={`px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors ${
                  activeTab === tab.id
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'
                }`}
              >
                <span>{tab.icon}</span>
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {/* Content Area */}
        <div className="flex-1 overflow-auto p-6">
          {loading && (
            <div className="flex items-center justify-center h-64">
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-blue-600 rounded-full animate-pulse"></div>
                <div className="w-4 h-4 bg-blue-600 rounded-full animate-pulse" style={{animationDelay: '0.1s'}}></div>
                <div className="w-4 h-4 bg-blue-600 rounded-full animate-pulse" style={{animationDelay: '0.2s'}}></div>
                <span className="ml-2 text-gray-600">Loading...</span>
              </div>
            </div>
          )}

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
              <div className="flex items-center">
                <span className="text-red-600 mr-2">⚠️</span>
                <span className="text-red-800">Error: {error}</span>
                <button
                  onClick={() => setError(null)}
                  className="ml-auto text-red-600 hover:text-red-800"
                >
                  ×
                </button>
              </div>
            </div>
          )}

          {!loading && !error && (
            <>
              {activeTab === 'summary' && renderSummaryTab()}
              {activeTab === 'runs' && renderRunDetailsTab()}
              {activeTab === 'tables' && renderTablesTab()}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-3 border-t bg-gray-50 rounded-b-lg">
          <div className="flex justify-between items-center text-sm text-gray-500">
            <div>
              💡 Tip: Use the Table Browser to explore data, export CSV files, and search across records
            </div>
            <div className="flex items-center space-x-4">
              <span>Total Tables: {summary?.table_counts ? Object.keys(summary.table_counts).length : 0}</span>
              <span>•</span>
              <span>Last Updated: {new Date().toLocaleTimeString()}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// Hook to integrate with your main App component
export const useDataBrowser = () => {
  const [isOpen, setIsOpen] = useState(false);

  const openBrowser = () => setIsOpen(true);
  const closeBrowser = () => setIsOpen(false);

  const DataBrowserModal = () => 
    isOpen ? <DataBrowser onClose={closeBrowser} /> : null;

  return {
    openBrowser,
    closeBrowser,
    isOpen,
    DataBrowserModal
  };
};