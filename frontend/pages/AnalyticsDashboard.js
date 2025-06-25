import React, { useState, useEffect } from 'react';
import { 
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, 
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  ScatterChart, Scatter 
} from 'recharts';
import { 
  TrendingUp, DollarSign, Activity, Database, 
  RefreshCw, AlertCircle, ArrowUpRight, ArrowDownRight,
  Calendar, Filter, BarChart2, PieChart as PieChartIcon
} from 'lucide-react';

const AnalyticsDashboard = ({ styles, COLORS, API_BASE_URL }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedDimension, setSelectedDimension] = useState('');
  const [selectedMetric, setSelectedMetric] = useState('');
  const [dateRange, setDateRange] = useState('all');
  const [insights, setInsights] = useState([]);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE_URL}/qa/summary-stats`);
      if (!response.ok) throw new Error('Failed to fetch data');
      
      const result = await response.json();
      setData(result);
      
      // Set default selections
      if (result.metadata?.categorical_columns?.length > 0) {
        setSelectedDimension(result.metadata.categorical_columns[0]);
      }
      if (result.metadata?.numeric_columns?.length > 0) {
        setSelectedMetric(result.metadata.numeric_columns[0]);
      }
      
      // Generate insights
      generateInsights(result.summary_stats);
      
    } catch (error) {
      console.error('Dashboard data fetch error:', error);
      setError('Failed to load dashboard data. Please check your connection.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const generateInsights = (stats) => {
    const newInsights = [];
    
    // Numeric insights
    Object.entries(stats.numeric_stats || {}).forEach(([column, colStats]) => {
      if (colStats.std && colStats.mean && (colStats.std / colStats.mean) > 0.5) {
        newInsights.push({
          icon: '📊',
          title: 'High Variance Detected',
          value: `${formatColumnName(column)} shows significant variation`,
          detail: `CV: ${(colStats.std / colStats.mean).toFixed(2)}`
        });
      }
      
      if (colStats.max && colStats.min) {
        const range = colStats.max - colStats.min;
        newInsights.push({
          icon: '📈',
          title: 'Data Range',
          value: `${formatColumnName(column)} spans ${formatValue(range)}`,
          detail: `From ${formatValue(colStats.min)} to ${formatValue(colStats.max)}`
        });
      }
    });
    
    // Categorical insights
    Object.entries(stats.categorical_stats || {}).forEach(([column, colStats]) => {
      if (colStats.top_values && colStats.top_values.length > 0) {
        const topValue = colStats.top_values[0];
        const percentage = ((topValue.count / stats.total_records) * 100).toFixed(1);
        newInsights.push({
          icon: '🎯',
          title: 'Dominant Category',
          value: `"${topValue.value}" leads in ${formatColumnName(column)}`,
          detail: `${percentage}% of all records`
        });
      }
    });
    
    setInsights(newInsights.slice(0, 4));
  };

  const formatColumnName = (name) => {
    return name.split('_').map(word => 
      word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    ).join(' ');
  };

  const formatValue = (value) => {
    if (typeof value !== 'number') return value;
    
    if (value >= 1000000) {
      return '$' + (value / 1000000).toFixed(1) + 'M';
    } else if (value >= 1000) {
      return '$' + (value / 1000).toFixed(1) + 'K';
    } else {
      return '$' + value.toFixed(2);
    }
  };

  const formatNumber = (value) => {
    if (value === null || value === undefined) return value;
    const numValue = typeof value === 'string' ? parseFloat(value) : value;
    
    if (typeof numValue === 'number' && !isNaN(numValue)) {
      if (Number.isInteger(numValue)) {
        return numValue.toLocaleString();
      } else {
        return parseFloat(numValue.toFixed(2)).toLocaleString(undefined, {
          minimumFractionDigits: 0,
          maximumFractionDigits: 2
        });
      }
    }
    return value;
  };

  const handleRefresh = () => {
    setRefreshing(true);
    fetchDashboardData();
  };

  const filterDataByDateRange = (sourceData) => {
    if (dateRange === 'all' || !data?.metadata?.date_columns?.length) {
      return sourceData;
    }
    
    // This is a placeholder - implement actual date filtering based on your data structure
    return sourceData;
  };

  // Prepare chart data
  const prepareDistributionData = () => {
    if (!selectedDimension || !data?.summary_stats?.categorical_stats?.[selectedDimension]) {
      return [];
    }
    return data.summary_stats.categorical_stats[selectedDimension].top_values || [];
  };

  const prepareTrendData = () => {
    if (!data?.sample_data || data.sample_data.length === 0) return [];
    
    // Simple trend - take first 20 records
    return data.sample_data.slice(0, 20).map((row, index) => ({
      index: index + 1,
      value: parseFloat(row[selectedMetric]) || 0
    }));
  };

  const prepareBarChartData = () => {
    if (!selectedDimension || !selectedMetric || !data?.sample_data) return [];
    
    // Aggregate data by dimension
    const aggregated = {};
    data.sample_data.forEach(row => {
      const key = row[selectedDimension] || 'Unknown';
      if (!aggregated[key]) {
        aggregated[key] = { sum: 0, count: 0 };
      }
      aggregated[key].sum += parseFloat(row[selectedMetric]) || 0;
      aggregated[key].count += 1;
    });
    
    return Object.entries(aggregated)
      .map(([key, val]) => ({
        name: key,
        value: val.sum / val.count
      }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 10);
  };

  const prepareScatterData = () => {
    if (!data?.metadata?.numeric_columns || data.metadata.numeric_columns.length < 2) return [];
    if (!data?.sample_data) return [];
    
    const xCol = selectedMetric || data.metadata.numeric_columns[0];
    const yCol = data.metadata.numeric_columns.find(col => col !== xCol) || data.metadata.numeric_columns[1];
    
    return data.sample_data.slice(0, 100).map(row => ({
      x: parseFloat(row[xCol]) || 0,
      y: parseFloat(row[yCol]) || 0
    }));
  };

  if (loading) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100%',
        flexDirection: 'column',
        gap: '1rem'
      }}>
        <RefreshCw size={32} className="animate-spin" color={COLORS.primary} />
        <span style={{ color: '#6b7280', fontSize: '0.875rem' }}>Loading analytics...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100%',
        flexDirection: 'column',
        gap: '1rem',
        padding: '2rem'
      }}>
        <AlertCircle size={48} color={COLORS.danger} />
        <span style={{ color: COLORS.danger, fontSize: '1rem' }}>{error}</span>
        <button
          onClick={fetchDashboardData}
          style={{
            ...styles.sendButton,
            marginTop: '1rem'
          }}
        >
          Try Again
        </button>
      </div>
    );
  }

  return (
    <div style={{ 
      height: '100%', 
      overflowY: 'auto',
      backgroundColor: '#fafbfc'
    }}>
      {/* Controls Section */}
      <div style={{
        ...styles.visualizationCard,
        margin: '1.5rem',
        marginBottom: '1rem'
      }}>
        <div style={{ 
          display: 'flex', 
          gap: '1rem', 
          alignItems: 'center',
          flexWrap: 'wrap'
        }}>
          <div style={{ flex: '1', minWidth: '200px' }}>
            <label style={{
              display: 'block',
              fontSize: '0.75rem',
              color: '#6b7280',
              marginBottom: '0.25rem',
              textTransform: 'uppercase',
              letterSpacing: '0.05em'
            }}>
              Dimension
            </label>
            <select
              value={selectedDimension}
              onChange={(e) => setSelectedDimension(e.target.value)}
              style={{
                width: '100%',
                padding: '0.5rem',
                borderRadius: '6px',
                border: '1px solid #e5e7eb',
                fontSize: '0.875rem',
                backgroundColor: 'white',
                cursor: 'pointer'
              }}
            >
              <option value="">Select dimension...</option>
              {data?.metadata?.categorical_columns?.map(col => (
                <option key={col} value={col}>{formatColumnName(col)}</option>
              ))}
            </select>
          </div>
          
          <div style={{ flex: '1', minWidth: '200px' }}>
            <label style={{
              display: 'block',
              fontSize: '0.75rem',
              color: '#6b7280',
              marginBottom: '0.25rem',
              textTransform: 'uppercase',
              letterSpacing: '0.05em'
            }}>
              Metric
            </label>
            <select
              value={selectedMetric}
              onChange={(e) => setSelectedMetric(e.target.value)}
              style={{
                width: '100%',
                padding: '0.5rem',
                borderRadius: '6px',
                border: '1px solid #e5e7eb',
                fontSize: '0.875rem',
                backgroundColor: 'white',
                cursor: 'pointer'
              }}
            >
              <option value="">Select metric...</option>
              {data?.metadata?.numeric_columns?.map(col => (
                <option key={col} value={col}>{formatColumnName(col)}</option>
              ))}
            </select>
          </div>
          
          <div style={{ flex: '1', minWidth: '200px' }}>
            <label style={{
              display: 'block',
              fontSize: '0.75rem',
              color: '#6b7280',
              marginBottom: '0.25rem',
              textTransform: 'uppercase',
              letterSpacing: '0.05em'
            }}>
              Date Range
            </label>
            <select
              value={dateRange}
              onChange={(e) => setDateRange(e.target.value)}
              style={{
                width: '100%',
                padding: '0.5rem',
                borderRadius: '6px',
                border: '1px solid #e5e7eb',
                fontSize: '0.875rem',
                backgroundColor: 'white',
                cursor: 'pointer'
              }}
            >
              <option value="all">All Time</option>
              <option value="30">Last 30 Days</option>
              <option value="90">Last 90 Days</option>
              <option value="365">Last Year</option>
            </select>
          </div>
          
          <button
            onClick={handleRefresh}
            style={{
              ...styles.iconButton,
              backgroundColor: COLORS.primary,
              color: 'white',
              border: 'none',
              padding: '0.5rem 1rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              marginTop: '1.25rem'
            }}
            disabled={refreshing}
          >
            <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* Metrics Grid */}
      <div style={{ 
        ...styles.kpiGrid,
        margin: '1.5rem',
        marginTop: '1rem'
      }}>
        {data?.summary_stats?.total_records && (
          <KPICard
            label="Total Records"
            value={data.summary_stats.total_records}
            icon={Database}
            color={COLORS.primary}
          />
        )}
        
        {Object.entries(data?.summary_stats?.numeric_stats || {}).slice(0, 3).map(([col, stats]) => (
          <React.Fragment key={col}>
            <KPICard
              label={`Total ${formatColumnName(col)}`}
              value={formatValue(stats.sum)}
              icon={DollarSign}
              color={COLORS.success}
              trend={stats.trend}
            />
            <KPICard
              label={`Average ${formatColumnName(col)}`}
              value={formatValue(stats.mean)}
              icon={TrendingUp}
              color={COLORS.secondary}
            />
          </React.Fragment>
        ))}
      </div>

      {/* Insights Panel */}
      {insights.length > 0 && (
        <div style={{
          margin: '1.5rem',
          padding: '1.5rem',
          backgroundColor: 'white',
          borderRadius: '12px',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
        }}>
          <h3 style={{
            fontSize: '1rem',
            fontWeight: '600',
            marginBottom: '1rem',
            color: COLORS.dark,
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem'
          }}>
            <Activity size={18} color={COLORS.primary} />
            Key Insights
          </h3>
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
            gap: '1rem'
          }}>
            {insights.map((insight, index) => (
              <div key={index} style={{
                padding: '1rem',
                backgroundColor: '#f9fafb',
                borderRadius: '8px',
                borderLeft: `4px solid ${COLORS.primary}`,
                display: 'flex',
                gap: '1rem',
                alignItems: 'flex-start'
              }}>
                <span style={{ fontSize: '1.5rem' }}>{insight.icon}</span>
                <div>
                  <div style={{
                    fontSize: '0.875rem',
                    fontWeight: '600',
                    color: COLORS.dark,
                    marginBottom: '0.25rem'
                  }}>
                    {insight.title}
                  </div>
                  <div style={{
                    fontSize: '0.813rem',
                    color: '#4b5563',
                    marginBottom: '0.25rem'
                  }}>
                    {insight.value}
                  </div>
                  {insight.detail && (
                    <div style={{
                      fontSize: '0.75rem',
                      color: '#6b7280'
                    }}>
                      {insight.detail}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Charts Grid */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(500px, 1fr))',
        gap: '1.5rem',
        margin: '1.5rem'
      }}>
        {/* Distribution Chart */}
        <div style={styles.visualizationCard}>
          <div style={styles.cardHeader}>
            <h3 style={styles.cardTitle}>
              <PieChartIcon size={18} style={{ marginRight: '0.5rem', display: 'inline' }} />
              Distribution Analysis
            </h3>
          </div>
          <div style={{ height: '300px', padding: '1rem' }}>
            {prepareDistributionData().length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={prepareDistributionData()}
                    dataKey="count"
                    nameKey="value"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label
                  >
                    {prepareDistributionData().map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS.chartColors[index % COLORS.chartColors.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#6b7280'
              }}>
                Select a dimension to view distribution
              </div>
            )}
          </div>
        </div>

        {/* Trend Chart */}
        <div style={styles.visualizationCard}>
          <div style={styles.cardHeader}>
            <h3 style={styles.cardTitle}>
              <TrendingUp size={18} style={{ marginRight: '0.5rem', display: 'inline' }} />
              Trend Analysis
            </h3>
          </div>
          <div style={{ height: '300px', padding: '1rem' }}>
            {prepareTrendData().length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={prepareTrendData()}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                  <XAxis dataKey="index" stroke="#6b7280" />
                  <YAxis stroke="#6b7280" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'rgba(255,255,255,0.95)',
                      border: '1px solid #e5e7eb',
                      borderRadius: '8px'
                    }}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="value" 
                    stroke={COLORS.primary}
                    strokeWidth={2}
                    dot={{ fill: COLORS.primary, r: 4 }}
                    activeDot={{ r: 6 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#6b7280'
              }}>
                Select a metric to view trends
              </div>
            )}
          </div>
        </div>

        {/* Bar Chart */}
        <div style={styles.visualizationCard}>
          <div style={styles.cardHeader}>
            <h3 style={styles.cardTitle}>
              <BarChart2 size={18} style={{ marginRight: '0.5rem', display: 'inline' }} />
              Top 10 by {formatColumnName(selectedMetric || 'Metric')}
            </h3>
          </div>
          <div style={{ height: '300px', padding: '1rem' }}>
            {prepareBarChartData().length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={prepareBarChartData()}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                  <XAxis 
                    dataKey="name" 
                    stroke="#6b7280"
                    angle={-45}
                    textAnchor="end"
                    height={80}
                  />
                  <YAxis stroke="#6b7280" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'rgba(255,255,255,0.95)',
                      border: '1px solid #e5e7eb',
                      borderRadius: '8px'
                    }}
                    formatter={(value) => formatNumber(value)}
                  />
                  <Bar dataKey="value" fill={COLORS.primary} radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#6b7280'
              }}>
                Select dimension and metric to view data
              </div>
            )}
          </div>
        </div>

        {/* Scatter Plot */}
        <div style={styles.visualizationCard}>
          <div style={styles.cardHeader}>
            <h3 style={styles.cardTitle}>
              <Activity size={18} style={{ marginRight: '0.5rem', display: 'inline' }} />
              Correlation Analysis
            </h3>
          </div>
          <div style={{ height: '300px', padding: '1rem' }}>
            {data?.metadata?.numeric_columns?.length >= 2 ? (
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                  <XAxis 
                    type="number" 
                    dataKey="x" 
                    name={selectedMetric || data.metadata.numeric_columns[0]}
                    stroke="#6b7280"
                  />
                  <YAxis 
                    type="number" 
                    dataKey="y" 
                    name={data.metadata.numeric_columns.find(col => col !== selectedMetric) || data.metadata.numeric_columns[1]}
                    stroke="#6b7280"
                  />
                  <Tooltip 
                    cursor={{ strokeDasharray: '3 3' }}
                    contentStyle={{ 
                      backgroundColor: 'rgba(255,255,255,0.95)',
                      border: '1px solid #e5e7eb',
                      borderRadius: '8px'
                    }}
                  />
                  <Scatter 
                    name="Data Points" 
                    data={prepareScatterData()} 
                    fill={COLORS.secondary}
                    opacity={0.6}
                  />
                </ScatterChart>
              </ResponsiveContainer>
            ) : (
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#6b7280'
              }}>
                Insufficient numeric columns for correlation analysis
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Summary Stats */}
      <div style={{
        margin: '1.5rem',
        padding: '1rem',
        backgroundColor: 'white',
        borderRadius: '8px',
        border: '1px solid #e5e7eb',
        fontSize: '0.813rem',
        color: '#6b7280',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <span>
          <Database size={14} style={{ marginRight: '0.5rem', display: 'inline' }} />
          Analyzing {data?.summary_stats?.total_records?.toLocaleString() || 0} records
        </span>
        <span>
          {data?.metadata?.total_columns} columns • 
          {data?.metadata?.numeric_columns?.length} numeric • 
          {data?.metadata?.categorical_columns?.length} categorical
        </span>
      </div>
    </div>
  );
};

// KPI Card Component (reused from main dashboard)
const KPICard = ({ label, value, icon: Icon, color, trend }) => {
  const getTrendIcon = () => {
    if (!trend) return null;
    return trend > 0 ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />;
  };

  const getTrendColor = () => {
    if (!trend) return '#6b7280';
    return trend > 0 ? '#059669' : '#dc2626';
  };

  return (
    <div style={{
      backgroundColor: '#f9fafb',
      borderRadius: '8px',
      padding: '1rem',
      border: '1px solid #e5e7eb',
      transition: 'all 0.2s',
      cursor: 'pointer'
    }}
    onMouseEnter={(e) => {
      e.currentTarget.style.transform = 'translateY(-2px)';
      e.currentTarget.style.boxShadow = '0 4px 6px rgba(0,0,0,0.1)';
    }}
    onMouseLeave={(e) => {
      e.currentTarget.style.transform = 'translateY(0)';
      e.currentTarget.style.boxShadow = 'none';
    }}>
      <div style={{
        width: '2.5rem',
        height: '2.5rem',
        borderRadius: '8px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        marginBottom: '0.5rem',
        backgroundColor: `${color}15`
      }}>
        <Icon size={24} color={color} />
      </div>
      <div style={{
        fontSize: '0.75rem',
        color: '#6b7280',
        marginBottom: '0.25rem',
        textTransform: 'uppercase',
        letterSpacing: '0.05em'
      }}>
        {label}
      </div>
      <div style={{
        fontSize: '1.5rem',
        fontWeight: '700',
        lineHeight: 1,
        color: color
      }}>
        {value}
      </div>
      {trend !== undefined && (
        <div style={{
          fontSize: '0.75rem',
          display: 'flex',
          alignItems: 'center',
          gap: '0.25rem',
          marginTop: '0.5rem',
          color: getTrendColor()
        }}>
          {getTrendIcon()}
          <span>{Math.abs(trend).toFixed(1)}% vs last period</span>
        </div>
      )}
    </div>
  );
};

export default AnalyticsDashboard;