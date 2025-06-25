import React, { useState, useEffect, useRef, useCallback } from 'react';
import { LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, Treemap, Funnel, FunnelChart, LabelList } from 'recharts';
import { Send, Download, Maximize2, Minimize2, TrendingUp, Users, DollarSign, Target, AlertCircle, CheckCircle, Clock, ChevronDown, ChevronUp, FileText, BarChart2, Activity, Filter, Calendar, RefreshCw, Loader, Trash2, X, Database, Brain, Zap, Eye, Code, Info, PlayCircle, PauseCircle, SkipForward, Hash, ArrowUpRight, ArrowDownRight } from 'lucide-react';
import AnalyticsDashboard from './AnalyticsDashboard';
// API Configuration
const API_BASE_URL = 'http://localhost:8083';

// Professional color palette
const COLORS = {
  primary: '#1e40af',
  secondary: '#7c3aed',
  success: '#059669',
  warning: '#d97706',
  danger: '#dc2626',
  dark: '#1f2937',
  light: '#f3f4f6',
  white: '#ffffff',
  chartColors: ['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444', '#6366f1', '#14b8a6', '#f97316']
};

// Styled components using inline styles for enterprise look
const styles = {
  container: {
    minHeight: '100vh',
    backgroundColor: '#f8fafc',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
  },
  header: {
    backgroundColor: COLORS.white,
    borderBottom: '1px solid #e5e7eb',
    padding: '1rem 2rem',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
  },
  headerContent: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    maxWidth: '1400px',
    margin: '0 auto'
  },
  logo: {
    fontSize: '1.5rem',
    fontWeight: '700',
    color: COLORS.dark,
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem'
  },
  mainContent: {
    maxWidth: '1400px',
    margin: '0 auto',
    padding: '2rem',
    height: 'calc(100vh - 80px)'
  },
  chatSection: {
    backgroundColor: COLORS.white,
    borderRadius: '12px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    height: '100%'
  },
  chatHeader: {
    padding: '1.5rem',
    borderBottom: '1px solid #e5e7eb',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  chatTitle: {
    fontSize: '1.25rem',
    fontWeight: '600',
    color: COLORS.dark
  },
  chatBody: {
    flex: 1,
    overflowY: 'auto',
    padding: '1.5rem',
    backgroundColor: '#fafbfc'
  },
  messageContainer: {
    marginBottom: '1.5rem'
  },
  userMessage: {
    backgroundColor: COLORS.primary,
    color: COLORS.white,
    padding: '1rem 1.25rem',
    borderRadius: '12px 12px 4px 12px',
    marginLeft: 'auto',
    maxWidth: '80%',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
  },
  botMessage: {
    backgroundColor: COLORS.white,
    color: COLORS.dark,
    padding: '1.25rem',
    borderRadius: '4px 12px 12px 12px',
    maxWidth: '90%',
    boxShadow: '0 2px 4px rgba(0,0,0,0.05)',
    border: '1px solid #e5e7eb'
  },
  inputSection: {
    padding: '1.5rem',
    borderTop: '1px solid #e5e7eb',
    backgroundColor: COLORS.white
  },
  inputContainer: {
    display: 'flex',
    gap: '0.75rem'
  },
  input: {
    flex: 1,
    padding: '0.75rem 1rem',
    borderRadius: '8px',
    border: '1px solid #d1d5db',
    fontSize: '0.95rem',
    outline: 'none',
    transition: 'border-color 0.2s',
    fontFamily: 'inherit'
  },
  sendButton: {
    padding: '0.75rem 1.5rem',
    backgroundColor: COLORS.primary,
    color: COLORS.white,
    border: 'none',
    borderRadius: '8px',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    fontSize: '0.95rem',
    fontWeight: '500',
    transition: 'all 0.2s',
    fontFamily: 'inherit'
  },
  visualizationCard: {
    backgroundColor: COLORS.white,
    borderRadius: '12px',
    padding: '1.5rem',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    marginBottom: '1.5rem'
  },
  cardHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '1rem'
  },
  cardTitle: {
    fontSize: '1.125rem',
    fontWeight: '600',
    color: COLORS.dark
  },
  cardActions: {
    display: 'flex',
    gap: '0.5rem'
  },
  iconButton: {
    padding: '0.5rem',
    backgroundColor: 'transparent',
    border: '1px solid #e5e7eb',
    borderRadius: '6px',
    cursor: 'pointer',
    transition: 'all 0.2s',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center'
  },
  badge: {
    padding: '0.25rem 0.75rem',
    borderRadius: '9999px',
    fontSize: '0.75rem',
    fontWeight: '500'
  },
  loader: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    color: '#6b7280',
    fontSize: '0.875rem'
  },
  executionTimeline: {
    backgroundColor: '#f9fafb',
    borderRadius: '8px',
    padding: '1rem',
    marginBottom: '1rem',
    border: '1px solid #e5e7eb'
  },
  timelineItem: {
    display: 'flex',
    alignItems: 'flex-start',
    gap: '1rem',
    marginBottom: '0.75rem',
    position: 'relative',
    paddingLeft: '2rem'
  },
  timelineDot: {
    position: 'absolute',
    left: '0.5rem',
    top: '0.25rem',
    width: '0.75rem',
    height: '0.75rem',
    borderRadius: '50%',
    border: '2px solid white',
    boxShadow: '0 0 0 1px rgba(0,0,0,0.1)'
  },
  timelineContent: {
    flex: 1
  },
  timelinePhase: {
    fontWeight: '600',
    fontSize: '0.875rem',
    marginBottom: '0.25rem',
    textTransform: 'capitalize'
  },
  timelineDetail: {
    fontSize: '0.813rem',
    color: '#6b7280',
    lineHeight: '1.4'
  },
  kpiGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
    gap: '1rem',
    marginBottom: '1.5rem'
  },
  kpiCard: {
    backgroundColor: '#f9fafb',
    borderRadius: '8px',
    padding: '1rem',
    border: '1px solid #e5e7eb',
    transition: 'all 0.2s',
    cursor: 'pointer'
  },
  kpiIcon: {
    width: '2.5rem',
    height: '2.5rem',
    borderRadius: '8px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    marginBottom: '0.5rem'
  },
  kpiLabel: {
    fontSize: '0.75rem',
    color: '#6b7280',
    marginBottom: '0.25rem',
    textTransform: 'uppercase',
    letterSpacing: '0.05em'
  },
  kpiValue: {
    fontSize: '1.5rem',
    fontWeight: '700',
    lineHeight: 1
  },
  kpiChange: {
    fontSize: '0.75rem',
    display: 'flex',
    alignItems: 'center',
    gap: '0.25rem',
    marginTop: '0.5rem'
  },
  evidenceTrail: {
    backgroundColor: '#fafbfc',
    borderRadius: '8px',
    padding: '1rem',
    marginTop: '1rem',
    border: '1px solid #f3f4f6'
  },
  evidenceItem: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '0.5rem',
    padding: '0.375rem 0.75rem',
    backgroundColor: '#eff6ff',
    borderRadius: '6px',
    fontSize: '0.813rem',
    margin: '0.25rem',
    cursor: 'pointer',
    transition: 'all 0.2s',
    border: '1px solid #dbeafe'
  },
  queryDebugger: {
    backgroundColor: '#1f2937',
    color: '#f3f4f6',
    borderRadius: '8px',
    padding: '1rem',
    fontFamily: 'monospace',
    fontSize: '0.813rem',
    overflow: 'auto',
    maxHeight: '200px',
    marginTop: '0.5rem'
  },
  interactiveTable: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '0.875rem'
  },
  tableHeader: {
    backgroundColor: '#f9fafb',
    borderBottom: '2px solid #e5e7eb',
    position: 'sticky',
    top: 0,
    zIndex: 1
  },
  tableHeaderCell: {
    padding: '0.75rem',
    textAlign: 'left',
    fontWeight: '600',
    cursor: 'pointer',
    userSelect: 'none',
    transition: 'background-color 0.2s'
  },
  tableRow: {
    borderBottom: '1px solid #f3f4f6',
    transition: 'background-color 0.2s'
  },
  tableCell: {
    padding: '0.75rem'
  },
  actionButton: {
    padding: '0.375rem 0.75rem',
    backgroundColor: COLORS.primary,
    color: COLORS.white,
    border: 'none',
    borderRadius: '6px',
    fontSize: '0.75rem',
    cursor: 'pointer',
    transition: 'all 0.2s'
  },
  streamingIndicator: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '0.5rem',
    padding: '0.5rem 1rem',
    backgroundColor: '#eff6ff',
    borderRadius: '6px',
    fontSize: '0.813rem',
    color: COLORS.primary,
    marginBottom: '1rem'
  }
};

// Helper function to format numerical values
const formatNumber = (value) => {
  if (value === null || value === undefined) return value;
  
  // Check if the value is a string that looks like a number
  const numValue = typeof value === 'string' ? parseFloat(value) : value;
  
  if (typeof numValue === 'number' && !isNaN(numValue)) {
    // Check if it's an integer or needs rounding
    if (Number.isInteger(numValue)) {
      return numValue.toLocaleString();
    } else {
      // Round to 2 decimal places
      return parseFloat(numValue.toFixed(2)).toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2
      });
    }
  }
  return value;
};

// PDF Export utility function
const exportToPDF = async (message, question) => {
  // Create a new window for the PDF content
  const printWindow = window.open('', '_blank');
  
  // Function to convert canvas to image with proper scaling
  const captureChartAsImage = async (vizContent) => {
    return new Promise((resolve) => {
      // Create a temporary container
      const tempDiv = document.createElement('div');
      tempDiv.style.position = 'absolute';
      tempDiv.style.left = '-9999px';
      tempDiv.style.width = '1200px';  // Larger width for better rendering
      tempDiv.style.height = '600px';   // Larger height
      document.body.appendChild(tempDiv);
      
      // Create a temporary iframe to render the chart
      const tempIframe = document.createElement('iframe');
      tempIframe.style.width = '100%';
      tempIframe.style.height = '100%';
      tempDiv.appendChild(tempIframe);
      
      const iframeDoc = tempIframe.contentDocument || tempIframe.contentWindow.document;
      
      const tempHtml = `
        <!DOCTYPE html>
        <html>
        <head>
          <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
          <style>
            body { margin: 0; padding: 20px; background: white; }
            .chart-container { 
              position: relative; 
              width: 1160px;
              height: 560px;
              margin: 0 auto;
            }
            canvas { 
              width: 100% !important;
              height: 100% !important;
            }
          </style>
        </head>
        <body>
          <div class="chart-container">
            ${vizContent}
          </div>
          <script>
            window.addEventListener('load', function() {
              // Wait for chart to render
              setTimeout(function() {
                // Find the canvas element
                const canvas = document.querySelector('canvas');
                if (canvas && canvas.toDataURL) {
                  // Get the chart instance
                  const chart = Chart.getChart(canvas);
                  if (chart) {
                    // Update chart options for better PDF rendering
                    chart.options.responsive = true;
                    chart.options.maintainAspectRatio = false;
                    chart.options.devicePixelRatio = 2; // Higher quality
                    
                    // Update bar chart specific options if it's a bar chart
                    if (chart.config.type === 'bar' && chart.options.scales) {
                      // Ensure proper bar spacing
                      if (chart.options.scales.x) {
                        chart.options.scales.x.grid = { display: false };
                      }
                      if (chart.options.scales.y) {
                        chart.options.scales.y.beginAtZero = true;
                        chart.options.scales.y.grid = { color: 'rgba(0,0,0,0.1)' };
                      }
                    }
                    
                    // Force chart resize and update
                    chart.resize(1160, 560);
                    chart.update('none');
                  }
                  
                  // Wait for the resize to take effect
                  setTimeout(function() {
                    // Create high-quality image
                    const imageData = canvas.toDataURL('image/png', 1.0);
                    
                    window.parent.postMessage({
                      type: 'chartImage',
                      data: imageData
                    }, '*');
                  }, 500);
                } else {
                  // If no canvas found, send null
                  window.parent.postMessage({
                    type: 'chartImage',
                    data: null
                  }, '*');
                }
              }, 1500); // Increased wait time for chart rendering
            });
          </script>
        </body>
        </html>
      `;
      
      // Listen for the chart image
      const messageHandler = (event) => {
        if (event.data && event.data.type === 'chartImage') {
          window.removeEventListener('message', messageHandler);
          document.body.removeChild(tempDiv);
          resolve(event.data.data);
        }
      };
      
      window.addEventListener('message', messageHandler);
      
      iframeDoc.open();
      iframeDoc.write(tempHtml);
      iframeDoc.close();
      
      // Timeout fallback
      setTimeout(() => {
        window.removeEventListener('message', messageHandler);
        document.body.removeChild(tempDiv);
        resolve(null);
      }, 4000);
    });
  };
  
  // Function to extract table HTML with rounded numbers
  const extractTableHTML = (vizContent) => {
    // Extract table content from the visualization
    const tableMatch = vizContent.match(/<table[^>]*>[\s\S]*?<\/table>/i);
    if (tableMatch) {
      let tableHTML = tableMatch[0];
      
      // Process table cells to round numerical values
      tableHTML = tableHTML.replace(/<td[^>]*>([^<]+)<\/td>/gi, (match, content) => {
        const trimmed = content.trim();
        const num = parseFloat(trimmed);
        if (!isNaN(num) && trimmed.match(/^\-?\d*\.?\d+$/)) {
          return match.replace(content, formatNumber(num));
        }
        return match;
      });
      
      return tableHTML;
    }
    return null;
  };
  
  // Process visualizations - capture images for charts, extract HTML for tables
  let processedVisualizations = [];
  if (message.visualizations && message.visualizations.length > 0) {
    for (const viz of message.visualizations) {
      const isTable = viz.content.includes('<table') && !viz.content.includes('<canvas');
      
      if (isTable) {
        const tableHTML = extractTableHTML(viz.content);
        processedVisualizations.push({
          type: 'table',
          title: viz.title,
          tableHTML: tableHTML
        });
      } else {
        const chartImage = await captureChartAsImage(viz.content);
        processedVisualizations.push({
          type: 'chart',
          title: viz.title,
          image: chartImage
        });
      }
    }
  }
  
  // Generate HTML content for PDF
  const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <title>CRO Analytics Report</title>
      <style>
        body {
          font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
          line-height: 1.6;
          color: #1f2937;
          max-width: 800px;
          margin: 0 auto;
          padding: 40px;
        }
        .header {
          border-bottom: 2px solid #e5e7eb;
          padding-bottom: 20px;
          margin-bottom: 20px;
        }
        .logo {
          font-size: 24px;
          font-weight: 700;
          color: #1e40af;
          margin-bottom: 10px;
        }
        .timestamp {
          color: #6b7280;
          font-size: 14px;
        }
        .question {
          background-color: #eff6ff;
          border-left: 4px solid #1e40af;
          padding: 15px;
          margin: 15px 0 20px 0;
          font-weight: 500;
        }
        .headline {
          font-size: 22px;
          font-weight: 700;
          color: #1e40af;
          margin-bottom: 10px;
          line-height: 1.3;
        }
        .content {
          margin-top: 5px;
        }
        .section {
          margin: 20px 0;
        }
        .section-title {
          font-size: 18px;
          font-weight: 600;
          color: #1f2937;
          margin-bottom: 10px;
        }
        .kpi-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: 15px;
          margin: 20px 0;
        }
        .kpi-card {
          background-color: #f9fafb;
          border: 1px solid #e5e7eb;
          border-radius: 8px;
          padding: 15px;
        }
        .kpi-label {
          font-size: 12px;
          color: #6b7280;
          text-transform: uppercase;
          letter-spacing: 0.05em;
        }
        .kpi-value {
          font-size: 24px;
          font-weight: 700;
          color: #1e40af;
          margin: 5px 0;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          margin: 15px 0;
          font-size: 0.875rem;
        }
        th {
          background-color: #f9fafb;
          border-bottom: 2px solid #e5e7eb;
          padding: 10px;
          text-align: left;
          font-weight: 600;
        }
        td {
          border-bottom: 1px solid #f3f4f6;
          padding: 10px;
        }
        .timeline {
          background-color: #f9fafb;
          border-radius: 8px;
          padding: 15px;
          margin: 15px 0;
        }
        .timeline-item {
          margin-bottom: 10px;
          padding-left: 20px;
          position: relative;
        }
        .timeline-dot {
          position: absolute;
          left: 0;
          top: 6px;
          width: 8px;
          height: 8px;
          background-color: #1e40af;
          border-radius: 50%;
        }
        .metadata {
          margin-top: 30px;
          padding-top: 20px;
          border-top: 1px solid #e5e7eb;
          font-size: 12px;
          color: #6b7280;
        }
        ul, ol {
          margin: 10px 0;
          padding-left: 30px;
        }
        li {
          margin-bottom: 8px;
        }
        strong {
          font-weight: 600;
        }
        h1, h2, h3 {
          margin: 20px 0 10px 0;
          font-weight: 600;
        }
        pre {
          background-color: #f3f4f6;
          padding: 10px;
          border-radius: 4px;
          overflow-x: auto;
          font-family: monospace;
          font-size: 12px;
        }
        code {
          background-color: #f3f4f6;
          padding: 2px 4px;
          border-radius: 3px;
          font-family: monospace;
          font-size: 12px;
        }
        .viz-section {
          margin: 20px 0;
          page-break-inside: avoid;
        }
        .viz-item {
          background: white;
          border: 1px solid #e5e7eb;
          border-radius: 8px;
          padding: 20px;
          margin-bottom: 25px;
          page-break-inside: avoid;
        }
        .viz-title {
          font-size: 18px;
          font-weight: 600;
          margin-bottom: 15px;
          color: #1f2937;
        }
        .chart-wrapper {
          display: flex;
          justify-content: center;
          align-items: center;
          background-color: #f9fafb;
          padding: 20px;
          border-radius: 8px;
        }
        .chart-image {
          max-width: 100%;
          width: 100%;
          height: auto;
          display: block;
        }
        .data-table {
          overflow-x: auto;
        }
        .data-table table {
          margin: 0;
          font-size: 0.813rem;
        }
        @media print {
          body {
            padding: 20px;
          }
          .header {
            page-break-after: avoid;
          }
          .section {
            page-break-inside: avoid;
          }
          table {
            page-break-inside: avoid;
          }
          .viz-section {
            page-break-inside: avoid;
          }
          .viz-pair {
            page-break-inside: avoid;
          }
        }
      </style>
    </head>
    <body>
      <div class="header">
        <div class="logo">CRO Analytics Report</div>
        <div class="timestamp">Generated on ${new Date().toLocaleString()}</div>
      </div>
      
      <div class="question">
        <strong>Question:</strong> ${question}
      </div>
      
      ${renderBlocksForPDF(message.blocks)}
      
      ${message.content && (!message.blocks || message.blocks.length === 0) ? `
      <div class="content">
        ${convertMarkdownToHTML(message.content)}
      </div>
      ` : ''}
      
      ${processedVisualizations.length > 0 ? `
      <div class="section">
        <div class="section-title">Visualizations</div>
        ${processedVisualizations.map((viz, index) => {
          if (viz.type === 'table') {
            return `
              <div class="viz-item" style="margin-bottom: 30px;">
                <div class="viz-title">${viz.title || 'Data Table'}</div>
                ${viz.tableHTML ? `
                  <div class="data-table">
                    ${viz.tableHTML}
                  </div>
                ` : '<div style="padding: 40px; text-align: center; color: #6b7280;">Table data</div>'}
              </div>
            `;
          } else {
            return `
              <div class="viz-item" style="margin-bottom: 30px; page-break-inside: avoid;">
                <div class="viz-title">${viz.title || 'Chart'}</div>
                ${viz.image ? `
                  <div class="chart-wrapper" style="background: #f9fafb; padding: 20px; border-radius: 8px;">
                    <img src="${viz.image}" alt="${viz.title}" style="width: 100%; height: auto; max-width: 700px; margin: 0 auto; display: block;" />
                  </div>
                ` : '<div style="padding: 40px; text-align: center; color: #6b7280;">Chart visualization</div>'}
              </div>
            `;
          }
        }).join('')}
      </div>
      ` : ''}
      
      ${message.dataSources ? `
      <div class="section">
        <div class="section-title">Data Sources</div>
        <div style="background-color: #f9fafb; padding: 15px; border-radius: 8px; border: 1px solid #e5e7eb;">
          ${message.dataSources.sources && message.dataSources.sources.length > 0 ? `
          <div style="margin-bottom: 10px;">
            <strong>Source Files:</strong>
            <div style="margin-top: 5px;">
              ${message.dataSources.sources.map(source => `
                <span style="
                  display: inline-block;
                  padding: 4px 12px;
                  margin: 2px 4px;
                  background-color: #eff6ff;
                  border: 1px solid #dbeafe;
                  border-radius: 6px;
                  font-size: 0.813rem;
                  font-family: monospace;
                ">
                  ${source}
                </span>
              `).join('')}
            </div>
          </div>
          ` : ''}
          
          ${message.dataSources.source_details && message.dataSources.source_details.length > 0 ? `
          <div style="margin-top: 10px;">
            <strong>Query Details:</strong>
            <table style="margin-top: 5px; font-size: 0.813rem;">
              <thead>
                <tr>
                  <th style="text-align: left; padding: 8px;">Tool</th>
                  <th style="text-align: left; padding: 8px;">Tables</th>
                  <th style="text-align: left; padding: 8px;">Records</th>
                  <th style="text-align: left; padding: 8px;">Type</th>
                </tr>
              </thead>
              <tbody>
                ${message.dataSources.source_details.map(detail => `
                  <tr>
                    <td style="padding: 8px;">${detail.tool}</td>
                    <td style="padding: 8px;">${detail.tables.join(', ')}</td>
                    <td style="padding: 8px;">${detail.record_count}</td>
                    <td style="padding: 8px;">
                      <span style="
                        padding: 2px 6px;
                        background-color: ${detail.query_type === 'SQL' ? '#dbeafe' : '#e9d5ff'};
                        border-radius: 4px;
                        font-size: 0.75rem;
                      ">
                        ${detail.query_type}
                      </span>
                    </td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
          ` : ''}
          
          ${message.dataSources.total_records_analyzed ? `
          <div style="
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid #e5e7eb;
            font-size: 0.813rem;
            display: flex;
            justify-content: space-between;
          ">
            <span><strong>Total Records Analyzed:</strong> ${message.dataSources.total_records_analyzed.toLocaleString()}</span>
            ${message.dataSources.query_types ? `
              <span><strong>Query Types:</strong> ${message.dataSources.query_types.join(', ')}</span>
            ` : ''}
          </div>
          ` : ''}
        </div>
      </div>
      ` : ''}
      
      ${message.reasoning_steps && message.reasoning_steps.length > 0 ? `
      <div class="section">
        <div class="section-title">Reasoning Process</div>
        <ol>
          ${message.reasoning_steps.map(step => `<li>${step}</li>`).join('')}
        </ol>
      </div>
      ` : ''}
      
      ${message.traces?.execution_history && message.traces.execution_history.length > 0 ? `
      <div class="section">
        <div class="section-title">Execution Timeline</div>
        <div class="timeline">
          ${message.traces.execution_history.map(step => `
            <div class="timeline-item">
              <div class="timeline-dot"></div>
              <strong>${step.phase}</strong>
              ${step.tool ? `<br>Tool: ${step.tool} • ${step.result_count || 0} results • ${step.duration?.toFixed(2)}s` : ''}
              ${step.plan ? `<br>${step.plan.reasoning}` : ''}
            </div>
          `).join('')}
        </div>
      </div>
      ` : ''}
      
      <div class="metadata">
        <p>
          Session ID: ${message.sessionId}<br>
          Processing Time: ${message.metadata?.total_execution_time?.toFixed(2)}s<br>
          Iterations: ${message.metadata?.iterations}
        </p>
      </div>
    </body>
    </html>
  `;
  
  // Write content to new window
  printWindow.document.write(htmlContent);
  printWindow.document.close();
  
  // Trigger print dialog after content loads
  printWindow.onload = () => {
    printWindow.print();
  };
};

// Helper function to render blocks for PDF
const renderBlocksForPDF = (blocks) => {
  if (!blocks || blocks.length === 0) return '';
  
  let htmlContent = '';
  let isFirstBlock = true;
  
  blocks.forEach(block => {
    switch (block.type) {
      case 'headline':
        const cleanHeadline = block.text.replace(/\*\*/g, '');
        htmlContent += `<div class="headline">${cleanHeadline}</div>`;
        break;
        
      case 'markdown':
        // For first markdown block after headline, don't add extra spacing
        if (isFirstBlock) {
          htmlContent += `<div class="content">${convertMarkdownToHTML(block.content)}</div>`;
          isFirstBlock = false;
        } else {
          htmlContent += `<div class="section">${convertMarkdownToHTML(block.content)}</div>`;
        }
        break;
        
      default:
        break;
    }
  });
  
  return htmlContent;
};

// Helper function to convert markdown to HTML
const convertMarkdownToHTML = (markdown) => {
  let html = markdown;
  
  // Bold text
  html = html.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
  
  // Headers
  html = html.replace(/### (.*?)(?=\n|$)/g, '<h3>$1</h3>');
  html = html.replace(/## (.*?)(?=\n|$)/g, '<h2>$1</h2>');
  html = html.replace(/# (.*?)(?=\n|$)/g, '<h1>$1</h1>');
  
  // Lists
  html = html.replace(/^   - (.*?)$/gm, '<li style="margin-left: 20px;">$1</li>');
  html = html.replace(/^- (.*?)$/gm, '<li>$1</li>');
  
  // Wrap consecutive list items in ul tags
  html = html.replace(/(<li>.*?<\/li>\s*)+/g, '<ul>$&</ul>');
  
  // Numbered lists
  html = html.replace(/^(\d+)\. (.*?)$/gm, '<li>$2</li>');
  
  // Code blocks
  html = html.replace(/```([\s\S]*?)```/g, '<pre>$1</pre>');
  
  // Inline code
  html = html.replace(/`(.*?)`/g, '<code>$1</code>');
  
  // Line breaks
  html = html.replace(/\n\n/g, '</p><p>');
  html = '<p>' + html + '</p>';
  
  return html;
};

// Enhanced visualization components
const DynamicVisualization = ({ data, type, title, onDataPointClick }) => {
  const [selectedDataPoint, setSelectedDataPoint] = useState(null);

  const handleClick = (data, index) => {
    setSelectedDataPoint({ data, index });
    if (onDataPointClick) onDataPointClick(data, index);
  };

  switch (type) {
    case 'funnel':
      return (
        <ResponsiveContainer width="100%" height={300}>
          <FunnelChart>
            <Tooltip 
              contentStyle={{ 
                backgroundColor: 'rgba(255,255,255,0.95)',
                border: '1px solid #e5e7eb',
                borderRadius: '8px'
              }}
            />
            <Funnel
              dataKey="value"
              data={data}
              isAnimationActive
            >
              <LabelList position="center" fill="#fff" />
              {data.map((entry, index) => (
                <Cell 
                  key={`cell-${index}`} 
                  fill={COLORS.chartColors[index % COLORS.chartColors.length]}
                  onClick={() => handleClick(entry, index)}
                  style={{ cursor: 'pointer' }}
                />
              ))}
            </Funnel>
          </FunnelChart>
        </ResponsiveContainer>
      );

    case 'treemap':
      return (
        <ResponsiveContainer width="100%" height={300}>
          <Treemap
            data={data}
            dataKey="value"
            ratio={4/3}
            stroke="#fff"
            fill={COLORS.primary}
          >
            {data.map((entry, index) => (
              <Cell 
                key={`cell-${index}`} 
                fill={COLORS.chartColors[index % COLORS.chartColors.length]}
              />
            ))}
          </Treemap>
        </ResponsiveContainer>
      );

    default:
      return null;
  }
};

// HTML Visualization Component for backend Chart.js visualizations and tables
const HTMLVisualization = ({ id, title, content, height = 400 }) => {
  const iframeRef = useRef(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isTable, setIsTable] = useState(false);

  useEffect(() => {
    if (iframeRef.current && content) {
      // Check if content contains a table
      const hasTable = content.includes('<table') && !content.includes('<canvas');
      setIsTable(hasTable);
      
      const iframeDoc = iframeRef.current.contentDocument || iframeRef.current.contentWindow.document;
      
      // Create a complete HTML document with Chart.js from CDN
      const htmlContent = `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          ${!hasTable ? '<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>' : ''}
          <style>
            html, body {
              margin: 0;
              padding: 0;
              width: 100%;
              ${hasTable ? '' : 'height: 100%;'}
              background-color: transparent;
            }
            body {
              padding: ${hasTable ? '0' : '16px'};
              font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
              box-sizing: border-box;
            }
            .chart-container {
              position: relative;
              height: ${height - 32}px;
              width: 100%;
            }
            table {
              width: 100%;
              border-collapse: collapse;
              font-size: 0.875rem;
              background-color: white;
            }
            th {
              background-color: #f9fafb;
              border-bottom: 2px solid #e5e7eb;
              padding: 0.75rem;
              text-align: left;
              font-weight: 600;
              color: #1f2937;
              position: sticky;
              top: 0;
              z-index: 10;
            }
            td {
              border-bottom: 1px solid #f3f4f6;
              padding: 0.75rem;
              color: #4b5563;
            }
            tr:hover {
              background-color: #f9fafb;
            }
            tbody tr:last-child td {
              border-bottom: none;
            }
            /* Custom scrollbar for tables */
            ::-webkit-scrollbar {
              height: 6px;
              width: 6px;
            }
            ::-webkit-scrollbar-track {
              background: #f3f4f6;
            }
            ::-webkit-scrollbar-thumb {
              background: #d1d5db;
              border-radius: 3px;
            }
            ::-webkit-scrollbar-thumb:hover {
              background: #9ca3af;
            }
          </style>
        </head>
        <body>
          ${content}
          ${hasTable ? `
          <script>
            // Round numerical values in table cells
            document.querySelectorAll('td').forEach(function(cell) {
              const content = cell.textContent.trim();
              const num = parseFloat(content);
              if (!isNaN(num) && content.match(/^\\-?\\d*\\.?\\d+$/)) {
                if (Number.isInteger(num)) {
                  cell.textContent = num.toLocaleString();
                } else {
                  // Round to 2 decimal places and remove trailing zeros
                  const rounded = parseFloat(num.toFixed(2));
                  cell.textContent = rounded.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2
                  });
                }
              }
            });
            
            // Function to resize the iframe
            function resizeIframe() {
              const body = document.body;
              const html = document.documentElement;
              const table = document.querySelector('table');
              
              if (table) {
                const tableHeight = table.offsetHeight;
                const maxHeight = 400; // Maximum height for tables
                const finalHeight = Math.min(tableHeight + 2, maxHeight);
                
                // Send the height to parent
                if (window.parent) {
                  window.parent.postMessage({
                    type: 'resize',
                    height: finalHeight,
                    id: '${id}'
                  }, '*');
                }
              }
            }
            
            // Call resize on load and after a delay to ensure content is rendered
            window.addEventListener('load', function() {
              resizeIframe();
              // Call again after a short delay to catch any late rendering
              setTimeout(resizeIframe, 100);
              setTimeout(resizeIframe, 500);
            });
            
            // Also observe for any DOM changes
            const observer = new MutationObserver(resizeIframe);
            observer.observe(document.body, {
              childList: true,
              subtree: true,
              attributes: true
            });
          </script>
          ` : ''}
        </body>
        </html>
      `;
      
      iframeDoc.open();
      iframeDoc.write(htmlContent);
      iframeDoc.close();
      
      // Handle iframe load
      iframeRef.current.onload = () => {
        setIsLoading(false);
        
        // For tables, also try to resize after load
        if (hasTable && iframeRef.current) {
          setTimeout(() => {
            try {
              const iframeDocument = iframeRef.current.contentDocument || iframeRef.current.contentWindow.document;
              const table = iframeDocument.querySelector('table');
              if (table) {
                const tableHeight = table.offsetHeight;
                const maxHeight = 400;
                const finalHeight = Math.min(tableHeight + 2, maxHeight);
                iframeRef.current.style.height = finalHeight + 'px';
              }
            } catch (e) {
              console.log('Could not access iframe content directly');
            }
          }, 100);
        }
      };
    }
  }, [content, height, id, isTable]);

  // Listen for resize messages from iframe
  useEffect(() => {
    const handleMessage = (event) => {
      if (event.data && event.data.type === 'resize' && event.data.id === id && iframeRef.current) {
        const newHeight = event.data.height;
        if (newHeight > 0) {
          iframeRef.current.style.height = newHeight + 'px';
        }
      }
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [id]);

  return (
    <div style={{
      backgroundColor: COLORS.white,
      borderRadius: '12px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.06)',
      border: '1px solid #e5e7eb',
      overflow: 'hidden',
      transition: 'all 0.3s ease',
      position: 'relative'
    }}
    onMouseEnter={(e) => {
      e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)';
      e.currentTarget.style.transform = 'translateY(-2px)';
    }}
    onMouseLeave={(e) => {
      e.currentTarget.style.boxShadow = '0 2px 8px rgba(0,0,0,0.06)';
      e.currentTarget.style.transform = 'translateY(0)';
    }}>
      {/* Title bar if title exists */}
      {title && (
        <div style={{
          padding: '0.75rem 1.25rem',
          borderBottom: '1px solid #e5e7eb',
          backgroundColor: '#fafbfc',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}>
          <h4 style={{
            margin: 0,
            fontSize: '0.9375rem',
            fontWeight: '600',
            color: COLORS.dark,
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem'
          }}>
            {isTable ? <BarChart2 size={16} color={COLORS.primary} /> : <Activity size={16} color={COLORS.primary} />}
            {title}
          </h4>
          <div style={{
            display: 'flex',
            gap: '0.5rem'
          }}>
            {isTable && (
              <span style={{
                fontSize: '0.75rem',
                color: '#6b7280',
                padding: '0.125rem 0.5rem',
                backgroundColor: '#f3f4f6',
                borderRadius: '4px'
              }}>
                Table
              </span>
            )}
          </div>
        </div>
      )}
      
      {/* Loading state */}
      {isLoading && (
        <div style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          backgroundColor: 'rgba(255, 255, 255, 0.9)',
          zIndex: 10
        }}>
          <div style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            gap: '0.5rem'
          }}>
            <Loader size={24} className="animate-spin" color={COLORS.primary} />
            <span style={{ fontSize: '0.875rem', color: '#6b7280' }}>
              Loading {isTable ? 'data' : 'visualization'}...
            </span>
          </div>
        </div>
      )}
      
      {/* Content iframe */}
      <iframe
        ref={iframeRef}
        id={id}
        style={{
          width: '100%',
          height: isTable ? '300px' : `${height}px`,
          minHeight: isTable ? '150px' : '200px',
          maxHeight: isTable ? '400px' : `${height}px`,
          border: 'none',
          backgroundColor: '#ffffff',
          display: isLoading ? 'none' : 'block',
          overflow: isTable ? 'auto' : 'hidden'
        }}
        title={title || 'Visualization'}
        sandbox="allow-scripts allow-same-origin"
        scrolling={isTable ? "auto" : "no"}
      />
    </div>
  );
};

// Enhanced KPI Card Component
const KPICard = ({ label, value, icon: Icon, color, trend, onClick }) => {
  const getTrendIcon = () => {
    if (!trend) return null;
    return trend > 0 ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />;
  };

  const getTrendColor = () => {
    if (!trend) return '#6b7280';
    return trend > 0 ? COLORS.success : COLORS.danger;
  };

  return (
    <div 
      style={{
        ...styles.kpiCard,
        ':hover': { transform: 'translateY(-2px)', boxShadow: '0 4px 6px rgba(0,0,0,0.1)' }
      }}
      onClick={onClick}
      onMouseEnter={(e) => {
        e.currentTarget.style.transform = 'translateY(-2px)';
        e.currentTarget.style.boxShadow = '0 4px 6px rgba(0,0,0,0.1)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.transform = 'translateY(0)';
        e.currentTarget.style.boxShadow = 'none';
      }}
    >
      <div style={{
        ...styles.kpiIcon,
        backgroundColor: `${color}15`
      }}>
        <Icon size={24} color={color} />
      </div>
      <div style={styles.kpiLabel}>{label}</div>
      <div style={{ ...styles.kpiValue, color }}>{formatNumber(value)}</div>
      {trend !== undefined && (
        <div style={{ ...styles.kpiChange, color: getTrendColor() }}>
          {getTrendIcon()}
          <span>{Math.abs(trend)}% vs last period</span>
        </div>
      )}
    </div>
  );
};

// Interactive Table Component
const InteractiveTable = ({ data, columns, onRowClick, onAction }) => {
  const [sortColumn, setSortColumn] = useState(null);
  const [sortDirection, setSortDirection] = useState('asc');
  const [hoveredRow, setHoveredRow] = useState(null);

  const handleSort = (column) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const sortedData = React.useMemo(() => {
    if (!sortColumn) return data;
    
    return [...data].sort((a, b) => {
      const aVal = a[sortColumn];
      const bVal = b[sortColumn];
      
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
      }
      
      return sortDirection === 'asc' 
        ? String(aVal).localeCompare(String(bVal))
        : String(bVal).localeCompare(String(aVal));
    });
  }, [data, sortColumn, sortDirection]);

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={styles.interactiveTable}>
        <thead style={styles.tableHeader}>
          <tr>
            {columns.map((col) => (
              <th
                key={col.key}
                style={styles.tableHeaderCell}
                onClick={() => handleSort(col.key)}
                onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  {col.label}
                  {sortColumn === col.key && (
                    <span style={{ fontSize: '0.75rem' }}>
                      {sortDirection === 'asc' ? '↑' : '↓'}
                    </span>
                  )}
                </div>
              </th>
            ))}
            {onAction && <th style={styles.tableHeaderCell}>Actions</th>}
          </tr>
        </thead>
        <tbody>
          {sortedData.map((row, index) => (
            <tr
              key={index}
              style={{
                ...styles.tableRow,
                backgroundColor: hoveredRow === index ? '#f9fafb' : 'transparent',
                cursor: onRowClick ? 'pointer' : 'default'
              }}
              onMouseEnter={() => setHoveredRow(index)}
              onMouseLeave={() => setHoveredRow(null)}
              onClick={() => onRowClick && onRowClick(row)}
            >
              {columns.map((col) => (
                <td key={col.key} style={styles.tableCell}>
                  {col.render ? col.render(row[col.key], row) : formatNumber(row[col.key])}
                </td>
              ))}
              {onAction && (
                <td style={styles.tableCell}>
                  <button
                    style={styles.actionButton}
                    onClick={(e) => {
                      e.stopPropagation();
                      onAction(row);
                    }}
                    onMouseEnter={(e) => e.target.style.opacity = '0.8'}
                    onMouseLeave={(e) => e.target.style.opacity = '1'}
                  >
                    View Details
                  </button>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

// Execution Timeline Component
const ExecutionTimeline = ({ history }) => {
  const getPhaseColor = (phase) => {
    switch (phase) {
      case 'planning': return COLORS.primary;
      case 'retrieval': return COLORS.secondary;
      case 'reasoning': return COLORS.success;
      case 'reflection': return COLORS.warning;
      default: return COLORS.dark;
    }
  };

  return (
    <div style={styles.executionTimeline}>
      <div style={{ fontSize: '0.875rem', fontWeight: '600', marginBottom: '0.75rem' }}>
        Execution Timeline
      </div>
      {history.map((step, index) => (
        <div key={index} style={styles.timelineItem}>
          <div 
            style={{
              ...styles.timelineDot,
              backgroundColor: getPhaseColor(step.phase)
            }}
          />
          <div style={styles.timelineContent}>
            <div style={{
              ...styles.timelinePhase,
              color: getPhaseColor(step.phase)
            }}>
              {step.phase}
            </div>
            <div style={styles.timelineDetail}>
              {step.tool && (
                <div>
                  <Database size={12} style={{ display: 'inline', marginRight: '0.25rem' }} />
                  {step.tool} • {step.result_count || 0} results • {step.duration?.toFixed(2)}s
                </div>
              )}
              {step.plan && (
                <div style={{ marginTop: '0.25rem' }}>
                  {step.plan.reasoning}
                </div>
              )}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
};

// Query Debugger Component
const QueryDebugger = ({ query, results, error }) => {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div style={{ marginTop: '0.5rem' }}>
      <button
        style={{
          ...styles.badge,
          backgroundColor: error ? '#fef2f2' : '#f3f4f6',
          color: error ? COLORS.danger : '#6b7280',
          cursor: 'pointer',
          border: `1px solid ${error ? '#fecaca' : '#e5e7eb'}`,
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem'
        }}
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <Code size={14} />
        SQL Query
        {isExpanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>
      {isExpanded && (
        <div style={styles.queryDebugger}>
          <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{query}</pre>
          {error && (
            <div style={{ color: '#ef4444', marginTop: '0.5rem' }}>
              Error: {error}
            </div>
          )}
          {results && (
            <div style={{ color: '#10b981', marginTop: '0.5rem' }}>
              Returned {results} rows
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// Main Dashboard Component
export default function EnhancedCRODashboard() {
  const [messages, setMessages] = useState([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [streamingMessage, setStreamingMessage] = useState('');
  const [sessionId, setSessionId] = useState('');
  const [error, setError] = useState(null);
  const [connectionStatus, setConnectionStatus] = useState('checking');
  const [showTraceDetails, setShowTraceDetails] = useState({});
  const [exportingPDF, setExportingPDF] = useState({});
  const [activeTab, setActiveTab] = useState('chat');
  const chatBodyRef = useRef(null);
  const streamingRef = useRef(false);
  
  // Initialize session ID on mount
  useEffect(() => {
    const newSessionId = `session-${Date.now()}`;
    setSessionId(newSessionId);
    checkAPIHealth();
    
    const healthCheckInterval = setInterval(() => {
      checkAPIHealth();
    }, 30000);
    
    return () => clearInterval(healthCheckInterval);
  }, []);

  // Scroll to bottom when new messages arrive
  useEffect(() => {
    if (chatBodyRef.current) {
      chatBodyRef.current.scrollTop = chatBodyRef.current.scrollHeight;
    }
  }, [messages, streamingMessage]);

  // Check API health
  const checkAPIHealth = async () => {
    setConnectionStatus('checking');
    try {
      const response = await fetch(`${API_BASE_URL}/health`);
      if (response.ok) {
        setConnectionStatus('connected');
        setError(null);
      } else {
        setConnectionStatus('disconnected');
        setError('API is not responding properly');
      }
    } catch (err) {
      setConnectionStatus('disconnected');
      setError('Cannot connect to API. Please ensure the backend is running on port 8083.');
    }
  };

  // Handle PDF export
  const handleExportPDF = async (message, messageIndex) => {
    setExportingPDF({ ...exportingPDF, [message.id]: true });
    
    // Find the corresponding user question
    let userQuestion = 'Analysis Query';
    for (let i = messageIndex - 1; i >= 0; i--) {
      if (messages[i].type === 'user') {
        userQuestion = messages[i].content;
        break;
      }
    }
    
    try {
      // Export to PDF
      await exportToPDF(message, userQuestion);
    } catch (error) {
      console.error('Error exporting PDF:', error);
    }
    
    // Reset exporting state after a delay
    setTimeout(() => {
      setExportingPDF({ ...exportingPDF, [message.id]: false });
    }, 1000);
  };

  // Enhanced message sending with rich response handling
  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading || !sessionId) return;
    
    const question = inputValue;
    setInputValue('');
    setIsLoading(true);
    setStreamingMessage('');
    setError(null);
    streamingRef.current = true;
    
    // Add user message
    const userMessage = {
      id: Date.now(),
      type: 'user',
      content: question,
      timestamp: new Date()
    };
    setMessages(prev => [...prev, userMessage]);

    try {
      // Send query to backend
      const response = await fetch(`${API_BASE_URL}/qa`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Session-ID': sessionId
        },
        body: JSON.stringify({
          question,
          max_iterations: 3,
          session_id: sessionId
        })
      });
      
      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }
      
      const data = await response.json();
      
      // Check if blocks are present and contain the answer content
      const hasContentBlocks = data.blocks && data.blocks.some(block => 
        block.type === 'markdown' || block.type === 'headline'
      );
      
      // If blocks contain the content, clear the answer to avoid duplication
      if (hasContentBlocks) {
        data.answer = '';
      }
      
      // Simulate streaming for better UX
      let accumulatedContent = '';
      const contentToStream = data.answer || (hasContentBlocks ? 'Analysis complete. See results below.' : 'No response received');
      const words = contentToStream.split(' ');
      let currentIndex = 0;
      
      const streamInterval = setInterval(() => {
        if (currentIndex < words.length && streamingRef.current) {
          const chunk = words.slice(currentIndex, currentIndex + 5).join(' ');
          accumulatedContent += chunk + ' ';
          setStreamingMessage(accumulatedContent);
          currentIndex += 5;
        } else {
          clearInterval(streamInterval);
          
          // Create enhanced bot message with all data including visualizations
          const botMessage = {
            id: Date.now() + 1,
            type: 'bot',
            content: hasContentBlocks ? '' : data.answer,
            timestamp: new Date(),
            sessionId: sessionId,
            metadata: {
              total_execution_time: data.total_execution_time,
              iterations: data.iterations
            },
            evidence: data.evidence || [],
            traces: data._traces || {},
            blocks: data.blocks || [],
            visualizations: data.visualizations || [],
            dataSources: data.data_sources || null,
            dataSource: data.data_source || null,  // Raw query data
            kpis: data.kpis || {},
            errors: data.errors || [],
            reasoning_steps: data.reasoning_steps || []
          };
          
          setMessages(prev => [...prev, botMessage]);
          setStreamingMessage('');
          setIsLoading(false);
        }
      }, 30);
      
    } catch (err) {
      console.error('Query error:', err);
      setError(err.message);
      setStreamingMessage('');
      setIsLoading(false);
      streamingRef.current = false;
    }
  };

  // Helper function to render markdown content
  const renderMarkdown = (content) => {
    // Bold text
    let formatted = content.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    
    // Headers
    formatted = formatted.replace(/### (.*?)(?=\n|$)/g, '<h3 style="font-size: 1.125rem; font-weight: 600; margin: 1.5rem 0 0.75rem 0; color: #1f2937;">$1</h3>');
    formatted = formatted.replace(/## (.*?)(?=\n|$)/g, '<h2 style="font-size: 1.25rem; font-weight: 700; margin: 1.5rem 0 0.75rem 0; color: #1f2937;">$1</h2>');
    formatted = formatted.replace(/# (.*?)(?=\n|$)/g, '<h1 style="font-size: 1.5rem; font-weight: 700; margin: 1.5rem 0 0.75rem 0; color: #1f2937;">$1</h1>');
    
    // Lists with custom bullets
    formatted = formatted.replace(/^   - (.*?)$/gm, '<li style="margin-left: 2rem; margin-bottom: 0.5rem; list-style: none; position: relative;"><span style="position: absolute; left: -1.5rem; color: #3b82f6;">•</span>$1</li>');
    formatted = formatted.replace(/^- (.*?)$/gm, '<li style="margin-left: 1rem; margin-bottom: 0.5rem; list-style: none; position: relative;"><span style="position: absolute; left: -1rem; color: #3b82f6;">•</span>$1</li>');
    
    // Numbered lists
    formatted = formatted.replace(/^(\d+)\. (.*?)$/gm, '<div style="margin-bottom: 0.75rem; position: relative; padding-left: 2rem;"><span style="position: absolute; left: 0; font-weight: 600; color: #3b82f6;">$1.</span>$2</div>');
    
    // Arrow indicators
    formatted = formatted.replace(/→ (.*?)(?=\n|$)/g, '<div style="margin-left: 1rem; margin-top: 0.5rem; display: flex; align-items: flex-start;"><span style="color: #3b82f6; margin-right: 0.5rem;">→</span><span>$1</span></div>');
    
    // Code blocks
    formatted = formatted.replace(/```([\s\S]*?)```/g, '<pre style="background-color: #1f2937; color: #f3f4f6; padding: 1rem; border-radius: 6px; overflow-x: auto; font-size: 0.875rem; margin: 0.75rem 0; font-family: monospace;">$1</pre>');
    
    // Inline code
    formatted = formatted.replace(/`(.*?)`/g, '<code style="background-color: #f3f4f6; padding: 0.125rem 0.25rem; border-radius: 3px; font-size: 0.875rem; font-family: monospace;">$1</code>');
    
    // Line breaks
    formatted = formatted.replace(/\n\n/g, '</p><p style="margin: 0.75rem 0;">');
    formatted = '<p style="margin: 0.75rem 0;">' + formatted + '</p>';
    
    return <div dangerouslySetInnerHTML={{ __html: formatted }} />;
  };
  const [collapsedDataSources, setCollapsedDataSources] = useState({});
  // Enhanced message rendering with rich visualizations
  const renderEnhancedMessage = (message, messageIndex) => {
    if (message.type === 'user') {
      return (
        <div style={styles.messageContainer}>
          <div style={styles.userMessage}>{message.content}</div>
        </div>
      );
    }

    // Parse and render blocks from backend
    const renderBlocks = () => {
      if (!message.blocks || message.blocks.length === 0) return null;

      return message.blocks.map((block, index) => {
        switch (block.type) {
          case 'headline':
            // Skip rendering headline blocks entirely
            return null;

          case 'markdown':
            return (
              <div key={index} style={{
                lineHeight: '1.6',
                color: COLORS.dark
              }}>
                {renderMarkdown(block.content)}
              </div>
            );

          case 'kpis':
            return (
              <div key={index} style={styles.kpiGrid}>
                {Object.entries(block.values).map(([key, value]) => (
                  <KPICard
                    key={key}
                    label={key.replace(/_/g, ' ')}
                    value={value}
                    icon={
                      key.includes('pipeline') ? TrendingUp :
                      key.includes('deal') ? DollarSign :
                      key.includes('win') ? Target : Activity
                    }
                    color={COLORS.primary}
                    trend={block.trends?.[key]}
                  />
                ))}
              </div>
            );

          case 'chart':
          case 'table':
            // Skip these - they'll be rendered from visualizations array
            return null;

          default:
            return null;
        }
      });
    };

    return (
      <div style={styles.messageContainer}>
        <div style={styles.botMessage}>
          {/* Streaming Indicator */}
          {isLoading && messages.length > 0 && messages[messages.length - 1].type === 'user' && (
            <div style={styles.messageContainer}>
              <div style={{
                ...styles.botMessage,
                display: 'flex',
                alignItems: 'center',
                gap: '0.75rem',
                padding: '1rem 1.25rem'
              }}>
                {/* Animated spinner */}
                <div style={{
                  width: '32px',
                  height: '32px',
                  position: 'relative',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center'
                }}>
                  {/* Outer ring */}
                  <div style={{
                    position: 'absolute',
                    width: '32px',
                    height: '32px',
                    border: '3px solid #e5e7eb',
                    borderRadius: '50%'
                  }} />
                  {/* Spinning ring */}
                  <div style={{
                    position: 'absolute',
                    width: '32px',
                    height: '32px',
                    border: '3px solid transparent',
                    borderTopColor: COLORS.primary,
                    borderRadius: '50%',
                    animation: 'spin 1s linear infinite'
                  }} />
                  {/* Inner pulse */}
                  <div style={{
                    width: '8px',
                    height: '8px',
                    backgroundColor: COLORS.primary,
                    borderRadius: '50%',
                    animation: 'pulse 1.5s ease-in-out infinite'
                  }} />
                </div>
                
                {/* Loading text with typing animation */}
                <div style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '0.25rem'
                }}>
                  <div style={{
                    fontSize: '0.9375rem',
                    fontWeight: '500',
                    color: COLORS.dark
                  }}>
                    Analyzing your request
                    <span style={{
                      animation: 'blink 1.4s infinite',
                      marginLeft: '2px'
                    }}>...</span>
                  </div>
                  <div style={{
                    fontSize: '0.813rem',
                    color: '#6b7280',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem'
                  }}>
                    <Brain size={14} style={{ animation: 'pulse 2s ease-in-out infinite' }} />
                    Processing data and generating insights
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Render Blocks */}
          {renderBlocks()}

          {/* Render Visualizations */}
          {message.visualizations && message.visualizations.length > 0 && (
            <div style={{ 
              marginTop: '1rem',
              display: 'grid',
              gridTemplateColumns: message.visualizations.filter(viz => viz.content && viz.content.trim() !== '').length === 1 
                ? '1fr' 
                : 'repeat(auto-fit, minmax(400px, 1fr))',
              gap: '1rem'
            }}>
              {message.visualizations
                .filter(viz => viz.content && viz.content.trim() !== '') // Filter out empty visualizations
                .map((viz, index) => (
                  <HTMLVisualization
                    key={viz.id || index}
                    id={viz.id}
                    title={viz.title}
                    content={viz.content}
                    height={400}
                  />
                ))}
            </div>
          )}

          {/* Execution Timeline */}
          {message.traces?.execution_history && message.traces.execution_history.length > 0 && (
            <ExecutionTimeline history={message.traces.execution_history} />
          )}

          {/* Main Answer Content - Only show if no blocks or for backward compatibility */}
          {(!message.blocks || message.blocks.length === 0) && (
            <div style={{ marginTop: '1rem' }}>
              {renderMarkdown(message.content)}
            </div>
          )}

          {/* Reasoning Steps */}
          {message.reasoning_steps && message.reasoning_steps.length > 0 && (
            <div style={{
              marginTop: '1rem',
              padding: '1rem',
              backgroundColor: '#fafbfc',
              borderRadius: '8px',
              border: '1px solid #f3f4f6'
            }}>
              <div style={{ 
                display: 'flex', 
                alignItems: 'center', 
                gap: '0.5rem',
                marginBottom: '0.75rem',
                fontSize: '0.875rem',
                fontWeight: '600'
              }}>
                <Brain size={16} color={COLORS.secondary} />
                Reasoning Process
              </div>
              {message.reasoning_steps.map((step, index) => (
                <div key={index} style={{
                  fontSize: '0.813rem',
                  color: '#6b7280',
                  marginBottom: '0.5rem',
                  paddingLeft: '1.5rem',
                  position: 'relative'
                }}>
                  <span style={{
                    position: 'absolute',
                    left: '0',
                    color: COLORS.secondary
                  }}>
                    {index + 1}.
                  </span>
                  {step}
                </div>
              ))}
            </div>
          )}

          {/* Evidence Trail - Only show if evidence has meaningful variety */}
          {message.evidence && message.evidence.length > 0 && (
            (() => {
              // Group evidence by source type
              const evidenceGroups = message.evidence.reduce((acc, id) => {
                const match = id.match(/^([^_]+)_/);
                const source = match ? match[1] : 'other';
                acc[source] = (acc[source] || 0) + 1;
                return acc;
              }, {});
              
              // Only show if there's variety in sources or specific interesting evidence
              const uniqueSources = Object.keys(evidenceGroups);
              if (uniqueSources.length === 1 && uniqueSources[0] === 'duckdb' && evidenceGroups['duckdb'] > 3) {
                // For repetitive duckdb sources, show a summary instead
                return (
                  <div style={{
                    marginTop: '1rem',
                    padding: '0.75rem',
                    backgroundColor: '#f9fafb',
                    borderRadius: '6px',
                    fontSize: '0.813rem',
                    color: '#6b7280',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem'
                  }}>
                    <Database size={14} />
                    Analysis based on {evidenceGroups['duckdb']} database queries
                  </div>
                );
              } else {
                // Show detailed evidence for varied sources
                return (
                  <div style={styles.evidenceTrail}>
                    <div style={{ fontSize: '0.813rem', fontWeight: '600', marginBottom: '0.5rem' }}>
                      Evidence Sources
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
                      {Object.entries(evidenceGroups).map(([source, count]) => (
                        <span 
                          key={source} 
                          style={{
                            ...styles.evidenceItem,
                            textTransform: 'capitalize'
                          }}
                          onMouseEnter={(e) => {
                            e.currentTarget.style.backgroundColor = '#dbeafe';
                            e.currentTarget.style.transform = 'scale(1.05)';
                          }}
                          onMouseLeave={(e) => {
                            e.currentTarget.style.backgroundColor = '#eff6ff';
                            e.currentTarget.style.transform = 'scale(1)';
                          }}
                        >
                          {source === 'duckdb' && <Database size={12} />}
                          {source === 'vector' && <Brain size={12} />}
                          {source === 'neo4j' && <Activity size={12} />}
                          {source} ({count})
                        </span>
                      ))}
                    </div>
                  </div>
                );
              }
            })()
          )}

          {/* Data Source Queries - Display raw query information */}
          {message.dataSource && Array.isArray(message.dataSource) && message.dataSource.length > 0 && (
            <div style={{
              marginTop: '1rem',
              padding: '1rem',
              backgroundColor: '#f9fafb',
              borderRadius: '8px',
              border: '1px solid #e5e7eb'
            }}>
              <div style={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'space-between',
                marginBottom: collapsedDataSources[`${message.id}-dataSource`] ? 0 : '0.75rem',
                cursor: 'pointer'
              }}
              onClick={() => setCollapsedDataSources({
                ...collapsedDataSources,
                [`${message.id}-dataSource`]: !collapsedDataSources[`${message.id}-dataSource`]
              })}
              >
                <div style={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  gap: '0.5rem',
                  fontSize: '0.875rem',
                  fontWeight: '600'
                }}>
                  <Database size={16} color={COLORS.primary} />
                  Data Source Queries
                </div>
                <button
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    padding: '0.25rem',
                    borderRadius: '4px',
                    transition: 'background-color 0.2s'
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                  onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                >
                  {collapsedDataSources[`${message.id}-dataSource`] ? 
                    <ChevronDown size={16} color="#6b7280" /> : 
                    <ChevronUp size={16} color="#6b7280" />
                  }
                </button>
              </div>
              
              {!collapsedDataSources[`${message.id}-dataSource`] && (
                <>
                  <div style={{ display: 'grid', gap: '0.75rem' }}>
                    {message.dataSource.map((source, index) => (
                      <div 
                        key={index}
                        style={{ 
                          padding: '0.75rem',
                          backgroundColor: '#ffffff',
                          borderRadius: '6px',
                          border: '1px solid #f3f4f6'
                        }}
                      >
                        <div style={{ 
                          display: 'flex', 
                          alignItems: 'center', 
                          gap: '1rem',
                          marginBottom: '0.5rem',
                          fontSize: '0.813rem'
                        }}>
                          <span style={{ 
                            fontWeight: '600',
                            color: COLORS.primary,
                            textTransform: 'uppercase',
                            fontSize: '0.75rem',
                            padding: '0.25rem 0.5rem',
                            backgroundColor: '#eff6ff',
                            borderRadius: '4px'
                          }}>
                            {source.tool}
                          </span>
                          <span style={{ color: '#6b7280' }}>
                            {source.result_count} records returned
                          </span>
                        </div>
                        <div style={{
                          padding: '0.5rem',
                          backgroundColor: '#1f2937',
                          borderRadius: '4px',
                          overflow: 'auto',
                          maxHeight: '100px'
                        }}>
                          <code style={{
                            color: '#f3f4f6',
                            fontSize: '0.75rem',
                            fontFamily: 'monospace',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word'
                          }}>
                            {source.query}
                          </code>
                        </div>
                      </div>
                    ))}
                  </div>
                  
                  {/* Summary */}
                  <div style={{
                    marginTop: '0.75rem',
                    paddingTop: '0.75rem',
                    borderTop: '1px solid #e5e7eb',
                    fontSize: '0.813rem',
                    color: '#4b5563',
                    display: 'flex',
                    justifyContent: 'space-between'
                  }}>
                    <span>
                      <strong>Total Queries:</strong> {message.dataSource.length}
                    </span>
                    <span>
                      <strong>Total Records:</strong> {message.dataSource.reduce((sum, item) => sum + (item.result_count || 0), 0).toLocaleString()}
                    </span>
                  </div>
                </>
              )}
            </div>
          )}

          {/* Data Sources - Keep existing structured format display */}
          
          {message.dataSources && Array.isArray(message.dataSources) && message.dataSources.length > 0 && (
            <div style={{
              marginTop: '1rem',
              padding: '1rem',
              backgroundColor: '#f9fafb',
              borderRadius: '8px',
              border: '1px solid #e5e7eb'
            }}>
              <div style={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'space-between',
                marginBottom: collapsedDataSources[`${message.id}-dataSources`] ? 0 : '0.75rem',
                cursor: 'pointer'
              }}
              onClick={() => setCollapsedDataSources({
                ...collapsedDataSources,
                [`${message.id}-dataSources`]: !collapsedDataSources[`${message.id}-dataSources`]
              })}
              >
                <div style={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  gap: '0.5rem',
                  fontSize: '0.875rem',
                  fontWeight: '600'
                }}>
                  <Database size={16} color={COLORS.primary} />
                  Data Sources
                </div>
                <button
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    padding: '0.25rem',
                    borderRadius: '4px',
                    transition: 'background-color 0.2s'
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                  onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                >
                  {collapsedDataSources[`${message.id}-dataSources`] ? 
                    <ChevronDown size={16} color="#6b7280" /> : 
                    <ChevronUp size={16} color="#6b7280" />
                  }
                </button>
              </div>
              
              {!collapsedDataSources[`${message.id}-dataSources`] && (
                <>
                  <div style={{ display: 'grid', gap: '0.75rem' }}>
                    {message.dataSources.map((source, index) => (
                      <div 
                        key={index}
                        style={{ 
                          padding: '0.75rem',
                          backgroundColor: '#ffffff',
                          borderRadius: '6px',
                          border: '1px solid #f3f4f6'
                        }}
                      >
                        <div style={{ 
                          display: 'flex', 
                          alignItems: 'center', 
                          gap: '1rem',
                          marginBottom: '0.5rem',
                          fontSize: '0.813rem'
                        }}>
                          <span style={{ 
                            fontWeight: '600',
                            color: COLORS.primary,
                            textTransform: 'uppercase',
                            fontSize: '0.75rem',
                            padding: '0.25rem 0.5rem',
                            backgroundColor: '#eff6ff',
                            borderRadius: '4px'
                          }}>
                            {source.tool}
                          </span>
                          <span style={{ color: '#6b7280' }}>
                            {source.result_count} records returned
                          </span>
                          {source.type && (
                            <span style={{
                              padding: '0.125rem 0.375rem',
                              backgroundColor: source.type === 'database' ? '#dbeafe' : '#e9d5ff',
                              borderRadius: '4px',
                              fontSize: '0.75rem'
                            }}>
                              {source.type}
                            </span>
                          )}
                        </div>
                        <div style={{
                          padding: '0.5rem',
                          backgroundColor: '#1f2937',
                          borderRadius: '4px',
                          overflow: 'auto',
                          maxHeight: '100px'
                        }}>
                          <code style={{
                            color: '#f3f4f6',
                            fontSize: '0.75rem',
                            fontFamily: 'monospace',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word'
                          }}>
                            {source.query}
                          </code>
                        </div>
                      </div>
                    ))}
                  </div>
                  
                  {/* Summary */}
                  <div style={{
                    marginTop: '0.75rem',
                    paddingTop: '0.75rem',
                    borderTop: '1px solid #e5e7eb',
                    fontSize: '0.813rem',
                    color: '#4b5563',
                    display: 'flex',
                    justifyContent: 'space-between'
                  }}>
                    <span>
                      <strong>Total Queries:</strong> {message.dataSources.length}
                    </span>
                    <span>
                      <strong>Total Records:</strong> {message.dataSources.reduce((sum, item) => sum + (item.result_count || 0), 0).toLocaleString()}
                    </span>
                  </div>
                </>
              )}
            </div>
          )}

          {/* Query Debugger for raw results */}
          {message.traces?.raw_results && message.traces.raw_results.length > 0 && (
            <div style={{ marginTop: '1rem' }}>
              <button
                style={{
                  ...styles.badge,
                  backgroundColor: '#f3f4f6',
                  color: '#6b7280',
                  cursor: 'pointer',
                  border: '1px solid #e5e7eb'
                }}
                onClick={() => setShowTraceDetails({
                  ...showTraceDetails,
                  [message.id]: !showTraceDetails[message.id]
                })}
              >
                <Database size={14} style={{ marginRight: '0.25rem' }} />
                View Query Details
                {showTraceDetails[message.id] ? <ChevronUp size={14} style={{ marginLeft: '0.25rem' }} /> : <ChevronDown size={14} style={{ marginLeft: '0.25rem' }} />}
              </button>
              
              {showTraceDetails[message.id] && (
                <div style={{ marginTop: '0.5rem' }}>
                  {message.traces.raw_results.map((result, index) => (
                    <QueryDebugger
                      key={index}
                      query={result.query}
                      results={result.result_count}
                      error={result.error}
                    />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Metadata Footer */}
          <div style={{
            marginTop: '1rem',
            paddingTop: '1rem',
            borderTop: '1px solid #f3f4f6',
            fontSize: '0.75rem',
            color: '#6b7280',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: '0.5rem'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flexWrap: 'wrap' }}>
              <span>
                Processed in {message.metadata?.total_execution_time?.toFixed(2)}s • 
                {message.metadata?.iterations} iterations
              </span>
              {/* Show data source summary from raw dataSource array */}
              {message.dataSource && Array.isArray(message.dataSource) && message.dataSource.length > 0 && (
                <span style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  padding: '0.25rem 0.5rem',
                  backgroundColor: '#f3f4f6',
                  borderRadius: '4px'
                }}>
                  <Database size={12} />
                  {message.dataSource.length} queries • 
                  {message.dataSource.reduce((sum, item) => sum + (item.result_count || 0), 0).toLocaleString()} records
                </span>
              )}
              {/* Alternative: Show from structured dataSources if available */}
              {!message.dataSource && message.dataSources && Array.isArray(message.dataSources) && (
                <span style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  padding: '0.25rem 0.5rem',
                  backgroundColor: '#f3f4f6',
                  borderRadius: '4px'
                }}>
                  <Database size={12} />
                  {message.dataSources.length} queries • 
                  {message.dataSources.reduce((sum, item) => sum + (item.result_count || 0), 0).toLocaleString()} records
                </span>
              )}
            </div>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <button
                style={{
                  ...styles.badge,
                  backgroundColor: 'transparent',
                  color: COLORS.primary,
                  cursor: 'pointer',
                  border: `1px solid ${COLORS.primary}`,
                  opacity: exportingPDF[message.id] ? 0.6 : 1
                }}
                onClick={() => handleExportPDF(message, messageIndex)}
                disabled={exportingPDF[message.id]}
              >
                {exportingPDF[message.id] ? (
                  <>
                    <Loader size={12} style={{ marginRight: '0.25rem' }} className="animate-spin" />
                    Exporting...
                  </>
                ) : (
                  <>
                    <Download size={12} style={{ marginRight: '0.25rem' }} />
                    Export PDF
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  };

  const handleClearChat = () => {
    if (window.confirm('Are you sure you want to clear all messages?')) {
      setMessages([]);
      setStreamingMessage('');
      setError(null);
      // Optionally reset session ID for a fresh start
      const newSessionId = `session-${Date.now()}`;
      setSessionId(newSessionId);
    }
  };

  return (
    <div style={styles.container}>
      {/* Header */}
      <header style={styles.header}>
        <div style={styles.headerContent}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '2rem' }}>
            <div style={styles.logo}>
              <Activity size={28} color={COLORS.primary} />
              <span>CRO Analytics Pro</span>
            </div>
            
            {/* Tab Navigation */}
            <div style={{
              display: 'flex',
              backgroundColor: '#f3f4f6',
              borderRadius: '8px',
              padding: '0.25rem',
              gap: '0.25rem'
            }}>
              <button
                onClick={() => setActiveTab('chat')}
                style={{
                  padding: '0.5rem 1rem',
                  backgroundColor: activeTab === 'chat' ? COLORS.white : 'transparent',
                  color: activeTab === 'chat' ? COLORS.primary : '#6b7280',
                  border: 'none',
                  borderRadius: '6px',
                  fontSize: '0.875rem',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  boxShadow: activeTab === 'chat' ? '0 1px 3px rgba(0,0,0,0.1)' : 'none'
                }}
              >
                <Send size={16} />
                Chat Assistant
              </button>
              <button
                onClick={() => setActiveTab('analytics')}
                style={{
                  padding: '0.5rem 1rem',
                  backgroundColor: activeTab === 'analytics' ? COLORS.white : 'transparent',
                  color: activeTab === 'analytics' ? COLORS.primary : '#6b7280',
                  border: 'none',
                  borderRadius: '6px',
                  fontSize: '0.875rem',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  boxShadow: activeTab === 'analytics' ? '0 1px 3px rgba(0,0,0,0.1)' : 'none'
                }}
              >
                <BarChart2 size={16} />
                Analytics Dashboard
              </button>
            </div>
          </div>
          
          <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
            {/* Clear Chat Button - Only show when in chat tab */}
            {activeTab === 'chat' && messages.length > 0 && (
              <button
                onClick={handleClearChat}
                style={{
                  padding: '0.5rem 1rem',
                  backgroundColor: 'transparent',
                  color: COLORS.danger,
                  border: `1px solid ${COLORS.danger}`,
                  borderRadius: '6px',
                  fontSize: '0.813rem',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem'
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = COLORS.danger;
                  e.currentTarget.style.color = COLORS.white;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = 'transparent';
                  e.currentTarget.style.color = COLORS.danger;
                }}
              >
                <Trash2 size={14} />
                Clear Chat
              </button>
            )}
            
            <div style={{
              padding: '0.5rem 1rem',
              backgroundColor: connectionStatus === 'connected' ? '#d1fae5' : '#fee2e2',
              borderRadius: '6px',
              fontSize: '0.813rem',
              color: connectionStatus === 'connected' ? COLORS.success : COLORS.danger,
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem'
            }}>
              <div style={{
                width: '8px',
                height: '8px',
                borderRadius: '50%',
                backgroundColor: connectionStatus === 'connected' ? COLORS.success : COLORS.danger
              }} />
              {connectionStatus === 'connected' ? 'Connected' : 'Disconnected'}
            </div>
            <span style={{ fontSize: '0.875rem', color: '#6b7280' }}>
              Session: {sessionId.slice(-8)}
            </span>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main style={styles.mainContent}>
        {activeTab === 'chat' ? (
          <div style={styles.chatSection}>
            <div style={styles.chatHeader}>
              <h2 style={styles.chatTitle}>Analytics Assistant</h2>
            </div>

            <div style={styles.chatBody} ref={chatBodyRef}>
              {error && (
                <div style={{
                  backgroundColor: '#fef2f2',
                  border: '1px solid #fecaca',
                  color: COLORS.danger,
                  padding: '1rem',
                  borderRadius: '8px',
                  marginBottom: '1rem',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem'
                }}>
                  <AlertCircle size={20} />
                  {error}
                </div>
              )}
              
              {/* Render all messages */}
              {messages.map((message, index) => renderEnhancedMessage(message, index))}
              
              {/* Loading Spinner - MOVED OUTSIDE and simplified condition */}
              {isLoading && (
                <div style={styles.messageContainer}>
                  <div style={{
                    ...styles.botMessage,
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.75rem',
                    padding: '1rem 1.25rem'
                  }}>
                    {/* Animated spinner */}
                    <div style={{
                      width: '32px',
                      height: '32px',
                      position: 'relative',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}>
                      {/* Outer ring */}
                      <div style={{
                        position: 'absolute',
                        width: '32px',
                        height: '32px',
                        border: '3px solid #e5e7eb',
                        borderRadius: '50%'
                      }} />
                      {/* Spinning ring */}
                      <div style={{
                        position: 'absolute',
                        width: '32px',
                        height: '32px',
                        border: '3px solid transparent',
                        borderTopColor: COLORS.primary,
                        borderRadius: '50%',
                        animation: 'spin 1s linear infinite'
                      }} />
                      {/* Inner pulse */}
                      <div style={{
                        width: '8px',
                        height: '8px',
                        backgroundColor: COLORS.primary,
                        borderRadius: '50%',
                        animation: 'pulse 1.5s ease-in-out infinite'
                      }} />
                    </div>
                    
                    {/* Loading text with typing animation */}
                    <div style={{
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '0.25rem'
                    }}>
                      <div style={{
                        fontSize: '0.9375rem',
                        fontWeight: '500',
                        color: COLORS.dark
                      }}>
                        Analyzing your request
                        <span style={{
                          animation: 'blink 1.4s infinite',
                          marginLeft: '2px'
                        }}>...</span>
                      </div>
                      <div style={{
                        fontSize: '0.813rem',
                        color: '#6b7280',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.5rem'
                      }}>
                        <Brain size={14} style={{ animation: 'pulse 2s ease-in-out infinite' }} />
                        Processing data and generating insights
                      </div>
                    </div>
                  </div>
                </div>
              )}
              
              {/* Streaming message */}
              {streamingMessage && (
                <div style={styles.messageContainer}>
                  <div style={styles.botMessage}>
                    <div style={styles.streamingIndicator}>
                      <Loader size={14} className="animate-spin" />
                      Analyzing your data...
                    </div>
                    <div style={{ marginTop: '1rem' }}>{streamingMessage}</div>
                  </div>
                </div>
              )}
            </div>

            <div style={styles.inputSection}>
              <div style={styles.inputContainer}>
                <input
                  type="text"
                  value={inputValue}
                  onChange={(e) => setInputValue(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && handleSendMessage()}
                  placeholder="Ask about sales metrics, pipeline, or accounts..."
                  style={styles.input}
                  disabled={isLoading || connectionStatus === 'disconnected'}
                />
                <button
                  onClick={handleSendMessage}
                  style={{
                    ...styles.sendButton,
                    opacity: (isLoading || connectionStatus === 'disconnected') ? 0.6 : 1,
                    cursor: (isLoading || connectionStatus === 'disconnected') ? 'not-allowed' : 'pointer'
                  }}
                  disabled={isLoading || connectionStatus === 'disconnected'}
                >
                  {isLoading ? <Loader size={18} className="animate-spin" /> : <Send size={18} />}
                  {isLoading ? 'Analyzing...' : 'Send'}
                </button>
              </div>
            </div>
          </div>
        ) : (
          <div style={styles.chatSection}>
            <AnalyticsDashboard 
              styles={styles}
              COLORS={COLORS}
              API_BASE_URL={API_BASE_URL}
            />
          </div>
        )}
      </main>
    </div>
  );
}