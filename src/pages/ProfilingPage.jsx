import React, { useState, useEffect } from 'react';
import { Box, Typography, Button, MenuItem, Select, CircularProgress, Paper, Table, TableHead, TableRow, TableCell, TableBody } from '@mui/material';
import MappingComponent from '../components/MappingComponent';
import EntitySelector from '../components/EntitySelector';

// You may need to define or import these props from the parent
// const entity = ...;
// const sourceSystems = ...;
const entities = ['customer', 'item', 'supplier'];


const ProfilingPage = ({ entity, sourceSystems, handleProcess }) => {
  const [selectedSourceSystem, setSelectedSourceSystem] = useState('');
  const [summaryData, setSummaryData] = useState(null);
  const [selectedEntity, setSelectedEntity] = useState('');
  const [selectedColumn, setSelectedColumn] = useState('');
  const [profileData, setProfileData] = useState(null);
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [error, setError] = useState('');
  // const entities = ['customer', 'item', 'supplier'];

  // Fetch summary when source system changes
useEffect(() => {
  if (!selectedEntity) {
    setSummaryData(null); // Clear when nothing selected
    return; // Don't fetch if nothing selected
  }
  
  const fetchSummary = async () => {
    try {
      const response = await fetch('http://localhost:5001/api/system_summary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ entity: selectedEntity }) // Pass selected entity
      });
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      console.log(data); 
      setSummaryData(data); // Save the stats
    } catch (err) {
      setSummaryData(null);
      // Optionally set error message here
    }
  };
  fetchSummary(); // Call backend
}, [selectedEntity]); // Runs every time selectedEntity changes


  // Fetch profiling for selected column
  const handleProfileColumn = async () => {
    if (!selectedColumn) return;
    setLoadingProfile(true);
    setError('');
    setProfileData(null);
    try {
      const res = await fetch('http://localhost:5001/api/profile_column', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_system: selectedSourceSystem, column_name: selectedColumn }),
      });
      if (!res.ok) {
        const errMsg = await res.text();
        throw new Error(errMsg || 'Failed to fetch profile');
      }
      const data = await res.json();
      setProfileData(data);
    } catch (e) {
      setError(e.message);
      setProfileData(null);
    } finally {
      setLoadingProfile(false);
    }
  };

  return (
    <Box
      component="main"
      sx={{
        mt: 1,
        ml: '0rem',
        mr: 0,
        px: 0,
        width: '100%',
        maxWidth: '100%',
        background: "transparent",
        boxSizing: "border-box",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "flex-start",
      }}
    >
      <Paper
        elevation={3}
        sx={{
          width: '100%',
          height: '100%',
          p: { xs: 2, sm: 4, md: 5 },
          borderRadius: 5,
          background: 'linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)',
          color: "#100808",
          boxSizing: "border-box",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          boxShadow: 3,
          overflowY: "auto",
        }}
      >
        <Typography variant="h4" align="center" gutterBottom sx={{ color: 'black', fontFamily: 'sans-serif' }}>
          EY Data Harmonization
        </Typography>
        <Typography variant="subtitle1" align="center" color="text.secondary" sx={{ mb: 4, color: 'black', fontFamily: 'sans-serif' }}>
          Streamlining Data Harmonization
        </Typography>

        {/* Entity Selection*/}
        <Box sx={{ mb: 4 }}>
          <EntitySelector
            entities={entities}
            selectedEntity={selectedEntity}
            onSelect={setSelectedEntity}
          />
        </Box>

        {summaryData && (
          <div>
            <div>Total Customers: {summaryData.customer_count}</div>
            <div>Missing Address: {summaryData.customers_no_address_count}</div>
            <div>Missing Contact: {summaryData.customers_no_contact_count}</div>
          </div>
        )}


        {/* Main Processing Interface (MappingComponent) */}
        {entity && (
          <MappingComponent
            entity={entity}
            sourceSystems={sourceSystems}
            selectedSourceSystem={selectedSourceSystem}
            setSelectedSourceSystem={setSelectedSourceSystem}
          />
        )}

        {/* Error */}
        {error && (
          <Typography color="error" sx={{ mt: 2 }}>
            {error}
          </Typography>
        )}

        {/* Summary Boxes */}
        {loadingSummary ? <CircularProgress sx={{ mt: 2 }} /> : (
          summaryData && (
            <Box sx={{ display: 'flex', justifyContent: 'space-between', gap: 2, mb: 3, mt: 2 }}>
              {[
                { label: 'Customers', value: summaryData.customer_count },
                { label: 'Addresses', value: summaryData.address_count },
                { label: 'Contacts', value: summaryData.contact_count }
              ].map(({ label, value }) => (
                <Box
                  key={label}
                  sx={{
                    background: 'linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)',
                    borderRadius: 2,
                    border: '1.5px solid #90caf9',
                    textAlign: 'center',
                    flex: 1,
                    p: 3,
                  }}
                >
                  <Typography fontWeight={700} color="#1976d2" fontSize={20}>{label}</Typography>
                  <Typography fontWeight={700} color="#0d47a1" fontSize={28} mt={1}>
                    {value ?? '--'}
                  </Typography>
                </Box>
              ))}
            </Box>
          )
        )}

        {/* Column select dropdown */}
        {summaryData && summaryData.customer_columns && (
          <Box sx={{ mb: 3 }}>
            <Typography variant="h6" gutterBottom>Select column to profile:</Typography>
            <Select
              value={selectedColumn}
              onChange={(e) => setSelectedColumn(e.target.value)}
              sx={{ minWidth: 300, mr: 2 }}
              displayEmpty
            >
              <MenuItem value="" disabled>Select a column</MenuItem>
              {summaryData.customer_columns.map(col => (
                <MenuItem key={col} value={col}>{col}</MenuItem>
              ))}
            </Select>
            <Button
              variant="contained"
              disabled={!selectedColumn || loadingProfile}
              onClick={handleProfileColumn}
              sx={{ ml: 2 }}
            >
              Profile Column
            </Button>
            {loadingProfile && <CircularProgress size={24} sx={{ ml: 2 }} />}
          </Box>
        )}

        {/* Profiling results (reuse your ProfilingPage table and chart style) */}
        {profileData && (
          <Box>
            <Typography variant="h6" sx={{ mt: 3 }}>Column Profiling Stats:</Typography>
            <Table
              sx={{
                mt: 2,
                borderRadius: 2,
                overflow: "hidden",
                boxShadow: "0 2px 14px rgba(33,150,243,0.09)"
              }}
            >
              <TableHead>
                <TableRow sx={{ backgroundColor: "#e3f2fd" }}>
                  {[
                    "Column", "Total Count", "Null Count", "Null %", "Unique Values", "Unique %", "Avg Length", "Max Length", "Min Length"
                  ].map(header => (
                    <TableCell key={header} sx={{ fontWeight: 700, color: "#1976d2", fontSize: 16 }}>
                      {header}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {Object.entries(profileData["Column Profiling"]).map(([col, stats], idx) => (
                  <TableRow key={col} sx={{ backgroundColor: idx % 2 === 0 ? "#f6fbff" : "#ffffff" }}>
                    <TableCell sx={{ fontWeight: 700 }}>{col}</TableCell>
                    <TableCell>{stats["Total Count"]}</TableCell>
                    <TableCell>{stats["Null Count"]}</TableCell>
                    <TableCell>{stats["Null Count%"]}</TableCell>
                    <TableCell>{stats["Unique Values"]}</TableCell>
                    <TableCell>{stats["Unique Value%"]}</TableCell>
                    <TableCell>{stats["Average Length"]?.toFixed(2)}</TableCell>
                    <TableCell>{stats["Longest Length"]}</TableCell>
                    <TableCell>{stats["Shortest Length"]}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <Box sx={{ mt: 4, mb: 2 }}>
              <Typography variant="h6">Charts:</Typography>
              <Box display="flex" justifyContent="center" alignItems="center" gap={4}>
                <img
                  src={profileData?.Chart_Paths?.['Null Counts'] || "/charts/null_counts_column.png"}
                  alt="Null Counts per Column"
                  style={{ maxWidth: 400, borderRadius: 8, boxShadow: "0 2px 8px rgba(0,0,0,0.15)" }}
                />
                <img
                  src={profileData?.Chart_Paths?.['Unique Values'] || "/charts/unique_values_column.png"}
                  alt="Unique Values per Column"
                  style={{ maxWidth: 400, borderRadius: 8, boxShadow: "0 2px 8px rgba(0,0,0,0.15)" }}
                />
              </Box>
            </Box>
          </Box>
        )}
      </Paper>
    </Box>
  );
};

export default ProfilingPage;
