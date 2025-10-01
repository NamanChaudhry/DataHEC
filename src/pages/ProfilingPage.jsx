import React, { useState } from 'react';
import { Button, Typography, Box, CircularProgress, Table, TableHead, TableBody, TableRow, TableCell } from '@mui/material';
 
const ProfilingPage = () => {
  const [file, setFile] = useState(null);
  const [profileData, setProfileData] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
 
  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setProfileData(null);
    setError('');
  };
 
  const handleUpload = async () => {
    if (!file) {
      setError('Please select a file first');
      return;
    }
    setLoading(true);
    const formData = new FormData();
    formData.append('file', file);
 
    try {
      const response = await fetch('http://localhost:5001/api/profile-upload', {
        method: 'POST',
        body: formData,
      });
      if (!response.ok) {
        const errMsg = await response.text();
        throw new Error(errMsg || 'Upload failed');
      }
      const result = await response.json();
      setProfileData(result);
      setError('');
    } catch (err) {
      setError(err.message || 'Error uploading file');
      setProfileData(null);
    } finally {
      setLoading(false);
    }
  };
 
  return (
    <Box sx={{ maxWidth: 1100, margin: 'auto', p: 2 }}>
      {/* Banner Header */}
      <Box
        sx={{
          background: 'linear-gradient(90deg, #1976d2 80%, #42a5f5 100%)',
          color: 'white',
          borderRadius: 3,
          boxShadow: '0 4px 24px rgba(25,118,210,0.13)',
          display: 'flex',
          alignItems: { xs: 'flex-start', sm: 'center' },
          justifyContent: 'space-between',
          px: { xs: 2, md: 5 },
          py: { xs: 2, md: 3 },
          mb: 4,
        }}
      >
        <Box>
          <Typography variant="h5" sx={{ fontWeight: 600 }}>
            Data Profile Analysis
          </Typography>
          <Typography variant="subtitle1" sx={{ opacity: 0.92 }}>
            Comprehensive data profiling report with quality assessment and recommendations
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <input
            accept=".xls,.xlsx"
            type="file"
            id="upload-input"
            onChange={handleFileChange}
            style={{ display: "none" }}
          />
          <label htmlFor="upload-input">
            <Button
              variant="outlined"
              color="inherit"
              sx={{
                borderRadius: 2,
                bgcolor: "#fff",
                color: "#1976d2",
                fontWeight: 600,
                border: "2px solid #1976d2",
                minWidth: 130,
              }}
              component="span"
              disabled={loading}
            >
              Choose File
            </Button>
          </label>
          <Button
            variant="contained"
            sx={{
              ml: 2,
              borderRadius: 2,
              bgcolor: "#42a5f5",
              color: "white",
              fontWeight: 700,
              minWidth: 100,
              boxShadow: "0 2px 8px rgba(66,165,245,0.21)"
            }}
            onClick={handleUpload}
            disabled={loading || !file}
          >
            Profile
          </Button>
          {loading && <CircularProgress size={24} sx={{ ml: 2 }} />}
        </Box>
      </Box>
 
      {error && (
        <Typography color="error" sx={{ mt: 2 }}>
          {error}
        </Typography>
      )}
 
      {/* Stats Row */}
      {profileData && (
        <Box sx={{
          display: "grid",
          gridTemplateColumns: { xs: "1fr", sm: "repeat(4, 1fr)" },
          gap: 3,
          mb: 5
        }}>
          {[
            { title: "Source", value: profileData["Source"] ?? "--", icon: "📦" },
            { title: "Total Rows", value: profileData["Total Records"] ?? "--", icon: "📋" },
            { title: "Total Columns", value: profileData["Total Columns"] ?? "--", icon: "📊" },
            { title: "Anomalies", value: profileData["Anomalies Detected"] ?? 0, icon: "⚠️" }
          ].map((feature, idx) => (
            <Box key={idx} sx={{
              background: 'linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)',
              borderRadius: 2,
              border: "1.5px solid #90caf9",
              textAlign: "center",
              p: 3,
              boxShadow: "0 2.5px 18px rgba(33,150,243,.06)"
            }}>
              <Typography fontSize={28} mb={1}>{feature.icon}</Typography>
              <Typography fontSize={17} fontWeight={700}>{feature.title}</Typography>
              <Typography fontSize={29} fontWeight={700} color="#1976d2" mt={1}>{feature.value}</Typography>
            </Box>
          ))}
        </Box>
      )}
 
      {/* Profiling Table and Charts */}
      {profileData && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="h6" sx={{ mt: 3 }}>
            Column Profiling Stats:
          </Typography>
 
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
              ].map((header, idx) => (
                <TableCell key={header} sx={{ fontWeight: 700, color: "#1976d2", fontSize: 16 }}>
                  {header}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {Object.entries(profileData["Column Profiling"]).map(([col, stats], idx) => (
              <TableRow
                key={col}
                sx={{
                  backgroundColor: idx % 2 === 0 ? "#f6fbff" : "#ffffff"
                }}
              >
                <TableCell sx={{ fontWeight: 700 }}>{col}</TableCell>
                <TableCell>{stats["Total Count"]}</TableCell>
                <TableCell>{stats["Null Count"]}</TableCell>
                <TableCell>{stats["Null Count%"]}</TableCell>
                <TableCell>{stats["Unique Values"]}</TableCell>
                <TableCell>{stats["Unique Value%"]}</TableCell>
                <TableCell>{stats["Average Length"]}</TableCell>
                <TableCell>{stats["Longest Length"]}</TableCell>
                <TableCell>{stats["Shortest Length"]}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
 
          <Typography variant="h6" sx={{ mt: 4, mb: 2 }}>Charts:</Typography>
          <Box display="flex" justifyContent="center" alignItems="center" gap={4}>
            <Box>
              <img
                src="/charts/null_counts.png"
                alt="Null Counts per Column"
                style={{ maxWidth: 400, borderRadius: 8, boxShadow: "0 2px 8px rgba(0,0,0,0.15)" }}
              />
              <Typography align="center" sx={{ mt: 1 }}>
                Null Counts per Column
              </Typography>
            </Box>
            <Box>
              <img
                src="/charts/unique_values.png"
                alt="Unique Values per Column"
                style={{ maxWidth: 400, borderRadius: 8, boxShadow: "0 2px 8px rgba(0,0,0,0.15)" }}
              />
              <Typography align="center" sx={{ mt: 1 }}>
                Unique Values per Column
              </Typography>
            </Box>
          </Box>
        </Box>
      )}
    </Box>
  );
};
 
export default ProfilingPage; 
 