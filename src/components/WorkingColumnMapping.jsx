// src/components/WorkingColumnMapping.jsx
import React from 'react';
import {
  Box,
  Typography,
  FormControlLabel,
  Checkbox,
  TextField,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Grid,
  Chip,
  Paper
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

const WorkingColumnMapping = ({
  columns = [],
  fuzzyColumns = [],
  exactColumns = [],
  thresholds = {},
  onMappingChange
}) => {
  const handleFuzzyToggle = (column) => {
    let newFuzzy = [...fuzzyColumns];
    let newExact = [...exactColumns];
    let newThresholds = { ...thresholds };

    if (newFuzzy.includes(column)) {
      newFuzzy = newFuzzy.filter(col => col !== column);
      delete newThresholds[column];
    } else {
      newFuzzy.push(column);
      newThresholds[column] = 90;
      newExact = newExact.filter(col => col !== column); // Remove from exact if selected
    }

    onMappingChange(newFuzzy, newExact, newThresholds);
  };

  const handleExactToggle = (column) => {
    let newExact = [...exactColumns];
    let newFuzzy = [...fuzzyColumns];
    let newThresholds = { ...thresholds };

    if (newExact.includes(column)) {
      newExact = newExact.filter(col => col !== column);
    } else {
      newExact.push(column);
      newFuzzy = newFuzzy.filter(col => col !== column); // Remove from fuzzy if selected
      delete newThresholds[column];
    }

    onMappingChange(newFuzzy, newExact, newThresholds);
  };

  const handleThresholdChange = (column, value) => {
    const newThresholds = { ...thresholds, [column]: parseInt(value) || 90 };
    onMappingChange(fuzzyColumns, exactColumns, newThresholds);
  };

  return (
    <Box>
      {/* Chips for selected columns */}
      {(fuzzyColumns.length > 0 || exactColumns.length > 0) && (
        <Box sx={{ mb: 2 }}>
          {fuzzyColumns.length > 0 && (
            <Paper elevation={1} sx={{ p: 2, mb: 2, backgroundColor: '#fffdf5' }}>
              <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                Selected Fuzzy Columns:
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                {fuzzyColumns.map(col => (
                  <Chip
                    key={`fuzzy-chip-${col}`}
                    label={
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <span style={{ fontSize: '13px' }}>Fuzzy: {col}</span>
                        <TextField
                          size="small"
                          value={thresholds[col] || 90}
                          onChange={(e) => handleThresholdChange(col, e.target.value)}
                          onClick={(e) => e.stopPropagation()}
                          sx={{
                            width: 50,
                            '& input': {
                              padding: '4px',
                              textAlign: 'center',
                              fontSize: '12px'
                            }
                          }}
                        />
                      </Box>
                    }
                    onDelete={() => handleFuzzyToggle(col)}
                    sx={{ bgcolor: '#fff8e1', borderColor: '#ffcd00' }}
                    variant="outlined"
                  />
                ))}
              </Box>
            </Paper>
          )}

          {exactColumns.length > 0 && (
            <Paper elevation={1} sx={{ p: 2, mb: 2, backgroundColor: '#f4faff' }}>
              <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                Selected Exact Columns:
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                {exactColumns.map(col => (
                  <Chip
                    key={`exact-chip-${col}`}
                    label={`Exact: ${col}`}
                    onDelete={() => handleExactToggle(col)}
                    sx={{ bgcolor: '#e3f2fd', fontSize: '13px' }}
                  />
                ))}
              </Box>
            </Paper>
          )}
        </Box>
      )}

      {/* Fuzzy Match Columns */}
      <Accordion defaultExpanded sx={{ mb: 2, border: '1px solid #ddd', borderRadius: 2, boxShadow: 1 }}>
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          sx={{ backgroundColor: '#f5f5f5', px: 2, py: 1 }}
        >
          <Typography variant="h6" fontSize={15} fontWeight={600}>
            Fuzzy Match Columns
          </Typography>
        </AccordionSummary>
        <AccordionDetails sx={{ px: 2, py: 2 }}>
          <Grid container spacing={2}>
            {columns.length === 0 ? (
              <Typography>No columns available</Typography>
            ) : (
              columns.map((col) => (
                <Grid item xs={6} sm={4} md={3} key={`fuzzy-${col}`}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={fuzzyColumns.includes(col)}
                        onChange={() => handleFuzzyToggle(col)}
                        size="small"
                        sx={{
                          color: '#FFCD00',
                          '&.Mui-checked': {
                            color: '#FFCD00'
                          }
                        }}
                      />
                    }
                    label={
                      <Typography sx={{ fontSize: '13px', fontWeight: 500 }}>
                        {col}
                      </Typography>
                    }
                    sx={{ ml: 0 }}
                  />
                </Grid>
              ))
            )}
          </Grid>
        </AccordionDetails>
      </Accordion>

      {/* Exact Match Columns */}
      <Accordion defaultExpanded sx={{ mb: 2, border: '1px solid #ddd', borderRadius: 2, boxShadow: 1 }}>
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          sx={{ backgroundColor: '#f5f5f5', px: 2, py: 1 }}
        >
          <Typography variant="h6" fontSize={15} fontWeight={600}>
            Exact Match Columns
          </Typography>
        </AccordionSummary>
        <AccordionDetails sx={{ px: 2, py: 2 }}>
          <Grid container spacing={2}>
            {columns.length === 0 ? (
              <Typography>No columns available</Typography>
            ) : (
              columns.map((col) => (
                <Grid item xs={6} sm={4} md={3} key={`exact-${col}`}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={exactColumns.includes(col)}
                        onChange={() => handleExactToggle(col)}
                        size="small"
                        sx={{
                          color: '#FFCD00',
                          '&.Mui-checked': {
                            color: '#FFCD00'
                          }
                        }}
                      />
                    }
                    label={
                      <Typography sx={{ fontSize: '13px', fontWeight: 500 }}>
                        {col}
                      </Typography>
                    }
                    sx={{ ml: 0 }}
                  />
                </Grid>
              ))
            )}
          </Grid>
        </AccordionDetails>
      </Accordion>
    </Box>
  );
};

export default WorkingColumnMapping;
