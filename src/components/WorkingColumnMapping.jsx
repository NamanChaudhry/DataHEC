// src/components/WorkingColumnMapping.jsx
import React from 'react';
import IconButton from '@mui/material/IconButton';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import {
  Box,
  FormControlLabel,
  Checkbox,
  TextField,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Grid,
  Chip,
  Paper,
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  Select,
  MenuItem
} from '@mui/material';

import Dialog from '@mui/material/Dialog';
import DialogTitle from '@mui/material/DialogTitle';
import DialogContent from '@mui/material/DialogContent';
import DialogActions from '@mui/material/DialogActions';
import Button from '@mui/material/Button';
import RadioGroup from '@mui/material/RadioGroup';
import Radio from '@mui/material/Radio';
import Divider from '@mui/material/Divider';
import Typography from '@mui/material/Typography';

import { useState, useEffect } from 'react';
import axios from 'axios';

import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

const WorkingColumnMapping = ({
  editable = false,
  columns = [],
  fuzzyColumns = [],
  exactColumns = [],
  thresholds = {},
  onMappingChange,
  onColumnMappingChange
}) => {
  const [aiMappedColumns, setAiMappedColumns] = useState({});
  const [columnMappings, setColumnMappings] = useState({});
  const [availableKeys, setAvailableKeys] = useState([]);
  const [openDialog, setOpenDialog] = useState(false);
  const [currentEditingColumn, setCurrentEditingColumn] = useState(null);


  async function fetchMapping() {
    try {
      const response = await axios.get("http://localhost:5001/api/header-mapping");
      const mapping = response.data;

      if (!mapping || mapping.error) {
        console.error("Invalid mapping received:", mapping);
        setAiMappedColumns({});
        setAvailableKeys([]);
        setColumnMappings({});
        return;
      }

      setAiMappedColumns(mapping);

      setAvailableKeys(Object.keys(mapping));

      const normalize = (str) =>
        str.toLowerCase().replace(/[_\s]+/g, "");

      const initialMapping = {};
      columns.forEach((col) => {
        const matchedEntry = Object.entries(mapping).find(
          ([key]) => key.toLowerCase().replace(/[_\s]+/g, "") === col.toLowerCase().replace(/[_\s]+/g, "")
        );
        initialMapping[col] = matchedEntry ? matchedEntry[1] : ""; // <-- use value instead of key
      });
      setColumnMappings(initialMapping);


      console.log("Header Mapping API response:", mapping);
      console.log(mapping);
    } catch (error) {
      console.error("Error fetching header mapping:", error);
    }
  }

  useEffect(() => {
    if (columns.length > 0) {
      fetchMapping();
    }
  }, [columns]);

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
  const handleColumnMappingChange = (col, selectedKey) => {
    setColumnMappings((prev) => ({
      ...prev,
      [col]: selectedKey
    }));

    if (onColumnMappingChange) {
      onColumnMappingChange({
        ...columnMappings,
        [col]: selectedKey
      });
    }
  };

  return (
    <Box>
      {/* Chips for selected columns */}
      {(fuzzyColumns.length > 0 || exactColumns.length > 0) && (
        <Box sx={{ mb: 2 }}>
          {editable == false && columns.length > 0 && (
            <Box
              sx={{
                mt: 4,
                px: 4,
                py: 4,
                borderRadius: 3,
                backgroundColor: '#f9fafc',
                border: '1px solid #e3e8ef',
                boxShadow: '0 2px 10px rgba(0, 0, 0, 0.04)'
              }}
            >
              <Typography
                variant="h6"
                fontWeight={600}
                sx={{
                  mb: 3,
                  fontSize: '1.3rem',
                  color: '#1a1a1a',
                  letterSpacing: '0.3px'
                }}
              >
                Column Mapping
              </Typography>

              <Table
                size="small"
                sx={{
                  width: "100%",
                  borderCollapse: "separate",
                  borderSpacing: "10px 10px",
                  "& th": {
                    fontSize: "13px",
                    fontWeight: 700,
                    color: "#555",
                    textTransform: "uppercase",
                    paddingBottom: 1
                  },
                  "& td": {
                    fontSize: "14px",
                    padding: "7px 7px",
                    backgroundColor: "#ffffff",
                    borderRadius: "8px",
                    border: "1px solid #e0e0e0",
                    color: "#333"
                  }
                }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ width: "38%", whiteSpace: "nowrap" }}>Source Columns</TableCell>
                    <TableCell>Target Columns</TableCell>
                    <TableCell align="center" sx={{ width: "0%", whiteSpace: "nowrap" }}>
                      Actions
                    </TableCell>
                  </TableRow>
                </TableHead>

                <TableBody>
                  {columns.map((col) => (
                    <TableRow key={`map-${col}`}>
                      <TableCell>{col}</TableCell>
                      <TableCell>
                        <span>{columnMappings[col] || "—"}</span>
                      </TableCell>
                      <TableCell align="center">
                        <IconButton
                          size="small"
                          onClick={() => {
                            setCurrentEditingColumn(col);
                            setOpenDialog(true);
                          }}
                        >
                          <EditOutlinedIcon fontSize="small" />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Box>
          )}

          {editable == true && fuzzyColumns.length > 0 && (
            <Paper elevation={1} sx={{ p: 2, mb: 2, mt: 2, backgroundColor: '#fffdf5' }}>
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

          {editable == true && exactColumns.length > 0 && (
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
      {editable == true && (
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
      )}

      {/* Exact Match Columns */}
      {editable == true && (
        <Accordion defaultExpanded sx={{ mb: 2, mt: 1, border: '1px solid #ddd', borderRadius: 2, boxShadow: 1 }}>
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
      )}

      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        maxWidth="xs"
        fullWidth
      >
        <DialogTitle>
          Select Mapping for <span style={{ color: '#1976d2' }}>{currentEditingColumn}</span>
        </DialogTitle>
        <Divider />
        <DialogContent sx={{ py: 3 }}>
          <RadioGroup
            value={columnMappings[currentEditingColumn] || ""}
            onChange={(e) => {
              handleColumnMappingChange(currentEditingColumn, e.target.value);
              setOpenDialog(false);
            }}
          >
            {Object.entries(aiMappedColumns).map(([key, value]) => (
              <FormControlLabel
                key={key}
                value={key}
                control={<Radio />}
                label={<Typography sx={{ fontSize: 15 }}>{value}</Typography>}
                sx={{ mb: 1 }}
              />
            ))}

          </RadioGroup>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)} color="inherit" variant="outlined">
            Cancel
          </Button>
        </DialogActions>
      </Dialog>

    </Box>

  );

};

export default WorkingColumnMapping;

