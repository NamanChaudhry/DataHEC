// src/components/WorkingColumnMapping.jsx
import React from 'react';
import IconButton from '@mui/material/IconButton';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import {
  Box,
  FormControlLabel,
  Checkbox,
  TextField,
  Accordion,
  FormControl,
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
import InputLabel from "@mui/material/InputLabel";


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
  onColumnMappingChange,
  sourceSystem,
  entity,
  selectedSourceSystem,
  setSelectedSourceSystem
}) => {
  const [aiMappedColumns, setAiMappedColumns] = useState({});
  const [columnMappings, setColumnMappings] = useState({});
  const [availableKeys, setAvailableKeys] = useState([]);
  const [openDialog, setOpenDialog] = useState(false);
  const [currentEditingColumn, setCurrentEditingColumn] = useState(null);
  const [showTable, setShowTable] = useState(false);
  const [targetColumnsMapping, setTargetColumnsMapping] = useState({});
  const [categoriesMapping, setCategoriesMapping] = useState({});
  const [tableRows, setTableRows] = useState([{ id: 1 }]);

  const targetOptions = [
    "Source System",
    "Customer Number",
    "Customer Name",
    "Customer Source Reference",
    "Taxpayer ID",
    "Taxpayer Registration Number",
    "Account Number",
    "Account Source Reference",
    "Account Type",
    "Account Description",
    "Account Established Date",
    "Customer Profile Class",
    "Capital IQ ID",
    "Transaction Activity Date",
    "Site Number",
    "Site Name",
    "Site Source Reference",
    "Account Address Set",
    "Location Source Reference",
    "Address Line 1",
    "Address Line 2",
    "Address Line 3",
    "Address Line 4",
    "City",
    "State",
    "Province",
    "Postal Code",
    "County",
    "Country",
    "Purpose",
    "Person Number",
    "Person Source Reference",
    "Salutary Introduction",
    "First Name",
    "Middle Name",
    "Last Name",
    "Job Title",
    "Responsibility Type",
    "Phone Number",
    "Phone Extension",
    "E-Mail Address",
    "Web URL"
  ];

  const categoryOptions = ["LTRIM", "RTRIM", "LOWER", "UPPER"];


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
        initialMapping[col] = matchedEntry ? matchedEntry[0] : "";
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

  const handleColumnMappingChange = (col, selectedCanonicalValue) => {
    const updatedMapping = {
      ...columnMappings,
      [col]: selectedCanonicalValue
    };

    setColumnMappings(updatedMapping);

    if (onColumnMappingChange) {
      onColumnMappingChange(updatedMapping);
    }
  };


  return (
    <Box>
      {/* Chips for selected columns */}
      {(fuzzyColumns.length > 0 || exactColumns.length > 0) && (
        <Box sx={{ mb: 2 }}>
          {/* {editable == false && columns.length > 0 && (
            <Box
              sx={{
                mt: 4,
                px: 4,
                py: 2,
                borderRadius: 3,
                backgroundColor: '#f7f7f7ff',
                border: '1px solid #e3e8ef',
                boxShadow: '0 2px 10px rgba(0, 0, 0, 0.04)'
              }}
            >
              <Typography
                variant="h6"
                fontWeight={600}
                sx={{
                  mb: 1,
                  fontSize: '1.4rem',
                  fontWeight:500,
                  color: '#1976d2',
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
                  borderSpacing: "5px 10px",
                  "& th": {
                    fontSize: "13px",
                    fontWeight: 700,
                    color: "black",
                    textTransform: "uppercase",
                    paddingBottom: 1
                  },
                  "& td": {
                    fontSize: "14px",
                    padding: "5px 3px",
                    //backgroundColor: "#736363ff",
                    borderRadius: "8px",
                    border: "0px solid #e0e0e0",
                    color: "black"
                  }
                }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ width: "38%", whiteSpace: "nowrap", color: "black" }}>Source Columns</TableCell>
                    <TableCell sx={{ color: "black" }}>Target Columns</TableCell>
                    <TableCell align="center" sx={{ width: "0%", whiteSpace: "nowrap", color: "black" }}>
                      Actions
                    </TableCell>
                  </TableRow>
                </TableHead>

                <TableBody>
                  {columns.map((col) => (
                    <TableRow key={`map-${col}`}>
                      <TableCell>{col}</TableCell>
                      <TableCell>
                        <span>{aiMappedColumns[columnMappings[col]] || "—"}</span>
                      </TableCell>
                      <TableCell align="center">
                        <IconButton
                          size="small"
                          sx={{ color: 'black' }}
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
          )} */}


          {editable === false && columns.length > 0 && (
            <>
              <Box
                sx={{
                  mt: 6,
                  px: 5,
                  py: 3,
                  borderRadius: 3,
                  backgroundColor: '#f7f7f7ff',
                  border: '1px solid #e3e8ef',
                  boxShadow: '0 4px 12px rgba(0, 0, 0, 0.06)',
                }}
              >
                <Typography
                  variant="h6"
                  fontWeight={600}
                  sx={{
                    mb: 1,
                    fontSize: '1.4rem',
                    fontWeight: 500,
                    color: '#1976d2',
                    letterSpacing: '0.3px'
                  }}
                >
                  Cleanse Section
                </Typography>

                <Table
                  sx={{
                    mt: 2,

                    width: '100%',
                    borderCollapse: 'separate',
                    borderSpacing: 0,
                    border: '1px solid #90caf9',
                    borderRadius: 2,
                    overflow: 'hidden',
                  }}
                >
                  <TableHead>
                    <TableRow
                      sx={{
                        background: 'linear-gradient(135deg, #0d3965 0%, #205988 100%)',
                        '& th': {
                          color: '#fff',
                          fontWeight: 700,
                          fontSize: '0.95rem',
                          borderBottom: '1px solid #90caf9',
                          textTransform: 'uppercase',

                        },
                      }}
                    >
                      <TableCell>Target Columns</TableCell>
                      <TableCell>Rules</TableCell>
                    </TableRow>
                  </TableHead>

                  <TableBody>
                    {tableRows.map((row, index) => (
                      <TableRow
                        key={row.id}
                        sx={{
                          backgroundColor: index % 2 === 0 ? '#f5f5f5' : '#e3f2fd',
                          '&:hover': { backgroundColor: '#cce4ff' },
                        }}
                      >
                        {/* Target Column Dropdown */}
                        <TableCell sx={{ padding: '10px 10px' }}>
                          <FormControl sx={{ width: '80%' }} size="small">
                            <Select
                              value={targetColumnsMapping[row.id] || ''}
                              onChange={(e) =>
                                setTargetColumnsMapping((prev) => ({
                                  ...prev,
                                  [row.id]: e.target.value,
                                }))
                              }
                              displayEmpty
                              MenuProps={{
                                PaperProps: {
                                  sx: {
                                    background: "linear-gradient(135deg, #0d3965 0%, #205988 100%)",
                                    color: 'white',
                                  },
                                },
                              }}
                            >
                              <MenuItem value="">
                                <em>Select target column...</em>
                              </MenuItem>
                              {targetOptions.map((opt) => (
                                <MenuItem
                                  key={opt}
                                  value={opt}
                                  sx={{
                                    '&.Mui-selected': {
                                      backgroundColor: '#1976d2',
                                      color: '#fff',
                                    },
                                    '&.Mui-selected:hover': {
                                      backgroundColor: '#1565c0',
                                    },
                                    '&:hover': {
                                      backgroundColor: '#1e4d7b',
                                    },
                                  }}
                                >
                                  {opt}
                                </MenuItem>
                              ))}
                            </Select>

                          </FormControl>
                        </TableCell>

                        {/* Category Dropdown */}
                        <TableCell
                          sx={{
                            padding: '10px 10px',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between'
                          }}
                        >
                          <FormControl sx={{ width: '85%' }} size="small">
                            <Select
                              multiple
                              displayEmpty
                              value={categoriesMapping[row.id] || []}
                              MenuProps={{
                                PaperProps: {
                                  sx: {
                                    background: "linear-gradient(135deg, #0d3965 0%, #205988 100%)",
                                    color: 'white',
                                  },
                                },
                              }}
                              onChange={(e) =>
                                setCategoriesMapping((prev) => ({
                                  ...prev,
                                  [row.id]: e.target.value,
                                }))
                              }
                              renderValue={(selected) => {
                                if (selected.length === 0) {
                                  return <em>Select rule...</em>;
                                }
                                return selected.join(', ');
                              }}
                            >
                              <MenuItem value="">
                                <em>Select rule...</em>
                              </MenuItem>
                              {categoryOptions.map((opt) => (
                                <MenuItem key={opt} value={opt}
                                  sx={{
                                    '&.Mui-selected': {
                                      backgroundColor: '#1976d2',
                                      color: '#fff',
                                    },
                                    '&.Mui-selected:hover': {
                                      backgroundColor: '#1565c0',
                                    },
                                    '&:hover': {
                                      backgroundColor: '#1e4d7b',
                                    },
                                  }}>
                                  <Checkbox
                                    checked={(categoriesMapping[row.id] || []).indexOf(opt) > -1}
                                    sx={{
                                      color: '#90caf9',
                                      '&.Mui-checked': {
                                        color: '#00e5ff',
                                      },
                                    }}
                                  />
                                  <Typography variant="body2">{opt}</Typography>
                                </MenuItem>
                              ))}
                            </Select>
                          </FormControl>

                          {/* Styled Remove Icon */}
                          <IconButton
                            onClick={() => setTableRows((prev) => prev.filter((r) => r.id !== row.id))}
                            sx={{
                              bgcolor: '#e8d4d7ff',
                              color: '#b02c22ff',
                              '&:hover': {
                                bgcolor: '#ffcdd2',
                              },
                            }}
                          >
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>


                </Table>
                <Box
                  sx={{
                    display: 'flex',
                    justifyContent: 'flex-end',
                    mt: 2
                  }}
                >
                  <Button
                    variant="contained"
                    size="small"
                    onClick={() =>
                      setTableRows((prev) => [...prev, { id: prev.length + 1 }])
                    }
                    sx={{
                      background: 'linear-gradient(135deg, #0d3965 0%, #205988 100%)',
                      color: '#fff',
                      '&:hover': { background: '#1565c0' },
                    }}
                  >
                    Add New Rule
                  </Button>
                </Box>
              </Box>
            </>
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
