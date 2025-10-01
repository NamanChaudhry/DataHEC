import React, { useState } from "react";
import {
    Box,
    Stack,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    Button,
    Typography,
    IconButton,
    Table,
    TableBody,
    TableHead,
    TableRow,
    TableCell,
} from "@mui/material";
import UploadFileIcon from "@mui/icons-material/UploadFile";
import InsertDriveFileIcon from "@mui/icons-material/InsertDriveFile";
import CloseIcon from "@mui/icons-material/Close";
import * as XLSX from "xlsx";
import SaveIcon from "@mui/icons-material/Save";
import CallMergeIcon from '@mui/icons-material/CallMerge';

const MappingInputs = ({
    entity,
    sourceSystems,
    selectedSourceSystem,
    setSelectedSourceSystem,
    selectedFile,
    setSelectedFile,
    availableFiles,
    setAvailableFiles,
}) => {
    const [showTable, setShowTable] = useState(false);
    const [sourceColumns, setSourceColumns] = useState([]);
    const [targetColumnsMapping, setTargetColumnsMapping] = useState({});
    const [categoriesMapping, setCategoriesMapping] = useState({});

    const targetOptions = [
        "Customer ID",
        "Customer Name",
        "Tax ID",
        "DUNS Number",
        "Account Type",
        "Address",
        "City",
        "State",
        "Postal Code",
        "Country",
        "Phone",
        "Email",
        "Website",
        "Transaction Date",
    ];
    const categoryOptions = ["Header", "Address", "Contact"];

    const handleMapClick = () => {
        if (!selectedSourceSystem || !selectedFile) {
            alert("Please select a source system and upload a file before mapping.");
            return;
        }
        setShowTable(true);
    };

    const handleFileChange = (file) => {
        setSelectedFile(file.name);
        setAvailableFiles((prev) => [
            ...prev,
            {
                name: file.name,
                type: file.name.includes("_Output") ? "output" : "source",
                displayName: file.name,
            },
        ]);

        const reader = new FileReader();

        if (file.name.endsWith(".csv")) {
            reader.onload = (e) => {
                const text = e.target.result;
                const lines = text.split(/\r\n|\n/);
                const headers = lines[0].split(",");
                setSourceColumns(headers);
            };
            reader.readAsText(file);
        } else if (file.name.endsWith(".json")) {
            reader.onload = (e) => {
                const json = JSON.parse(e.target.result);
                const headers = Array.isArray(json) && json.length > 0 ? Object.keys(json[0]) : [];
                setSourceColumns(headers);
            };
            reader.readAsText(file);
        } else if (file.name.endsWith(".xlsx") || file.name.endsWith(".xls")) {
            reader.onload = (e) => {
                const data = new Uint8Array(e.target.result);
                const workbook = XLSX.read(data, { type: "array" });
                const sheetName = workbook.SheetNames[0];
                const worksheet = workbook.Sheets[sheetName];
                const json = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
                const headers = json.length > 0 ? json[0] : [];
                setSourceColumns(headers);
            };
            reader.readAsArrayBuffer(file);
        }
    };

    return (
        <Box sx={{ width: "100%", overflow: "visible" }}>
            <Stack spacing={3}>
                {/* Row: Source System + Upload File */}
                <Stack direction="row" spacing={3} alignItems="flex-start">
                    {/* Source System */}
                    <Box flex={1}>
                        <Typography
                            variant="subtitle1"
                            sx={{ fontWeight: 600, color: "#1976d2", mb: 1 }}
                        >
                            Source System
                        </Typography>
                        <FormControl
                            fullWidth
                            variant="outlined"
                            sx={{
                                "& .MuiOutlinedInput-root": {
                                    borderRadius: "12px",
                                    backgroundColor: "#2d284a",
                                    boxShadow: "0 2px 6px rgba(0,0,0,0.1)",
                                    "& fieldset": { borderColor: "#90caf9" },
                                    "&:hover fieldset": { borderColor: "#42a5f5" },
                                    "&.Mui-focused fieldset": {
                                        borderColor: "#1976d2",
                                        borderWidth: "2px",
                                    },
                                },
                                "& .MuiSelect-select": {
                                    padding: "14px",
                                    fontSize: "1rem",
                                    fontWeight: 500,
                                    color: "#a9a7a7ff",
                                    backgroundColor: "#2d284a",
                                    borderRadius: "12px",
                                },
                                "& .MuiSvgIcon-root": {
                                    color: "white",
                                },
                            }}
                        >
                            <InputLabel
                                sx={{
                                    fontWeight: 500,
                                    fontSize: 12,
                                    color: "#acc1d7ff",
                                }}
                            >
                                Select source system
                            </InputLabel>
                            <Select
                                value={selectedSourceSystem}
                                onChange={(e) => setSelectedSourceSystem(e.target.value)}
                                disabled={!entity}
                            >
                                <MenuItem value="">
                                    <em>Select source system...</em>
                                </MenuItem>
                                {sourceSystems.map((system) => (
                                    <MenuItem key={system} value={system}>
                                        {system}
                                    </MenuItem>
                                ))}
                            </Select>
                        </FormControl>
                    </Box>

                    {/* Upload File */}
                    <Box flex={1}>
                        <Typography
                            variant="subtitle1"
                            sx={{ fontWeight: 600, color: "#1976d2", mb: 1 }}
                        >
                            Upload File
                        </Typography>
                        <FormControl fullWidth>
                            <Button
                                variant="outlined"
                                component="label"
                                startIcon={<UploadFileIcon sx={{ color: "#FFA500" }} />}
                                sx={{
                                    padding: "12px 18px",
                                    border: "2px dashed #90caf9",
                                    backgroundColor: "#2d284a",
                                    color: "#FFA500",
                                    "&:hover": { backgroundColor: "#3a3b47", borderColor: "#7cb342" },
                                }}
                            >
                                {selectedSourceSystem
                                    ? `Upload File for ${selectedSourceSystem}`
                                    : "Upload File"}
                                <input
                                    type="file"
                                    hidden
                                    onChange={(e) => {
                                        const file = e.target.files[0];
                                        if (file) handleFileChange(file);
                                    }}
                                    accept=".csv,.xlsx,.json"
                                />
                            </Button>
                        </FormControl>

                        {selectedFile && (
                            <Box
                                sx={{
                                    width: "50%",
                                    mt: 1,
                                    p: 1,
                                    display: "flex",
                                    alignItems: "center",
                                    bgcolor: "#cacbccff",
                                    borderRadius: 2,
                                    ml: "auto",
                                }}
                            >
                                <InsertDriveFileIcon
                                    sx={{ fontSize: 28, color: "#527cb4ff", mr: 1.5 }}
                                />
                                <Box sx={{ flexGrow: 1 }}>
                                    <Typography
                                        variant="body2"
                                        title={selectedFile}
                                        sx={{ whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                                    >
                                        {selectedFile}
                                    </Typography>
                                </Box>
                                <IconButton size="small" sx={{ p: 0.5 }} onClick={() => setSelectedFile("")}>
                                    <CloseIcon fontSize="small" />
                                </IconButton>
                            </Box>
                        )}
                    </Box>
                </Stack>

                <Box sx={{ display: "flex", justifyContent: "center", mt: 2 }}>
                    <Button
                        variant="contained"
                        color="primary"
                        startIcon={<CallMergeIcon />}
                        disabled={!selectedSourceSystem || !selectedFile}
                        onClick={handleMapClick}
                        sx={{
                            borderRadius: "12px",
                            fontWeight: 600,
                            textTransform: "none",
                            px: 4,
                            py: 1.1,
                        }}
                    >
                        Map
                    </Button>
                </Box>

                {showTable && (
                    <>
                        <Table
                            sx={{
                                mt: 6,
                                width: "100%",
                                borderCollapse: "separate",
                                borderSpacing: 0,
                                border: "1px solid #90caf9",
                                borderRadius: 2,
                                overflow: "hidden",
                            }}
                        >
                            <TableHead >
                                <TableRow
                                    sx={{
                                        background: 'linear-gradient(135deg, #0d3965 0%, #205988 100%)',
                                        "& th": {
                                            color: "#fff",
                                            fontWeight: 700,
                                            fontSize: "0.95rem",
                                            borderBottom: "1px solid #90caf9",
                                        },
                                    }}
                                >
                                    <TableCell>Source Columns</TableCell>
                                    <TableCell>Target Columns</TableCell>
                                    <TableCell>Categories</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {sourceColumns.map((col, index) => (
                                    <TableRow
                                        key={index}
                                        sx={{
                                            backgroundColor: index % 2 === 0 ? "#f5f5f5" : "#e3f2fd",
                                            "&:hover": { backgroundColor: "#cce4ff" },
                                        }}
                                    >
                                        <TableCell sx={{ fontWeight: 500, fontSize: "0.9rem", padding: "8px" }}>
                                            {col}
                                        </TableCell>
                                        <TableCell sx={{ padding: "5px 8px" }}>
                                            <FormControl sx={{ width: "70%" }} size="small">
                                                <Select
                                                    value={targetColumnsMapping[col] || ""}
                                                    onChange={(e) =>
                                                        setTargetColumnsMapping((prev) => ({
                                                            ...prev,
                                                            [col]: e.target.value,
                                                        }))
                                                    }
                                                >
                                                    <MenuItem value="">
                                                        <em>Select target column...</em>
                                                    </MenuItem>
                                                    {targetOptions.map((opt) => (
                                                        <MenuItem key={opt} value={opt}>
                                                            {opt}
                                                        </MenuItem>
                                                    ))}
                                                </Select>
                                            </FormControl>
                                        </TableCell>
                                        <TableCell sx={{ padding: "4px 8px" }}>
                                            <FormControl sx={{ width: "80%" }} size="small">
                                                <Select
                                                    value={categoriesMapping[col] || ""}
                                                    onChange={(e) =>
                                                        setCategoriesMapping((prev) => ({
                                                            ...prev,
                                                            [col]: e.target.value,
                                                        }))
                                                    }
                                                >
                                                    <MenuItem value="">
                                                        <em>Select category...</em>
                                                    </MenuItem>
                                                    {categoryOptions.map((opt) => (
                                                        <MenuItem key={opt} value={opt}>
                                                            {opt}
                                                        </MenuItem>
                                                    ))}
                                                </Select>
                                            </FormControl>
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                        <Box sx={{ display: "flex", justifyContent: "center", mt: 3 }}>
                            <Button
                                variant="contained"
                                color="success"
                                startIcon={<SaveIcon />}
                                onClick={() => {
                                    console.log("Saving mappings...", { targetColumnsMapping, categoriesMapping });
                                    alert("Mappings saved!");
                                }}

                                sx={{
                                    bgcolor: '#123f6cff',
                                    color: '#ffffff',
                                    '&:hover': {
                                        bgcolor: '#1565c0',
                                    },
                                    '&.Mui-disabled': {
                                        bgcolor: '#1c3951ff',
                                        color: '#ffffff',
                                    },
                                }}
                            >
                                Save
                            </Button>
                        </Box>
                    </>
                )}

            </Stack>
        </Box>
    );
};

export default MappingInputs;
