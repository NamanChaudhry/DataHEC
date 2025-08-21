// src/theme.js
import { createTheme } from "@mui/material/styles";

const theme = createTheme({
  palette: {
    primary: {
      main: "#1976d2",
      light: "#4791db",
      dark: "#115293",
    },
    secondary: {
      main: "#FFCD00", // EY Yellow
      light: "#FFD940",
      dark: "#DFB300",
    },
    background: {
      default: "#f5f5f5",
      paper: "#ffffff",
    },
    success: {
      main: "#4caf50",
    },
    error: {
      main: "#f44336",
    },
    warning: {
      main: "#ff9800",
    },
    info: {
      main: "#03a9f4",
    },
  },
  typography: {
    fontFamily: '"Segoe UI", "Helvetica", "Arial", sans-serif',
    h3: {
      fontWeight: 600,
    },
    h5: {
      fontWeight: 500,
    },
    button: {
      textTransform: "none",
      fontWeight: 500,
    },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          boxShadow: "0 2px 8px rgba(0, 0, 0, 0.1)",
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          boxShadow: "0 1px 3px rgba(0, 0, 0, 0.2)",
        },
        containedPrimary: {
          "&:hover": {
            backgroundColor: "#1565c0",
          },
        },
      },
    },
    MuiAccordion: {
      styleOverrides: {
        root: {
          boxShadow: "none",
          "&:before": {
            display: "none",
          },
          "&.Mui-expanded": {
            margin: 0,
          },
        },
      },
    },
    MuiAccordionSummary: {
      styleOverrides: {
        root: {
          minHeight: 56,
          "&.Mui-expanded": {
            minHeight: 56,
          },
        },
      },
    },
  },
});

export default theme;
