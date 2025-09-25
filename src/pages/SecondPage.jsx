import React, { useState } from "react";
import "./SecondPage.css";
import MainContent from "../components/MainContent";
import MatchRulePage from "../components/MatchRulePage";
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';

import {
  Drawer,
  List,
  ListItemButton,
  ListItemText,
  Collapse,
  Box,
  Typography,
} from "@mui/material";
import { ExpandLess, ExpandMore } from "@mui/icons-material";

const menuConfig = [
  {
    label: "Configure",
    children: [
      { label: "Source" },
      { label: "Data Domain" },
      {
        label: "Match Rule",
        onClick: (setActiveContent) => setActiveContent("matchRulePage"),
      },
      { label: "Merge Rule" },
    ],
  },
  {
    label: "Extract",
    children: [],
  },
  {
    label: "Profile",
    children: [],
  },
  {
    label: "Harmonize",
    onClick: (setActiveContent) => setActiveContent("thirdPage"),
  },
  {
    label: "Reports",
    children: [],
  },
];

const Sidebar = ({ activeContent, setActiveContent }) => {
  const [openMenus, setOpenMenus] = useState({
    ENTITY: false,
    ACTIVITY: false,
  });
  const [activeItem, setActiveItem] = useState("");

  const handleMouseEnter = (label) => {
    setOpenMenus((prev) => ({ ...prev, [label]: true }));
  };

  const handleMouseLeave = (label) => {
    setOpenMenus((prev) => ({ ...prev, [label]: false }));
  };

  const handleChildClick = (item) => {
    setActiveItem(item.label);
    if (item.onClick) item.onClick(setActiveContent);
  };

  return (
    <Drawer
      variant="permanent"
      PaperProps={{
        sx: {
          // background: "#23233a",
          background: '#272733',
          color: "#f9fafb",
          width: 180,
          borderRight: "20px solid #e0e0e0",
          boxSizing: "border-box",
          padding: 2,
          margin: 1,
          height: "calc(100vh - 16px)",
          border: "1px solid #e0e0e0",
          borderRadius: "16px",
        },
      }}
    >
      <Box
        sx={{
          p: 2,
          textAlign: "left",
          fontWeight: "bold",
          fontSize: 20,
          letterSpacing: 2,
          mb: 6,
        }}
      >
        <img
          src="/logo_EY.png"
          alt="EY Logo"
          style={{ height: "38px", width: "auto" }}
        />
      </Box>
      <List>
        {menuConfig.map((parent) => (
          <React.Fragment key={parent.label}>
            <Box
              onMouseEnter={() => handleMouseEnter(parent.label)}
              onMouseLeave={() => handleMouseLeave(parent.label)}
            >
              <ListItemButton
                sx={{
                  borderRadius: 2,
                  mb: 3,
                  mx: 0,
                  background: "#2d284a",
                  color: "#fff",
                  mt: 1,
                  ":hover": {
                    background: "#ffd600",
                    color: "#23233a",
                  },
                }}
                onClick={() => {
                  if (parent.onClick) parent.onClick(setActiveContent);
                }}
              >
                <ListItemText primary={parent.label} />
                <KeyboardArrowDownIcon sx={{ ml: 2 }} />
              </ListItemButton>

              <Collapse in={openMenus[parent.label]} timeout="auto" unmountOnExit>
                <List component="div" disablePadding>
                  {parent.children &&
                    parent.children.length > 0 &&
                    parent.children.map((child) => (
                      <ListItemButton
                        key={child.label}
                        onClick={() => {
                          handleChildClick(child);
                          if (parent.onClick) parent.onClick(setActiveContent);
                        }}
                        sx={{
                          ml: 1,
                          mb: 2,
                          borderRadius: 1,
                          background:
                            activeItem === child.label ? "#ffd600" : "transparent",
                          color: activeItem === child.label ? "#23233a" : "#fff",
                          "&:hover": {
                            background: "#ffd600",
                            color: "#23233a",
                            border: "1px solid #3c3c4e",
                          },
                          border:
                            activeItem === child.label ? "1px solid #3c3c4e" : "none",
                        }}
                      >
                        <ListItemText
                          primary={child.label}
                          primaryTypographyProps={{
                            fontSize: 13,
                          }}
                        />
                      </ListItemButton>
                    ))}
                </List>
              </Collapse>
            </Box>
          </React.Fragment>
        ))}
      </List>
    </Drawer>
  );
};

const SecondPage = () => {
  const [activeContent, setActiveContent] = useState("default");

  return (
    <div className="page-container">
      <Sidebar activeContent={activeContent} setActiveContent={setActiveContent} />

      <div className={activeContent === "thirdPage" ? "content content-full" : "content"}>
        {activeContent === "default" && (
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              textAlign: "center",
              py: 3,
              px: 4,
              maxWidth: "1200px",
              margin: "0 auto",
            }}
          >
            {/* Hero Section */}
            <Box sx={{ mb: 6 }}>
              <Typography
                variant="h2"
                component="h1"
                sx={{
                  fontWeight: 700,
                  fontSize: { xs: "2.5rem", md: "3.5rem" },
                  background: "linear-gradient(135deg, #1976d2 0%, #42a5f5 100%)",
                  backgroundClip: "text",
                  WebkitBackgroundClip: "text",
                  WebkitTextFillColor: "transparent",
                  mb: 2,
                }}
              >
                EY Data Harmonization
              </Typography>

              <Typography
                variant="h5"
                sx={{
                  color: "text.secondary",
                  fontWeight: 300,
                  mb: 4,
                  maxWidth: "600px",
                }}
              >
                Streamlining Data Harmonization Process
              </Typography>

              <Typography
                variant="body1"
                sx={{
                  fontSize: "1.1rem",
                  color: "text.primary",
                  maxWidth: "700px",
                  lineHeight: 1.7,
                  mb: 4,
                }}
              >
                Transform your data quality with our advanced harmonization platform.
                Process, clean, and unify data from multiple sources with enterprise-grade
                accuracy and efficiency.
              </Typography>
            </Box>

            {/* Features Grid */}
            <Box sx={{ width: "100%", mb: 6 }}>
              <Typography
                variant="h4"
                sx={{
                  fontWeight: 600,
                  mb: 4,
                  color: "primary.main",
                }}
              >
                Key Features
              </Typography>

              <Box
                sx={{
                  display: "grid",
                  gridTemplateColumns: { xs: "1fr", md: "1fr 1fr" },
                  gap: 4,
                  mb: 4,
                }}
              >
                {[
                  {
                    icon: "📊",
                    title: "Single File Mode",
                    description:
                      "Harmonize Single Source System files with intelligent deduplication and harmonization.",
                  },
                  {
                    icon: "🔄",
                    title: "Cross-System Mode",
                    description:
                      "Global deduplication across multiple data sources for comprehensive data unification.",
                  },
                  {
                    icon: "📈",
                    title: "Real-time Statistics",
                    description:
                      "Monitor processing progress with live analytics and detailed performance metrics.",
                  },
                  {
                    icon: "💾",
                    title: "Downloadable Results",
                    description:
                      "Export cleaned, harmonized data in multiple formats ready for immediate use.",
                  },
                ].map((feature, index) => (
                  <Box
                    key={index}
                    sx={{
                      p: 3,
                      borderRadius: 3,
                      background:
                        "linear-gradient(135deg, rgba(25, 118, 210, 0.05) 0%, rgba(66, 165, 245, 0.05) 100%)",
                      border: "1px solid rgba(25, 118, 210, 0.1)",
                      textAlign: "left",
                      transition: "transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out",
                      "&:hover": {
                        transform: "translateY(-2px)",
                        boxShadow: "0 8px 25px rgba(25, 118, 210, 0.15)",
                      },
                    }}
                  >
                    <Typography sx={{ fontSize: "2rem", mb: 1 }}>
                      {feature.icon}
                    </Typography>
                    <Typography
                      variant="h6"
                      sx={{ fontWeight: 600, mb: 1, color: "primary.main" }}
                    >
                      {feature.title}
                    </Typography>
                    <Typography
                      variant="body2"
                      sx={{ color: "text.secondary", lineHeight: 1.6 }}
                    >
                      {feature.description}
                    </Typography>
                  </Box>
                ))}
              </Box>
            </Box>

            {/* Getting Started Section */}
            <Box sx={{ width: "100%", maxWidth: "800px" }}>
              <Typography
                variant="h4"
                sx={{
                  fontWeight: 600,
                  mb: 4,
                  color: "primary.main",
                }}
              >
                Getting Started
              </Typography>

              <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
                {[
                  {
                    step: "01",
                    title: "Select an Entity",
                    description:
                      "Choose the type of data you want to process from our comprehensive entity library.",
                  },
                  {
                    step: "02",
                    title: "Upload Files",
                    description:
                      "Import your data files securely with support for multiple formats and sources.",
                  },
                  {
                    step: "03",
                    title: "Configure Rules",
                    description:
                      "Set up intelligent matching and merging rules tailored to your data requirements.",
                  },
                  {
                    step: "04",
                    title: "Process Data",
                    description:
                      "Execute harmonization and deduplication with real-time progress monitoring.",
                  },
                ].map((step, index) => (
                  <Box
                    key={index}
                    sx={{
                      display: "flex",
                      alignItems: "flex-start",
                      p: 3,
                      borderRadius: 2,
                      background: "rgba(255, 255, 255, 0.7)",
                      border: "1px solid rgba(0, 0, 0, 0.08)",
                      textAlign: "left",
                      transition: "all 0.2s ease-in-out",
                      "&:hover": {
                        background: "rgba(255, 255, 255, 0.9)",
                        border: "1px solid rgba(25, 118, 210, 0.2)",
                      },
                    }}
                  >
                    <Box
                      sx={{
                        minWidth: "50px",
                        height: "50px",
                        borderRadius: "50%",
                        background:
                          "linear-gradient(135deg, #1976d2 0%, #42a5f5 100%)",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        mr: 3,
                        mt: 0.5,
                      }}
                    >
                      <Typography
                        variant="h6"
                        sx={{ color: "white", fontWeight: 700 }}
                      >
                        {step.step}
                      </Typography>
                    </Box>
                    <Box>
                      <Typography
                        variant="h6"
                        sx={{ fontWeight: 600, mb: 1, color: "primary.main" }}
                      >
                        {step.title}
                      </Typography>
                      <Typography
                        variant="body2"
                        sx={{ color: "text.secondary", lineHeight: 1.6 }}
                      >
                        {step.description}
                      </Typography>
                    </Box>
                  </Box>
                ))}
              </Box>
            </Box>

            {/* CTA Section */}
            <Box sx={{ mt: 6, textAlign: "center" }}>
              <Typography variant="h6" sx={{ color: "text.secondary", mb: 2 }}>
                Ready to transform your data quality?
              </Typography>
              <Typography
                variant="body1"
                sx={{ color: "primary.main", fontWeight: 600 }}
              >
                Select a menu item from the sidebar to begin your data harmonization journey
              </Typography>
            </Box>
          </Box>
        )}

        {activeContent === "thirdPage" && <MainContent />}

        {activeContent === "matchRulePage" && <MatchRulePage />}
      </div>
    </div>
  );
};

export default SecondPage;

