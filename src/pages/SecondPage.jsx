import React, { useState } from "react";
import "./SecondPage.css";
import MainContent from "../components/MainContent";
import {
  Drawer,
  List,
  ListItemButton,
  ListItemText,
  Collapse,
  Box,
} from "@mui/material";
import { ExpandLess, ExpandMore } from "@mui/icons-material";

const menuConfig = [
  {
    label: "Entity",
    children: [
      { label: "Customer" },
      { label: "Item" },
      { label: "Supplier" },
    ],
  },
  {
    label: "Activity",
    children: [
      {
        label: "Configure Harmonization",
        onClick: (setActiveContent) => setActiveContent("thirdPage"),
      },
      { label: "Reports" },
      { label: "Match Rule" },
      { label: "Metadata" },
      { label: "Merge Rules" },
      { label: "Reconciliation" },
      { label: "Cross" },
    ],
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
        background: "#2c2c3a",
        color: "#fff",
        width: 180,
        borderRight: "1px solid #e0e0e0",
        boxSizing: "border-box",
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
        mb: 6
      }}
    >
        <img
    src="/logo_EY.png"
    alt="EY Logo"
    style={{ height: "48px", width: "auto" }}
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
                mb: 1,
                mx: 1,
                background: "#393961ff",
                color: "#fff",
                mt: 3,
              }}
            >
              <ListItemText primary={parent.label} />
              {openMenus[parent.label] ? <ExpandLess /> : <ExpandMore />}
            </ListItemButton>

            <Collapse in={openMenus[parent.label]} timeout="auto" unmountOnExit>
              <List component="div" disablePadding>
                {parent.children.map((child) => (
                  <ListItemButton
                    key={child.label}
                    onClick={() => handleChildClick(child)}
                    sx={{
                      ml: 3,
                      mb: 1,
                      borderRadius: 2,
                      background:
                        activeItem === child.label ? "#ffd600" : "transparent",
                      color: activeItem === child.label ? "#23233a" : "#fff",
                      "&:hover": {
                        background: "#ffd600",
                        color: "#23233a",
                      },
                    }}
                  >
                    <ListItemText
                      primary={child.label}
                      primaryTypographyProps={{
                        fontSize: 15,
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
        {activeContent === "default" ? (
          <>
            <h1>DataHEC</h1>
            <h2>How to Use This Tool</h2>
            <ol>
              <li>
                <b>Single File Mode:</b> Process individual files and generate
                outputs that can be reused later
              </li>
              <li>
                <b>Cross-System Mode:</b> Select multiple files for global
                deduplication
              </li>
              <li>
                <b>Workflow:</b> Process files individually first, then use those
                outputs in cross-system mode for comprehensive deduplication
              </li>
            </ol>
          </>
        ) : (
          <MainContent />
        )}
      </div>
    </div>
  );
};


export default SecondPage; 
