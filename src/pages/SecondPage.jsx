import React, { useState } from "react";
import "./SecondPage.css";
import MainContent from "../components/MainContent";
import ChevronRightIcon from '@mui/icons-material/ChevronRight';

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
      label: "Configure",
    children: [
      { label: "Source" },
      { label: "Data Domain" },
      { label: "Match Rule" },
      { label: "Merge Rule" },
    ],
  },
  {
    label: "Extract",
    children: [
    ],
  },


    {
    label: "Profile",
    children: [
    ],
    },
 {
      label: "Harmonize",
      onClick: (setActiveContent) => setActiveContent("thirdPage"),
    },
      {
    label: "Reports",
    children: [
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
        background: "#23233a",
        color: "#f9fafb",
        width: 180,
        borderRight: "20px solid #e0e0e0",
        boxSizing: "border-box",
        padding: 2,
        margin: 1,
        height: "calc(100vh - 16px)",
        border: "1px solid #e0e0e0",
        borderRadius: "16px",      },
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
                mb: 2,
                mx: 1,
                background: "#23233a",
                color: "#fff",
                mt: 1,
                border: "1px solid #fff",
                ":hover": {
                  background: "#ffd600",
                  color: "#23233a",
                  border: "3px solid #fff",
                }
              }}
              onClick={() => {
                // run only if this parent has an onClick (like Harmonize)
                if (parent.onClick) parent.onClick(setActiveContent);
              }}
            >
              <ChevronRightIcon sx={{ mr: 0.5 }} /> {/* Arrow in front */}
              <ListItemText primary={parent.label} />
              {/* {openMenus[parent.label] ? <ExpandLess /> : <ExpandMore />} */}
            </ListItemButton>

            <Collapse in={openMenus[parent.label]} timeout="auto" unmountOnExit>
              <List component="div" disablePadding>
                {parent.children &&
                  parent.children.length > 0 &&
                  parent.children.map((child) => (
                    <ListItemButton
                      key={child.label}
                      onClick={() => {
                        handleChildClick(child); // child logic
                        if (parent.onClick) parent.onClick(setActiveContent); // parent logic if defined
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
                          activeItem === child.label
                            ? "1px solid #3c3c4e"
                            : "none",
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
        {activeContent !== "default" && <MainContent />}

      </div>
    </div>
  );
};


export default SecondPage; 
