import React, { useState, useEffect } from "react";
import { ThemeProvider } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import theme from "./theme";
import AnimatedLogin from "./pages/AnimatedLogin";
import SecondPage from "./pages/SecondPage";
import Sidebar from "./components/Sidebar";       // your third page parts
import MainContent from "./components/MainContent";

const App = () => {
  const [currentPage, setCurrentPage] = useState("login"); // 'login', 'second', 'third'

  useEffect(() => {
    console.log("App mounted. currentPage:", currentPage);
  }, [currentPage]);

  const handleStart = () => {
    console.log("App: handleStart called -> showing second page");
    setCurrentPage("second");
  };

  const handleConfigure = () => {
    console.log("App: handleConfigure called -> showing third page");
    setCurrentPage("third");
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {currentPage === "login" && <AnimatedLogin onStart={handleStart} />}
      {currentPage === "second" && (
        <SecondPage onConfigureClick={handleConfigure} />
      )}
      {currentPage === "third" && (
        <div style={{ display: "flex" }}>
          <Sidebar />
          <MainContent />
        </div>
      )}
    </ThemeProvider>
  );
};

export default App;

