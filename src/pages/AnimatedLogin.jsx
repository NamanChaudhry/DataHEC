import React, { useEffect, useState } from "react";

const AnimatedLogin = ({ onStart }) => {
  const [fadeIn, setFadeIn] = useState(false);
  const [fadeOut, setFadeOut] = useState(false);

  useEffect(() => {
    const t = setTimeout(() => setFadeIn(true), 100);
    return () => clearTimeout(t);
  }, []);

const handleStart = () => {
  console.log("AnimatedLogin: Start clicked, onStart:", typeof onStart);
  setFadeOut(true);

  setTimeout(() => {
    if (typeof onStart === "function") {
      onStart(); // this will trigger App.jsx -> setShowMain(true)
    }
  }, 500); // wait for fadeOut animation
};


  const styles = {
    wrapper: {
      opacity: fadeOut ? 0 : 1,
      transition: "opacity 500ms ease",
    },
    background: {
      backgroundImage: `url(${require("./homePageBg.png")})`,
      backgroundSize: "cover",
      backgroundPosition: "center",
      height: "100vh",
      display: "flex",
      flexDirection: "column",
      justifyContent: "center",
      alignItems: "center",
      color: "#fff",
      position: "relative",
    },
    heading: {
      fontSize: "3rem",
      fontWeight: "bold",
      marginBottom: "20px",
      opacity: fadeIn ? 1 : 0,
      transform: fadeIn ? "translateY(0)" : "translateY(-20px)",
      transition: "opacity 600ms ease, transform 600ms ease",
    },
    startBtn: {
      padding: "10px 30px",
      fontSize: "1.2rem",
      backgroundColor: "#ffd500",
      border: "none",
      borderRadius: "5px",
      cursor: "pointer",
      opacity: fadeIn ? 1 : 0,
      transform: fadeIn ? "translateY(0)" : "translateY(20px)",
      transition: "opacity 600ms ease 150ms, transform 600ms ease 150ms",
    },
    footerLeft: {
      position: "absolute",
      bottom: "10px",
      left: "10px",
      fontSize: "0.9rem",
    },
    footerRight: {
      position: "absolute",
      bottom: "10px",
      right: "10px",
    },
  };

  return (
    <div style={styles.wrapper}>
      <div style={styles.background}>
        <h1 style={styles.heading}>Good Afternoon,</h1>

        <button
          style={styles.startBtn}
          onClick={handleStart}
          onMouseOver={(e) => (e.target.style.backgroundColor = "#e6c200")}
          onMouseOut={(e) => (e.target.style.backgroundColor = "#ffd500")}
        >
          Start
        </button>

        <div style={styles.footerLeft}>&copy;Copyright@EY 2024</div>
        <div style={styles.footerRight}>EY</div>
      </div>
    </div>
  );
};

export default AnimatedLogin;
