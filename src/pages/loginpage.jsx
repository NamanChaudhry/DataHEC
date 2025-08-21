import React, { useState } from "react";

function AnimatedLogin() {
  const [username, setUsername] = useState("");
  const [isLoggedIn, setIsLoggedIn] = useState(false);

  const handleLogin = () => {
    if (username.trim()) {
      setIsLoggedIn(true);
    }
  };

  return (
    <div style={{ textAlign: "center", marginTop: "50px" }}>
      {!isLoggedIn ? (
        <div>
          <h1>Welcome Back!</h1>
          <input
            type="text"
            placeholder="Enter your name"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
          <button onClick={handleLogin}>Login</button>
        </div>
      ) : (
        <div style={{ animation: "fadeIn 1s" }}>
          <h1>Hi, {username}!</h1>
          <p>Let's get started!</p>
          <button>Start</button>
        </div>
      )}
    </div>
  );
}

export default AnimatedLogin;



