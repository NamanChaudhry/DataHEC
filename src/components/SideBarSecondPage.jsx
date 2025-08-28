import React, { useState } from "react";

const SidebarSecondPage = () => {
  const [openMenu, setOpenMenu] = useState(null);

  const toggleMenu = (menu) => {
    setOpenMenu(openMenu === menu ? null : menu);
  };

  return (
    <div className="h-screen w-64 bg-[#2c2c3a] text-white flex flex-col shadow-lg">
      {/* Menu Items */}
      <div className="flex-1 mt-4">
        {/* Entity */}
        <div>
          <button
            className="w-full flex justify-between items-center px-4 py-2 text-left hover:bg-gray-700"
            onClick={() => toggleMenu("entity")}
          >
            <span className="font-medium">Entity</span>
            {/* Dropdown indicator can be added later */}
          </button>
          {openMenu === "entity" && (
            <div className="pl-8">
              <button className="block w-full text-left px-2 py-1 hover:bg-gray-700">
                Entity 1
              </button>
              <button className="block w-full text-left px-2 py-1 hover:bg-gray-700">
                Entity 2
              </button>
            </div>
          )}
        </div>

        {/* Activity */}
        <div>
          <button
            className="w-full flex justify-between items-center px-4 py-2 text-left hover:bg-gray-700"
            onClick={() => toggleMenu("activity")}
          >
            <span className="font-medium">Activity</span>
            {/* Dropdown indicator can be added later */}
          </button>
          {openMenu === "activity" && (
            <div className="pl-8">
              <button className="block w-full text-left px-2 py-1 hover:bg-gray-700">
                Upload Files
              </button>
              <button className="block w-full text-left px-2 py-1 hover:bg-gray-700">
                Process Data
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default SidebarSecondPage;

