"use client";

import React, { useEffect, useState } from "react";

export default function DevtoolsMount(): JSX.Element | null {
  const [Dev, setDev] = useState<React.ComponentType | null>(null);

  useEffect(() => {
    if (process.env.NODE_ENV === "production") return;
    let mounted = true;
    import("@orbital/pulse-notify")
      .then((mod) => {
        if (mounted && mod && mod.PulseNotifyDevtools) setDev(() => mod.PulseNotifyDevtools);
      })
      .catch(() => {});
    return () => {
      mounted = false;
    };
  }, []);

  if (!Dev) return null;

  return (
    <div style={{ position: "fixed", right: 12, bottom: 12, zIndex: 99999 }}>
      <Dev />
    </div>
  );
}
