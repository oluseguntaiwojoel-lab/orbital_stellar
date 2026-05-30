"use client";

import React, { useState } from "react";
import { useStellarEvent } from "@orbital/pulse-notify";

function HookRow({ serverUrl, address }: { serverUrl: string; address: string }) {
  const { event, connected, error } = useStellarEvent(serverUrl, address, { event: "*" });

  return (
    <div style={{ padding: 8, border: "1px solid #eee", borderRadius: 6, marginBottom: 8 }}>
      <div><strong>Address:</strong> {address}</div>
      <div><strong>Connected:</strong> {connected ? "yes" : "no"} {error ? ` — ${error}` : ""}</div>
      <div><strong>Last event:</strong> {event ? event.timestamp ?? JSON.stringify(event) : "—"}</div>
    </div>
  );
}

export default function Page() {
  const serverUrl = "/api/dev";
  const [addresses, setAddresses] = useState<string[]>(() => [
    "GDEV1EXAMPLEADDRESS0000000000000000000",
    "GDEV2EXAMPLEADDRESS0000000000000000000",
    "GDEV3EXAMPLEADDRESS0000000000000000000",
  ]);
  const [spawnCount, setSpawnCount] = useState(50);

  function spawnMany(n: number) {
    setAddresses((prev) => {
      const next = prev.slice();
      for (let i = 0; i < n; i++) {
        const id = `GDEMO${Date.now().toString(36)}${Math.random().toString(36).slice(2,8).toUpperCase()}`;
        next.push(id);
      }
      return next;
    });
  }

  function clearAll() {
    setAddresses([]);
  }

  return (
    <div style={{ padding: 24 }}>
      <h2>Devtools demo — multiple hooks</h2>
      <p>This page mounts several <strong>useStellarEvent</strong> hooks connected to the local dev SSE endpoint.</p>

      <div style={{ margin: "12px 0", display: "flex", gap: 8, alignItems: "center" }}>
        <label style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <span>Spawn count:</span>
          <input type="number" value={spawnCount} onChange={(e) => setSpawnCount(Number(e.target.value) || 0)} style={{ width: 96 }} />
        </label>
        <button onClick={() => spawnMany(spawnCount)} style={{ padding: "6px 12px" }}>Spawn</button>
        <button onClick={clearAll} style={{ padding: "6px 12px" }}>Clear</button>
        <div style={{ marginLeft: 12, color: "#666" }}>Current hooks: {addresses.length}</div>
      </div>

      <div>
        {addresses.map((a) => (
          <HookRow key={a} serverUrl={serverUrl} address={a} />
        ))}
      </div>
    </div>
  );
}
