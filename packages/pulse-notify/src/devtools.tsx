import { useState, useEffect } from "react";
import { __devtoolsGetConnections, __devtoolsSubscribe } from "./connectionPool.js";

export type DevToolsConnection = ReturnType<
  typeof __devtoolsGetConnections
>[number];

/**
 * OrbitalDevTools — React DevTools panel for monitoring active EventSource connections.
 *
 * Displays all active hooks and their connection state in real-time.
 * Dev-only component — tree-shakable in production builds.
 *
 * @example
 * ```tsx
 * import { __OrbitalDevTools } from "@orbital/pulse-notify";
 *
 * export function App() {
 *   return (
 *     <>
 *       <YourApp />
 *       {process.env.NODE_ENV === "development" && <__OrbitalDevTools />}
 *     </>
 *   );
 * }
 * ```
 */
export function __OrbitalDevTools() {
  const [connections, setConnections] = useState<DevToolsConnection[]>([]);

  useEffect(() => {
    // Initial snapshot
    setConnections(__devtoolsGetConnections());

    // Subscribe to updates
    const unsubscribe = __devtoolsSubscribe(() => {
      setConnections(__devtoolsGetConnections());
    });

    return unsubscribe;
  }, []);

  return (
    <div
      style={{
        position: "fixed",
        bottom: 0,
        right: 0,
        width: "500px",
        maxHeight: "400px",
        backgroundColor: "#1e1e1e",
        color: "#e0e0e0",
        border: "1px solid #333",
        borderRadius: "4px 4px 0 0",
        fontFamily: "monospace",
        fontSize: "12px",
        zIndex: 999999,
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        boxShadow: "0 -2px 8px rgba(0, 0, 0, 0.3)",
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: "8px 12px",
          backgroundColor: "#2d2d2d",
          borderBottom: "1px solid #444",
          fontWeight: "bold",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <span>🛰️ Orbital EventSource Connections ({connections.length})</span>
        <span style={{ fontSize: "10px", color: "#888" }}>
          {connections.reduce((sum, conn) => sum + conn.subscriberCount, 0)}{" "}
          subscribers
        </span>
      </div>

      {/* Connection List */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "8px",
        }}
      >
        {connections.length === 0 ? (
          <div style={{ padding: "20px 8px", textAlign: "center", color: "#888" }}>
            No active connections
          </div>
        ) : (
          connections.map((conn) => (
            <ConnectionItem key={conn.key} connection={conn} />
          ))
        )}
      </div>
    </div>
  );
}

function ConnectionItem({ connection }: { connection: DevToolsConnection }) {
  const lastEventText = connection.lastEventAt
    ? formatTimestamp(connection.lastEventAt)
    : "—";

  return (
    <div
      style={{
        marginBottom: "8px",
        padding: "8px",
        backgroundColor: "#252525",
        border: `1px solid ${connection.connected ? "#22c55e" : "#ef4444"}`,
        borderRadius: "3px",
      }}
    >
      {/* Status badge and address */}
      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" }}>
        <span
          style={{
            display: "inline-block",
            width: "8px",
            height: "8px",
            borderRadius: "50%",
            backgroundColor: connection.connected ? "#22c55e" : "#ef4444",
            flexShrink: 0,
          }}
        />
        <span style={{ fontWeight: "bold", color: "#60a5fa" }}>
          {connection.address}
        </span>
        <span style={{ color: "#888", fontSize: "11px" }}>
          ({connection.subscriberCount} subscriber{connection.subscriberCount !== 1 ? "s" : ""})
        </span>
      </div>

      {/* Server URL */}
      <div style={{ fontSize: "11px", color: "#888", marginBottom: "4px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        Server: {connection.serverUrl}
      </div>

      {/* Last event timestamp */}
      <div style={{ fontSize: "11px", color: "#888" }}>
        Last event: {lastEventText}
      </div>
    </div>
  );
}

function formatTimestamp(timestamp: number): string {
  const now = Date.now();
  const diff = now - timestamp;

  if (diff < 1000) {
    return "just now";
  }
  if (diff < 60000) {
    return `${Math.floor(diff / 1000)}s ago`;
  }
  if (diff < 3600000) {
    return `${Math.floor(diff / 60000)}m ago`;
  }

  const date = new Date(timestamp);
  return date.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
