import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { Activity, AlertTriangle, CheckCircle2, Clock, RefreshCw, Send } from "lucide-react";
import "./styles.css";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

type Severity = "P0" | "P1" | "P2" | "P3" | "P4";
type Status = "OPEN" | "INVESTIGATING" | "RESOLVED" | "CLOSED";

type Incident = {
  id: string;
  component_id: string;
  status: Status;
  severity: Severity;
  start_time: string;
  end_time?: string | null;
  resolved_at?: string | null;
  mttr_seconds?: number | null;
  signal_count?: number;
  rca?: RCA | null;
};

// Paginated response shape from GET /incidents/active
type PaginatedIncidents = {
  items: Incident[];
  total: number;
  page: number;
  page_size: number;
};

type RCA = {
  id: string;
  work_item_id: string;
  root_cause_category: string;
  fix_applied: string;
  prevention_steps: string;
  submitted_at: string;
};

type Signal = {
  work_item_id: string;
  component_id: string;
  timestamp: string;
  severity: Severity;
  payload: Record<string, unknown>;
};

const statusOrder: Status[] = ["OPEN", "INVESTIGATING", "RESOLVED", "CLOSED"];
const categories = ["Capacity", "Dependency Failure", "Configuration", "Regression", "Network", "Unknown"];

async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(options?.headers ?? {}) },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail ?? `Request failed with ${response.status}`);
  }
  return response.json();
}

function formatDuration(seconds?: number | null) {
  if (seconds === null || seconds === undefined) return "Pending";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  if (seconds < 3600) return `${mins}m ${secs}s`;
  const hours = Math.floor(seconds / 3600);
  const remainMins = Math.floor((seconds % 3600) / 60);
  return `${hours}h ${remainMins}m`;
}

function toDateTimeLocal(value?: string | null) {
  const date = value ? new Date(value) : new Date();
  const offset = date.getTimezoneOffset() * 60000;
  return new Date(date.getTime() - offset).toISOString().slice(0, 16);
}

function App() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<Incident | null>(null);
  const [signals, setSignals] = useState<Signal[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [rca, setRca] = useState({
    root_cause_category: categories[0],
    fix_applied: "",
    prevention_steps: "",
    submitted_at: toDateTimeLocal(),
  });

  async function refresh() {
    try {
      setError(null);
      // API now returns PaginatedIncidents — extract .items
      const response = await api<PaginatedIncidents | Incident[]>("/incidents/active");
      const active: Incident[] = Array.isArray(response)
        ? response
        : (response as PaginatedIncidents).items ?? [];
      setIncidents(active);
      if (!selectedId && active.length > 0) setSelectedId(active[0].id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load active incidents");
    }
  }

  async function loadDetail(id: string) {
    const [item, rawSignals] = await Promise.all([
      api<Incident>(`/incidents/${id}`),
      api<Signal[]>(`/incidents/${id}/signals`),
    ]);
    setDetail(item);
    setSignals(rawSignals);
    setRca({
      root_cause_category: item.rca?.root_cause_category ?? categories[0],
      fix_applied: item.rca?.fix_applied ?? "",
      prevention_steps: item.rca?.prevention_steps ?? "",
      submitted_at: toDateTimeLocal(item.rca?.submitted_at ?? item.end_time),
    });
  }

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 3000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!selectedId) return;
    loadDetail(selectedId).catch((err) =>
      setError(err instanceof Error ? err.message : "Failed to load incident")
    );
  }, [selectedId]);

  const nextStatus = useMemo(() => {
    if (!detail) return null;
    const index = statusOrder.indexOf(detail.status);
    return statusOrder[index + 1] ?? null;
  }, [detail]);

  async function transitionStatus(status: Status) {
    if (!detail) return;
    setBusy(true);
    try {
      const updated = await api<Incident>(`/incidents/${detail.id}/status`, {
        method: "PATCH",
        body: JSON.stringify({ status }),
      });
      setDetail(updated);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Status update failed");
    } finally {
      setBusy(false);
    }
  }

  async function submitRca(closeAfter = false) {
    if (!detail) return;
    setBusy(true);
    try {
      // Step 1: Submit RCA
      await api<RCA>(`/incidents/${detail.id}/rca`, {
        method: "POST",
        body: JSON.stringify({
          ...rca,
          submitted_at: new Date(rca.submitted_at).toISOString(),
        }),
      });

      // Step 2: If closeAfter, transition to CLOSED via PATCH /status
      // /close endpoint removed — all transitions go through PATCH /status
      if (closeAfter && detail.status === "RESOLVED") {
        await api<Incident>(`/incidents/${detail.id}/status`, {
          method: "PATCH",
          body: JSON.stringify({ status: "CLOSED" }),
        });
      }

      // Step 3: Reload detail
      const updated = await api<Incident>(`/incidents/${detail.id}`);
      setDetail(updated);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "RCA submission failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="shell">
      <section className="sidebar">
        <div className="brand">
          <Activity size={24} />
          <div>
            <h1>IMS</h1>
            <span>Active incident command</span>
          </div>
        </div>
        <button className="iconButton" onClick={refresh} aria-label="Refresh incidents" title="Refresh incidents">
          <RefreshCw size={18} />
        </button>
        <div className="incidentList">
          {incidents.map((incident) => (
            <button
              key={incident.id}
              className={`incidentRow ${incident.id === selectedId ? "selected" : ""}`}
              onClick={() => setSelectedId(incident.id)}
            >
              <span className={`severity ${incident.severity}`}>{incident.severity}</span>
              <span>
                <strong>{incident.component_id}</strong>
                <small>{incident.status}</small>
              </span>
              {incident.signal_count !== undefined && incident.signal_count > 0 && (
                <small style={{ marginLeft: "auto", opacity: 0.6 }}>{incident.signal_count} signals</small>
              )}
            </button>
          ))}
          {incidents.length === 0 && <p className="empty">No active incidents.</p>}
        </div>
      </section>

      <section className="content">
        {error && (
          <div className="banner">
            <AlertTriangle size={18} />
            {error}
          </div>
        )}

        {detail ? (
          <>
            <div className="incidentHeader">
              <div>
                <span className={`severity ${detail.severity}`}>{detail.severity}</span>
                <h2>{detail.component_id}</h2>
                <p>{detail.id}</p>
              </div>
              <div className="statusPanel">
                <span>{detail.status}</span>
                <small>MTTR {formatDuration(detail.mttr_seconds)}</small>
                {detail.signal_count !== undefined && (
                  <small>{detail.signal_count} signals</small>
                )}
              </div>
            </div>

            <div className="toolbar">
              {nextStatus && (
                <button disabled={busy} onClick={() => transitionStatus(nextStatus)}>
                  <CheckCircle2 size={17} />
                  Move to {nextStatus}
                </button>
              )}
              <span>
                <Clock size={16} />
                {new Date(detail.start_time).toLocaleString()}
              </span>
              {detail.resolved_at && (
                <span>
                  <CheckCircle2 size={16} />
                  Resolved {new Date(detail.resolved_at).toLocaleString()}
                </span>
              )}
            </div>

            <div className="grid">
              <section className="panel">
                <h3>Raw Signals</h3>
                <div className="signals">
                  {signals.map((signal, index) => (
                    <article key={`${signal.timestamp}-${index}`} className="signal">
                      <div>
                        <span className={`severity ${signal.severity}`}>{signal.severity}</span>
                        <time>{new Date(signal.timestamp).toLocaleString()}</time>
                      </div>
                      <pre>{JSON.stringify(signal.payload, null, 2)}</pre>
                    </article>
                  ))}
                  {signals.length === 0 && <p className="empty">Signals are still being persisted.</p>}
                </div>
              </section>

              <section className="panel">
                <h3>Root Cause Analysis</h3>
                <label>
                  Incident Start
                  <input value={toDateTimeLocal(detail.start_time)} readOnly type="datetime-local" />
                </label>
                <label>
                  Incident End
                  <input
                    value={rca.submitted_at}
                    type="datetime-local"
                    onChange={(event) => setRca({ ...rca, submitted_at: event.target.value })}
                  />
                </label>
                <label>
                  Category
                  <select
                    value={rca.root_cause_category}
                    onChange={(event) => setRca({ ...rca, root_cause_category: event.target.value })}
                  >
                    {categories.map((category) => (
                      <option key={category}>{category}</option>
                    ))}
                  </select>
                </label>
                <label>
                  Fix Applied
                  <textarea
                    value={rca.fix_applied}
                    onChange={(event) => setRca({ ...rca, fix_applied: event.target.value })}
                  />
                </label>
                <label>
                  Prevention Steps
                  <textarea
                    value={rca.prevention_steps}
                    onChange={(event) => setRca({ ...rca, prevention_steps: event.target.value })}
                  />
                </label>
                <div className="actions">
                  <button disabled={busy} onClick={() => submitRca(false)}>
                    <Send size={17} />
                    Submit RCA
                  </button>
                  <button disabled={busy || detail.status !== "RESOLVED"} onClick={() => submitRca(true)}>
                    <CheckCircle2 size={17} />
                    Submit & Close
                  </button>
                </div>
              </section>
            </div>
          </>
        ) : (
          <div className="emptyState">Waiting for incidents from the ingestion stream.</div>
        )}
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);