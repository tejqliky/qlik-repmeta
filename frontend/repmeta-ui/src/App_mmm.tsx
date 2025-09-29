import React, { useEffect, useMemo, useRef, useState } from "react";

/** Types */
type Customer = { customer_id: number; customer_name: string };
type Server = { server_id: number; server_name: string; environment?: string };

/** ---- API base: force same-origin proxy in Docker/VM ---- */
const DEFAULT_API = "/api";
const configuredApi = (import.meta.env.VITE_API_BASE_URL ?? DEFAULT_API).trim();
const API_BASE = (/^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?/i.test(configuredApi) ? DEFAULT_API : configuredApi)
  .replace(/\/+$/, "");

/** ---- utils ---- */
async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const j = await res.json();
      // @ts-ignore
      detail = j?.detail ? j.detail : JSON.stringify(j);
    } catch {}
    throw new Error(detail);
  }
  return (await res.json()) as T;
}

function toast(msg: string, kind: "ok" | "err" = "ok") {
  const el = document.createElement("div");
  el.textContent = msg;
  el.style.position = "fixed";
  el.style.left = "50%";
  el.style.transform = "translateX(-50%)";
  el.style.bottom = "24px";
  el.style.padding = "10px 14px";
  el.style.borderRadius = "10px";
  el.style.fontSize = "14px";
  el.style.color = kind === "ok" ? "#064e3b" : "#7f1d1d";
  el.style.background = kind === "ok" ? "#d1fae5" : "#fee2e2";
  el.style.boxShadow = "0 8px 24px rgba(0,0,0,.18)";
  el.style.zIndex = "9999";
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 3200);
}

/** ---- small UI bits ---- */
function StatCard({ label, value }: { label: string; value: any }) {
  return (
    <div className="rounded-xl border bg-white p-3">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="text-xl font-semibold">{value ?? "—"}</div>
    </div>
  );
}

function Tip({
  text,
  children,
  className = "",
  side = "bottom",
}: {
  text: string;
  children: React.ReactNode;
  className?: string;
  side?: "top" | "bottom";
}) {
  const [open, setOpen] = useState(false);
  const pos =
    side === "top"
      ? "bottom-full mb-2 left-1/2 -translate-x-1/2"
      : "-bottom-10 left-1/2 -translate-x-1/2";
  return (
    <span
      className={`relative inline-flex ${className}`}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
    >
      {children}
      {open && (
        <span
          role="tooltip"
          className={`absolute z-50 ${pos} whitespace-pre rounded-md bg-gray-900 px-2 py-1 text-xs text-white shadow-lg`}
        >
          {text}
        </span>
      )}
    </span>
  );
}

function HelpDrawer({ open, onClose }: { open: boolean; onClose: () => void }) {
  return (
    <div className={`fixed inset-0 z-50 ${open ? "pointer-events-auto" : "pointer-events-none"}`} aria-hidden={!open}>
      {/* backdrop */}
      <div
        className={`absolute inset-0 bg-black/30 transition-opacity ${open ? "opacity-100" : "opacity-0"}`}
        onClick={onClose}
      />
      {/* panel */}
      <aside
        className={`absolute right-0 top-0 h-full w-[520px] max-w-[95vw] transform bg-white shadow-2xl transition-transform ${
          open ? "translate-x-0" : "translate-x-full"
        }`}
        aria-label="Quick User Guide"
      >
        <div className="flex items-center justify-between border-b px-5 py-3">
          <h2 className="text-lg font-semibold">Quick User Guide</h2>
          <button onClick={onClose} className="rounded p-2 hover:bg-gray-100" aria-label="Close">
            ✕
          </button>
        </div>
        <div className="prose max-w-none px-5 py-4">
          <h3>TL;DR (Fast Path)</h3>
          <ol>
            <li>Add customer → <em>Add</em></li>
            <li>Upload <strong>Repository JSON</strong> per server → <em>Upload &amp; Ingest</em></li>
            <li>(If your QEM metrics file has no <code>Host</code>) Upload <strong>QEM Servers TSV</strong></li>
            <li>Upload <strong>QEM Metrics TSV</strong></li>
            <li>(Optional) Upload <strong>Replicate License Log</strong></li>
            <li>Download <strong>Customer Technical Overview (.docx)</strong></li>
          </ol>

          <h3>Files &amp; Formats</h3>
          <ul>
            <li><strong>Repo JSON:</strong> parses servers, endpoints, tasks, settings.</li>
            <li><strong>QEM Servers TSV:</strong> columns <code>Name</code> and <code>Host</code>.</li>
            <li><strong>QEM Metrics TSV:</strong> per-row metrics, uses <code>Host</code> or the mapping above.</li>
            <li><strong>License Log:</strong> parses the <code>]I: Licensed to …</code> line for licensed sources/targets.</li>
          </ul>

          <h3>Troubleshooting</h3>
          <ul>
            <li><em>Failed to fetch / CORS:</em> open the site via the VM’s IP/host; UI proxies API at <code>/api</code>.</li>
            <li><em>Zeros in QEM tiles:</em> import Servers TSV first if Metrics TSV lacks <code>Host</code>.</li>
            <li><em>Export issues:</em> ensure Repo JSON ingested and DB schema applied.</li>
          </ul>

          <div className="mt-6 rounded-lg bg-indigo-50 p-4">
            <div className="font-semibold mb-1">Getting Started checklist</div>
            <ul className="list-disc pl-5">
              <li>Add a customer</li>
              <li>Upload at least one Repository JSON</li>
              <li>Import QEM Servers TSV (if needed)</li>
              <li>Import QEM Metrics TSV</li>
              <li>(Optional) Upload license log</li>
              <li>Export the report</li>
            </ul>
          </div>

          <p className="mt-4 text-sm text-slate-600">
            API: <a className="text-indigo-600 underline" href="/api/docs" target="_blank" rel="noreferrer">/api/docs</a>
          </p>
        </div>
      </aside>
    </div>
  );
}

/** ---- main component ---- */
export default function App() {
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [servers, setServers] = useState<Server[]>([]);
  const [customerId, setCustomerId] = useState<number | null>(null);
  const [helpOpen, setHelpOpen] = useState(false);

  const customerName = useMemo(
    () => customers.find((c) => c.customer_id === customerId)?.customer_name ?? "",
    [customers, customerId]
  );

  // File refs
  const repoFileRef = useRef<HTMLInputElement | null>(null);
  const qemServersFileRef = useRef<HTMLInputElement | null>(null);
  const qemFileRef = useRef<HTMLInputElement | null>(null);
  const licenseFileRef = useRef<HTMLInputElement | null>(null);

  // UI state
  const [newCustomerName, setNewCustomerName] = useState("");
  const [ingestMsg, setIngestMsg] = useState<string>("");
  const [serversUpsertMsg, setServersUpsertMsg] = useState<string>("");
  const [qemSummary, setQemSummary] = useState<any | null>(null);
  const [licenseSummary, setLicenseSummary] = useState<{
    all_sources?: boolean;
    all_targets?: boolean;
    sources?: string[];
    targets?: string[];
  } | null>(null);
  const [deleteAlsoServers, setDeleteAlsoServers] = useState<boolean>(false);
  const [busy, setBusy] = useState(false);

  // API health (derived from last customers fetch)
  const [apiOk, setApiOk] = useState<boolean | null>(null);

  /** helpers */
  const clearFileInputs = () => {
    if (repoFileRef.current) repoFileRef.current.value = "";
    if (qemServersFileRef.current) qemServersFileRef.current.value = "";
    if (qemFileRef.current) qemFileRef.current.value = "";
    if (licenseFileRef.current) licenseFileRef.current.value = "";
  };

  const reloadCustomers = async () => {
    try {
      const rows = await fetchJson<Customer[]>(`${API_BASE}/customers`);
      setCustomers(rows.sort((a, b) => a.customer_name.localeCompare(b.customer_name)));
      setApiOk(true);
      return rows;
    } catch (e: any) {
      setApiOk(false);
      toast(`Load customers failed: ${e.message}`, "err");
      throw e;
    }
  };

  /** Home: reset state & re-fetch */
  const resetApp = async () => {
    setBusy(false);
    setCustomerId(null);
    setNewCustomerName("");
    setIngestMsg("");
    setServersUpsertMsg("");
    setQemSummary(null);
    setLicenseSummary(null);
    setServers([]);
    clearFileInputs();
    await reloadCustomers();
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  /** effects */
  useEffect(() => {
    (async () => {
      const rows = await reloadCustomers().catch(() => []);
      if (rows.length === 1) setCustomerId(rows[0].customer_id);
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    (async () => {
      if (!customerId) {
        setServers([]);
        return;
      }
      try {
        const rows = await fetchJson<Server[]>(`${API_BASE}/customers/${customerId}/servers`);
        setServers(rows);
      } catch (e: any) {
        toast(`Load servers failed: ${e.message}`, "err");
      }
    })();
  }, [customerId]);

  /** actions */
  async function handleAddCustomer() {
    const name = newCustomerName.trim();
    if (!name) return toast("Enter a customer name.", "err");
    try {
      const created = await fetchJson<Customer>(`${API_BASE}/customers`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ customer_name: name }),
      });
      const merged = [...customers, created].sort((a, b) =>
        a.customer_name.localeCompare(b.customer_name, undefined, { sensitivity: "base" })
      );
      setCustomers(merged);
      setCustomerId(created.customer_id);
      setNewCustomerName("");
      toast(`Customer "${created.customer_name}" added.`, "ok");
    } catch (e: any) {
      toast(`Add customer failed: ${e.message}`, "err");
    }
  }

  async function handleUploadRepoJson() {
    if (!customerName) return toast("Pick a customer first.", "err");
    const file = repoFileRef.current?.files?.[0];
    if (!file) return toast("Choose a repository JSON file.", "err");
    try {
      setIngestMsg("Uploading…");
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);
      setBusy(true);
      const res = await fetch(`${API_BASE}/ingest-file`, { method: "POST", body: fd });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      setIngestMsg("Ingest complete.");
      if (customerId) {
        const rows = await fetchJson<Server[]>(`${API_BASE}/customers/${customerId}/servers`);
        setServers(rows);
      }
      toast("Repository JSON ingested.", "ok");
    } catch (e: any) {
      setIngestMsg("");
      toast(`Repo ingest failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
      if (repoFileRef.current) repoFileRef.current.value = "";
    }
  }

  async function handleUploadQemServersTsv() {
    if (!customerName) return toast("Pick a customer first.", "err");
    const file = qemServersFileRef.current?.files?.[0];
    if (!file) return toast("Choose a Servers TSV file.", "err");
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);
      setBusy(true);
      // Use the generic backend route with kind=aem-servers
      const res = await fetch(`${API_BASE}/ingest-qem-file?kind=aem-servers`, { method: "POST", body: fd });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      const j = await res.json(); // { upserts, ... }
      setServersUpsertMsg(`Mappings upserted: ${j?.upserts ?? 0} row(s)`);
      if (customerId) {
        const rows = await fetchJson<Server[]>(`${API_BASE}/customers/${customerId}/servers`);
        setServers(rows);
      }
      toast("Servers TSV ingested.", "ok");
    } catch (e: any) {
      toast(`Servers TSV ingest failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
      if (qemServersFileRef.current) qemServersFileRef.current.value = "";
    }
  }

  async function handleUploadQemTsv() {
    if (!customerName) return toast("Pick a customer first.", "err");
    const file = qemFileRef.current?.files?.[0];
    if (!file) return toast("Choose a QEM Metrics TSV file.", "err");
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);
      setBusy(true);
      // Use generic route; backend can infer, but 'metrics' is explicit
      const res = await fetch(`${API_BASE}/ingest-qem-file?kind=metrics`, { method: "POST", body: fd });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      const j = await res.json();

      const totals = {
        rows: j?.total_rows_processed ?? 0,
        inserted: j?.total_metrics_inserted ?? 0,
        matched: j?.total_tasks_matched ?? 0,
      };

      const runs: any[] = Array.isArray(j?.runs) ? j.runs : Array.isArray(j?.details) ? j.details : [];
      const pick = (obj: any, keys: string[]) => {
        for (const k of keys) {
          const v = obj?.[k];
          if (v !== undefined && v !== null && `${v}`.trim() !== "") return v;
        }
        return undefined;
      };
      const details = runs.map((r) => ({
        server: pick(r, ["server", "Server", "server_name", "name", "qem_server"]) ?? "—",
        host_key: pick(r, ["host_key", "HostKey", "host", "key", "server_host"]) ?? "—",
        match_mode: pick(r, ["match_mode", "mode"]) ?? "—",
        rows: Number(pick(r, ["rows", "row_count", "processed", "total_rows"])) || 0,
        inserted: Number(pick(r, ["inserted", "inserted_rows", "inserted_count", "metrics_inserted"])) || 0,
      }));

      const modes = Array.from(new Set(details.map((d) => d.match_mode).filter((m) => m && m !== "—")));
      const match_mode = modes.length === 1 ? modes[0] : modes.length ? "mixed" : "—";

      setQemSummary({ ...totals, match_mode, details });

      if (customerId) {
        const rows = await fetchJson<Server[]>(`${API_BASE}/customers/${customerId}/servers`);
        setServers(rows);
      }
      toast("QEM TSV ingested.", "ok");
    } catch (e: any) {
      toast(`QEM ingest failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
      if (qemFileRef.current) qemFileRef.current.value = "";
    }
  }

  async function handleUploadLicenseLog() {
    if (!customerName) return toast("Pick a customer first.", "err");
    const file = licenseFileRef.current?.files?.[0];
    if (!file) return toast("Choose a Replicate task log file.", "err");
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);
      setBusy(true);
      const res = await fetch(`${API_BASE}/ingest-license-log`, { method: "POST", body: fd });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      const j = await res.json(); // {licensed_all_sources, licensed_all_targets, licensed_sources, licensed_targets}
      setLicenseSummary({
        all_sources: !!j?.licensed_all_sources,
        all_targets: !!j?.licensed_all_targets,
        sources: j?.licensed_sources ?? [],
        targets: j?.licensed_targets ?? [],
      });
      toast("License log ingested.", "ok");
    } catch (e: any) {
      toast(`License ingest failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
      if (licenseFileRef.current) licenseFileRef.current.value = "";
    }
  }

  async function downloadCustomerDocx() {
    if (!customerName) return toast("Select a customer first.", "err");
    try {
      const url = `${API_BASE}/export/customer?customer=${encodeURIComponent(customerName)}`;
      const res = await fetch(url);
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `Customer_Technical_Overview_${customerName}.docx`.replace(/\s+/g, "_");
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e: any) {
      toast(`Export failed: ${e.message}`, "err");
    }
  }

  async function handleDeleteAll() {
    if (!customerId) return toast("Pick a customer first.", "err");
    if (!confirm("Delete all ingested data for this customer?")) return;
    try {
      setBusy(true);
      const qs = deleteAlsoServers ? "?drop_servers=1" : "";
      const res = await fetch(`${API_BASE}/customers/${customerId}/data${qs}`, { method: "DELETE" });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      setServers([]);
      setQemSummary(null);
      setServersUpsertMsg("");
      setIngestMsg("");
      setLicenseSummary(null);
      toast("Deleted.", "ok");
    } catch (e: any) {
      toast(`Delete failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
    }
  }

  /** ---- render ---- */
  const showCustomerHint = customers.length > 0 && !customerId;
  const noCustomers = customers.length === 0 && apiOk === true;

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Top bar with Home / Refresh / API status / Docs / Help */}
      <div className="bg-white border-b sticky top-0 z-40">
        <div className="max-w-7xl mx-auto p-3 flex items-center gap-3 justify-between">
          <div className="flex items-center gap-3">
            <button
              onClick={resetApp}
              className="inline-flex items-center gap-2 px-3 h-10 rounded-xl border hover:bg-slate-50"
              title="Reset view and reload customers"
            >
              <span>🏠</span>
              <span className="font-medium">Home</span>
            </button>
            <button
              onClick={reloadCustomers}
              className="inline-flex items-center gap-2 px-3 h-10 rounded-xl border hover:bg-slate-50"
              title="Refresh customers"
            >
              <span>↻</span>
              <span className="font-medium">Refresh</span>
            </button>
            <span
              className={`inline-flex items-center gap-2 px-3 h-10 rounded-xl text-sm ${
                apiOk === true
                  ? "bg-emerald-50 text-emerald-700 border border-emerald-200"
                  : apiOk === false
                  ? "bg-amber-50 text-amber-700 border border-amber-200"
                  : "bg-slate-50 text-slate-600 border"
              }`}
              title="API connectivity based on last fetch"
            >
              <span
                className={`inline-block w-2.5 h-2.5 rounded-full ${
                  apiOk === true ? "bg-emerald-500" : apiOk === false ? "bg-amber-500" : "bg-slate-400"
                }`}
              />
              <span className="font-medium">{apiOk === true ? "API OK" : apiOk === false ? "API issue" : "API…"}</span>
            </span>
          </div>
          <div className="flex items-center gap-2">
            <a
              href="/api/docs"
              target="_blank"
              className="inline-flex items-center gap-2 px-3 h-10 rounded-xl border hover:bg-slate-50"
              rel="noreferrer"
              title="Open API Swagger in a new tab"
            >
              <span>📄</span>
              <span className="font-medium">Docs</span>
            </a>
            <button
              onClick={() => setHelpOpen(true)}
              className="inline-flex items-center gap-2 px-3 h-10 rounded-xl bg-indigo-600 text-white hover:bg-indigo-700"
              title="Open the Quick User Guide"
            >
              <span>❔</span>
              <span className="font-medium">Help</span>
            </button>
          </div>
        </div>
      </div>

      <header className="border-b bg-white">
        <div className="max-w-7xl mx-auto p-4">
          <h1 className="text-2xl font-semibold">Qlik Replicate Metadata Console</h1>
          <p className="text-slate-500 text-sm">
            Ingest repository JSONs &amp; QEM TSVs, then export sleek Word reports.
          </p>
          {/* Smart empty states */}
          {apiOk === false && (
            <div className="mt-3 rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
              API looks unreachable. Make sure you’re opening the app via the VM’s IP/host and that <code>/api</code> is proxied.
            </div>
          )}
          {noCustomers && (
            <div className="mt-3 rounded-xl border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
              No customers yet — add one below to get started.
            </div>
          )}
          {showCustomerHint && (
            <div className="mt-3 rounded-xl border border-indigo-200 bg-indigo-50 p-3 text-sm text-indigo-900">
              Pick a customer from the dropdown after you add one to unlock uploads and export.
            </div>
          )}
        </div>
      </header>

      <main className="max-w-7xl mx-auto p-6 space-y-5 md:space-y-6">
        {/* Add Customer */}
        <section className="bg-white rounded-2xl shadow-sm p-4">
          <h3 className="text-sm font-semibold mb-2">Add customer</h3>
          <div className="flex flex-col sm:flex-row gap-3">
            <input
              id="add-customer"
              type="text"
              placeholder="Enter customer name"
              value={newCustomerName}
              onChange={(e) => setNewCustomerName(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleAddCustomer()}
              className="flex-1 rounded-xl border-slate-300 focus:ring-2 focus:ring-indigo-500 px-3 h-11"
            />
            <button
              onClick={handleAddCustomer}
              className="px-4 h-11 rounded-xl bg-indigo-600 text-white hover:bg-indigo-700"
            >
              Add
            </button>
          </div>
        </section>

        {/* Selections / Toolbar */}
        <section className="bg-white rounded-2xl shadow-sm p-4">
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto] items-end gap-4">
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">Customer</label>
              <select
                className="w-full rounded-xl border-slate-300 focus:ring-2 focus:ring-indigo-500 h-11"
                value={customerId ?? ""}
                onChange={(e) => setCustomerId(e.target.value ? Number(e.target.value) : null)}
              >
                <option value="">Select…</option>
                {customers.map((c) => (
                  <option key={c.customer_id} value={c.customer_id}>
                    {c.customer_name}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex gap-3">
              <Tip text={"Exports a modern Customer Technical Overview (.docx)\nusing the latest ingested data."}>
                <button
                  id="export-docx"
                  onClick={downloadCustomerDocx}
                  disabled={!customerName || busy}
                  className="px-4 h-11 rounded-xl bg-violet-700 text-white hover:bg-violet-800 disabled:opacity-50"
                >
                  Download Customer Technical Overview (.docx)
                </button>
              </Tip>
            </div>
          </div>

          {/* Customer Snapshot */}
          {!!customerId && (
            <div className="mt-4 rounded-xl border bg-slate-50 p-3">
              <div className="flex items-center justify-between mb-2">
                <div className="text-sm font-semibold">
                  Snapshot: <span className="text-indigo-700">{customerName}</span>
                </div>
                <div className="text-xs text-slate-500">
                  {servers.length} server{servers.length === 1 ? "" : "s"}
                </div>
              </div>
              {servers.length === 0 ? (
                <div className="text-sm text-slate-500">No servers ingested yet.</div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  {servers.map((s) => (
                    <div key={s.server_id} className="rounded-xl bg-white border p-3">
                      <div className="text-sm font-medium">{s.server_name}</div>
                      <div className="text-xs text-slate-500 mt-1">
                        {s.environment ? `Environment: ${s.environment}` : "Environment: —"}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </section>

        {/* Upload Repository JSON */}
        <section className="bg-white rounded-2xl shadow-sm p-5 md:p-6">
          <h3 className="text-base font-semibold mb-1">Upload Repository JSON</h3>
          <p className="text-xs text-slate-500 mb-3">
            Server is auto-detected from the file description (e.g. “Host name: USREM-HXT2, Time: …”).
          </p>
          <div className="flex flex-col items-start gap-3">
            <input id="upload-repo-json" ref={repoFileRef} type="file" accept=".json" className="text-sm" />
            <Tip text={"Qlik Replicate repository export (.json).\nParses servers, endpoints, tasks & settings."}>
              <button
                onClick={handleUploadRepoJson}
                disabled={!customerName || busy}
                className="px-4 h-11 rounded-xl bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                Upload &amp; Ingest
              </button>
            </Tip>
          </div>
          {ingestMsg && <p className="mt-3 text-sm text-slate-600">{ingestMsg}</p>}
        </section>

        {/* Upload QEM Servers TSV */}
        <section className="bg-white rounded-2xl shadow-sm p-5 md:p-6">
          <h3 className="text-base font-semibold mb-1">Upload QEM Servers (TSV)</h3>
          <p className="text-xs text-slate-500 mb-3">
            Upload the <span className="font-mono">AemServers_*.tsv</span>. We map its{" "}
            <span className="font-mono">Name</span> to the QEM “Server” column, and its{" "}
            <span className="font-mono">Host</span> to the Repo server name. Required before uploading a QEM Metrics TSV
            without a <span className="font-mono">Host</span> column.
          </p>
          <div className="flex flex-col items-start gap-3">
            <input id="upload-qem-servers" ref={qemServersFileRef} type="file" accept=".tsv,.txt" className="text-sm" />
            <Tip text={"AemServers TSV mapping Name → Server and Host → Repo server.\nNeeded if Metrics TSV lacks Host."}>
              <button
                onClick={handleUploadQemServersTsv}
                disabled={!customerName || busy}
                className="px-4 h-11 rounded-xl bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50"
              >
                Upload Servers TSV
              </button>
            </Tip>
          </div>
          {serversUpsertMsg && <p className="mt-3 text-sm text-slate-600">{serversUpsertMsg}</p>}
        </section>

        {/* Upload QEM Metrics TSV */}
        <section className="bg-white rounded-2xl shadow-sm p-5 md:p-6">
          <h3 className="text-base font-semibold mb-1">Upload QEM Metrics (TSV)</h3>
          <p className="text-xs text-slate-500 mb-3">
            If the TSV includes a <span className="font-mono">Host</span> column, we use it directly (legacy).
            Otherwise we resolve per row using the Servers mapping above (<em>Name → Host</em>).
          </p>
          <div className="flex flex-col items-start gap-3">
            <input id="upload-qem-metrics" ref={qemFileRef} type="file" accept=".tsv,.txt" className="text-sm" />
            <Tip text={"QEM per-task metrics TSV.\nUses Host if present, else the Name→Host mapping."}>
              <button
                onClick={handleUploadQemTsv}
                disabled={!customerName || busy}
                className="px-4 h-11 rounded-xl bg-sky-600 text-white hover:bg-sky-700 disabled:opacity-50"
              >
                Upload QEM
              </button>
            </Tip>
          </div>

          {qemSummary && (
            <div className="mt-4">
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                <StatCard label="Rows processed" value={qemSummary?.rows ?? "—"} />
                <StatCard label="Inserted" value={qemSummary?.inserted ?? "—"} />
                <StatCard label="Matched tasks" value={qemSummary?.matched ?? "—"} />
                <StatCard label="Match mode" value={qemSummary?.match_mode ?? "—"} />
              </div>

              {Array.isArray(qemSummary?.details) && qemSummary.details.length > 0 && (
                <div className="mt-3 overflow-x-auto">
                  <table className="min-w-[520px] text-sm">
                    <thead>
                      <tr className="text-left text-slate-500">
                        <th className="py-1 pr-4">Server</th>
                        <th className="py-1 pr-4">Host Key</th>
                        <th className="py-1 pr-4">Match mode</th>
                        <th className="py-1 pr-4">Rows</th>
                        <th className="py-1 pr-4">Inserted</th>
                      </tr>
                    </thead>
                    <tbody>
                      {qemSummary.details.map((r: any, i: number) => (
                        <tr key={i} className="border-t">
                          <td className="py-1 pr-4">{r.server ?? "—"}</td>
                          <td className="py-1 pr-4">{r.host_key ?? "—"}</td>
                          <td className="py-1 pr-4">{r.match_mode ?? "—"}</td>
                          <td className="py-1 pr-4">{r.rows ?? "—"}</td>
                          <td className="py-1 pr-4">{r.inserted ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </section>

        {/* Upload Replicate License Log */}
        <section className="bg-white rounded-2xl shadow-sm p-5 md:p-6">
          <h3 className="text-base font-semibold mb-1">Upload Replicate License Log</h3>
          <p className="text-xs text-slate-500 mb-3">
            Upload a task log for this customer. We parse the second{" "}
            <span className="font-mono">]I: Licensed to …</span> line to detect licensed sources/targets.
          </p>
          <div className="flex flex-col items-start gap-3">
            <input id="upload-license-log" ref={licenseFileRef} type="file" accept=".log,.txt" className="text-sm" />
            <Tip
              text={`Any Replicate task log containing "]I: Licensed to ...".\nWe extract licensed sources/targets.`}
            >
              <button
                onClick={handleUploadLicenseLog}
                disabled={!customerName || busy}
                className="px-4 h-11 rounded-xl bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
              >
                Upload License Log
              </button>
            </Tip>
          </div>

          {licenseSummary && (
            <div className="mt-3 text-sm">
              <div className="font-medium mb-1">Detected license</div>
              <div>
                Sources:&nbsp;
                {licenseSummary.all_sources ? (
                  <span>All</span>
                ) : licenseSummary.sources?.length ? (
                  licenseSummary.sources.join(", ")
                ) : (
                  "—"
                )}
              </div>
              <div>
                Targets:&nbsp;
                {licenseSummary.all_targets ? (
                  <span>All</span>
                ) : licenseSummary.targets?.length ? (
                  licenseSummary.targets.join(", ")
                ) : (
                  "—"
                )}
              </div>
            </div>
          )}
        </section>

        {/* Cleanup */}
        <section className="bg-white rounded-2xl shadow-sm p-5">
          <h3 className="text-base font-semibold mb-3">Cleanup</h3>
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <label className="inline-flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={deleteAlsoServers}
                onChange={(e) => setDeleteAlsoServers(e.target.checked)}
              />
              <span>Also drop this customer’s servers</span>
            </label>
            <button
              onClick={handleDeleteAll}
              disabled={!customerId || busy}
              className="px-4 h-11 rounded-xl bg-rose-600 text-white hover:bg-rose-700 disabled:opacity-50"
            >
              Delete ingested data for customer
            </button>
          </div>
        </section>
      </main>

      {/* Inline Quick User Guide */}
      <HelpDrawer open={helpOpen} onClose={() => setHelpOpen(false)} />
    </div>
  );
}
