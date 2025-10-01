import React, { useEffect, useMemo, useRef, useState } from "react";

/** Types */
type Customer = { customer_id: number; customer_name: string };
type Server = { server_id: number; server_name: string; environment?: string };

/** Keep your exact API base logic to avoid regressions */
const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8002";

/** ---- helpers ---- */
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
  el.style.borderRadius = "12px";
  el.style.fontSize = "14px";
  el.style.color = kind === "ok" ? "#064e3b" : "#7f1d1d";
  el.style.background = kind === "ok" ? "#d1fae5" : "#fee2e2";
  el.style.boxShadow = "0 8px 24px rgba(0,0,0,.18)";
  el.style.zIndex = "9999";
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 2800);
}

/** ---- small UI bits ---- */
function QlikMark() {
  return (
    <div className="relative grid h-9 w-9 place-items-center">
      <div className="h-9 w-9 rounded-full bg-emerald-600"/>
      <div className="absolute h-4 w-4 rounded-full bg-white"/>
    </div>
  );
}

function StatTile({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex items-center gap-2 rounded-full border border-emerald-600/30 bg-emerald-500/10 px-3 py-1">
      <span className="inline-block h-2 w-2 rounded-full bg-emerald-500"/>
      <span className="text-sm font-semibold text-emerald-700">{value}</span>
      <span className="text-[11px] uppercase tracking-wide text-emerald-800/70">{label}</span>
    </div>
  );
}

function Step({
  index, label, active, done,
}: { index: number; label: string; active?: boolean; done?: boolean }) {
  return (
    <div
      className={`group flex items-center gap-3 rounded-2xl px-3 py-2 transition border ${
        active
          ? "bg-emerald-50 border-emerald-300 text-emerald-800"
          : "border-slate-200 text-slate-600"
      }`}
    >
      <div
        className={`grid h-6 w-6 place-items-center rounded-full text-[11px] font-bold ${
          done ? "bg-emerald-600 text-white" : active ? "bg-emerald-500 text-white" : "bg-slate-200 text-slate-700"
        }`}
      >
        {done ? "✓" : index}
      </div>
      <span className="text-sm font-medium">{label}</span>
    </div>
  );
}

/** Generic, modern upload card wired to your refs and handlers */
function UploadCard({
  title,
  icon,
  description,
  fileRef,
  accept,
  cta,
  onUpload,
  disabled,
}: {
  title: string;
  icon: React.ReactNode;
  description: string;
  fileRef: React.MutableRefObject<HTMLInputElement | null>;
  accept: string;
  cta: string;
  onUpload: () => void;
  disabled?: boolean;
}) {
  const [fileName, setFileName] = useState<string>("");

  return (
    <div className="border-slate-200/70 bg-white/80 backdrop-blur-xl shadow-sm hover:shadow-md transition rounded-2xl border p-5">
      <div className="flex items-center gap-3 mb-2">
        <span className="grid h-9 w-9 place-items-center rounded-xl bg-emerald-500/15 text-emerald-600">{icon}</span>
        <div className="text-base md:text-lg font-semibold text-slate-900">{title}</div>
      </div>
      <p className="text-sm text-slate-600 mb-3">{description}</p>
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={() => fileRef.current?.click()}
          className="rounded-xl border border-emerald-400/40 px-3 h-10 text-sm bg-white hover:bg-slate-50"
        >
          Choose File
        </button>
        <input
          ref={fileRef}
          type="file"
          className="hidden"
          accept={accept}
          onChange={(e) => setFileName(e.target.files?.[0]?.name || "")}
        />
        <button
          type="button"
          onClick={onUpload}
          disabled={disabled}
          className="rounded-xl bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 text-white px-3 h-10 text-sm"
        >
          {cta}
        </button>
        {fileName && (
          <span className="rounded-full border border-emerald-400/40 text-emerald-700 text-xs px-2 py-1">
            {fileName}
          </span>
        )}
      </div>
    </div>
  );
}

/** ---- main app ---- */
export default function App() {
  // data
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [servers, setServers] = useState<Server[]>([]);
  const [customerId, setCustomerId] = useState<number | null>(null);

  // file refs
  const repoFileRef = useRef<HTMLInputElement | null>(null);
  const qemServersFileRef = useRef<HTMLInputElement | null>(null);
  const qemFileRef = useRef<HTMLInputElement | null>(null);
  const licenseFileRef = useRef<HTMLInputElement | null>(null);

  // ui state
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
  const [helpOpen, setHelpOpen] = useState(false);

  const customerName = useMemo(
    () => customers.find((c) => c.customer_id === customerId)?.customer_name ?? "",
    [customers, customerId]
  );

  /** data loaders */
  async function loadCustomers() {
    const rows = await fetchJson<Customer[]>(`${API_BASE}/customers`);
    setCustomers(rows.sort((a, b) => a.customer_name.localeCompare(b.customer_name)));
    if (rows.length === 1) setCustomerId(rows[0].customer_id);
  }
  async function loadServers(cid: number) {
    const rows = await fetchJson<Server[]>(`${API_BASE}/customers/${cid}/servers`);
    setServers(rows);
  }

  useEffect(() => {
    (async () => {
      try { await loadCustomers(); } catch (e: any) { toast(`Load customers failed: ${e.message}`, "err"); }
    })();
  }, []);
  useEffect(() => {
    (async () => {
      if (!customerId) { setServers([]); return; }
      try { await loadServers(customerId); } catch (e: any) { toast(`Load servers failed: ${e.message}`, "err"); }
    })();
  }, [customerId]);

  /** home/reset */
  async function handleHomeReset() {
    try {
      if (repoFileRef.current) repoFileRef.current.value = "";
      if (qemServersFileRef.current) qemServersFileRef.current.value = "";
      if (qemFileRef.current) qemFileRef.current.value = "";
      if (licenseFileRef.current) licenseFileRef.current.value = "";
      setCustomerId(null);
      setNewCustomerName("");
      setIngestMsg("");
      setServersUpsertMsg("");
      setQemSummary(null);
      setLicenseSummary(null);
      setDeleteAlsoServers(false);
      await loadCustomers();
      toast("Reset complete.", "ok");
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (e: any) {
      toast(`Reset failed: ${e.message}`, "err");
    }
  }

  /** actions — identical to your existing handlers, just moved inside the new UI */
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
      setIngestMsg("Uploading...");
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
      if (customerId) await loadServers(customerId);
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
      const res = await fetch(`${API_BASE}/ingest-qem-servers-file`, { method: "POST", body: fd });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      const j = await res.json();
      setServersUpsertMsg(`Mappings upserted: ${j?.upserts ?? 0} row(s)`);
      if (customerId) await loadServers(customerId);
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
      const res = await fetch(`${API_BASE}/ingest-qem-file`, { method: "POST", body: fd });
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
        server: pick(r, ["server", "Server", "server_name", "name", "qem_server"]) ?? "-",
        host_key: pick(r, ["host_key", "HostKey", "host", "key", "server_host"]) ?? "-",
        match_mode: pick(r, ["match_mode", "mode"]) ?? "-",
        rows: Number(pick(r, ["rows", "row_count", "processed", "total_rows"])) || 0,
        inserted: Number(pick(r, ["inserted", "inserted_rows", "inserted_count", "metrics_inserted"])) || 0,
      }));

      const modes = Array.from(new Set(details.map((d) => d.match_mode).filter((m) => m && m !== "-")));
      const match_mode = modes.length === 1 ? modes[0] : modes.length ? "mixed" : "-";

      setQemSummary({ ...totals, match_mode, details });

      if (customerId) await loadServers(customerId);
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
      const j = await res.json();
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

  /** ---- UI ---- */
  return (
    <div className="min-h-screen w-full bg-[radial-gradient(900px_400px_at_80%_-5%,rgba(36,211,102,0.18),transparent),radial-gradient(700px_400px_at_0%_-10%,rgba(16,185,129,0.15),transparent)] bg-gradient-to-br from-white via-slate-50 to-emerald-50">
      {/* Header */}
      <header className="sticky top-0 z-40 backdrop-blur-xl bg-white/70 border-b border-emerald-200/30">
        <div className="mx-auto max-w-7xl px-4 md:px-6 py-3 flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <QlikMark />
            <div>
              <div className="text-sm font-semibold tracking-tight text-slate-900">RepMeta Console — Qlik Theme</div>
              <div className="text-[11px] text-slate-600">Ingest JSON/TSV & export polished Word reports</div>
            </div>
          </div>
          <div className="hidden md:flex items-center gap-2">
            <button className="rounded-full border px-3 h-10" onClick={handleHomeReset} title="Home / Reset">
              Home / Reset
            </button>
            <button className="rounded-full bg-emerald-600 hover:bg-emerald-700 text-white px-3 h-10" onClick={downloadCustomerDocx}>
              Download Technical Overview (docx)
            </button>
          </div>
        </div>
      </header>

      {/* Content */}
      <main className="mx-auto max-w-7xl px-4 md:px-6 py-6 grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Left rail */}
        <div className="lg:col-span-1 space-y-6">
          <div className="border-emerald-400/30 bg-white/80 rounded-2xl border p-4">
            <div className="text-base font-semibold mb-2">Setup</div>
            <div className="flex flex-col gap-2">
              <Step index={1} label="Add Customer" done={!!customers.length}/>
              <Step index={2} label="Select Snapshot" done={!!customerId}/>
              <Step index={3} label="Upload Repository JSON" active/>
              <Step index={4} label="Upload Servers TSV"/>
              <Step index={5} label="Upload QEM TSV"/>
              <Step index={6} label="Upload License Log"/>
              <Step index={7} label="Generate Report"/>
            </div>
          </div>

          <div className="border-emerald-400/30 bg-white/80 rounded-2xl border p-4">
            <div className="text-base font-semibold mb-2">Context</div>
            <div className="flex flex-wrap gap-2 mb-3">
              <span className="rounded-full border border-emerald-400/40 px-3 py-1 text-sm">Customer: {customerName || "–"}</span>
              <span className="rounded-full border border-emerald-400/40 px-3 py-1 text-sm">Env: prod</span>
              <span className="rounded-full border border-emerald-400/40 px-3 py-1 text-sm">Servers: {servers.length}</span>
            </div>
            <div className="flex flex-wrap gap-2">
              <StatTile label="Tasks" value={qemSummary?.matched ?? "—"} />
              <StatTile label="QEM Events" value={qemSummary?.rows ?? "—"} />
            </div>
          </div>
        </div>

        {/* Workbench */}
        <div className="lg:col-span-3 space-y-8">
          {/* Add/Select customer */}
          <div className="rounded-2xl border bg-white shadow-sm p-4">
            <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto] items-end gap-4">
              <div className="flex flex-col sm:flex-row gap-3">
                <input
                  type="text"
                  placeholder="Enter customer name"
                  value={newCustomerName}
                  onChange={(e) => setNewCustomerName(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleAddCustomer()}
                  className="flex-1 rounded-xl border-slate-300 focus:ring-2 focus:ring-emerald-500 px-3 h-11"
                />
                <button
                  onClick={handleAddCustomer}
                  className="px-4 h-11 rounded-xl bg-emerald-600 text-white hover:bg-emerald-700"
                >
                  Add
                </button>
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-600 mb-1">Customer</label>
                <select
                  className="w-full rounded-xl border-slate-300 focus:ring-2 focus:ring-emerald-500 h-11"
                  value={customerId ?? ""}
                  onChange={(e) => setCustomerId(e.target.value ? Number(e.target.value) : null)}
                >
                  <option value="">Select...</option>
                  {customers.map((c) => (
                    <option key={c.customer_id} value={c.customer_id}>
                      {c.customer_name}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {!!customerId && (
              <div className="mt-4 rounded-xl border bg-slate-50 p-3">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm font-semibold">
                    Snapshot: <span className="text-emerald-700">{customerName}</span>
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
                          {s.environment ? `Environment: ${s.environment}` : "Environment: -"}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Uploads */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <UploadCard
              title="Upload Repository JSON"
              icon={<span className="text-emerald-700">{"{ }"}</span>}
              description='Auto-detects server (e.g., from "Host name: …").'
              fileRef={repoFileRef}
              accept=".json"
              cta="Upload & Ingest"
              onUpload={handleUploadRepoJson}
              disabled={!customerName || busy}
            />
            <UploadCard
              title="Upload QEM Servers (TSV)"
              icon={<span className="text-emerald-700">☰</span>}
              description='Map "ServerName" → Repo server names; required if QEM TSV lacks Host.'
              fileRef={qemServersFileRef}
              accept=".tsv,.csv,.txt"
              cta="Upload Servers TSV"
              onUpload={handleUploadQemServersTsv}
              disabled={!customerName || busy}
            />
            <UploadCard
              title="Upload QEM Metrics (TSV)"
              icon={<span className="text-emerald-700">📈</span>}
              description='If TSV includes Host, we use it; else we resolve via Servers mapping above.'
              fileRef={qemFileRef}
              accept=".tsv,.csv,.txt"
              cta="Upload QEM"
              onUpload={handleUploadQemTsv}
              disabled={!customerName || busy}
            />
            <UploadCard
              title="Upload Replicate License Log"
              icon={<span className="text-emerald-700">🔑</span>}
              description='Parses "Licensed to:" to detect licensed sources & targets.'
              fileRef={licenseFileRef}
              accept=".log,.txt"
              cta="Upload License Log"
              onUpload={handleUploadLicenseLog}
              disabled={!customerName || busy}
            />
          </div>

          {/* inline messages */}
          {ingestMsg && (
            <div className="rounded-xl border bg-white p-3 text-sm">{ingestMsg}</div>
          )}
          {serversUpsertMsg && (
            <div className="rounded-xl border bg-white p-3 text-sm">{serversUpsertMsg}</div>
          )}
          {qemSummary && (
            <div className="rounded-xl border bg-white p-3 text-sm">
              QEM Summary — rows: <b>{qemSummary.rows}</b>, inserted: <b>{qemSummary.inserted}</b>, matched:{" "}
              <b>{qemSummary.matched}</b>, mode: <b>{qemSummary.match_mode}</b>
            </div>
          )}
          {licenseSummary && (
            <div className="rounded-xl border bg-white p-3 text-sm">
              License — all sources: <b>{licenseSummary.all_sources ? "yes" : "no"}</b>, all targets:{" "}
              <b>{licenseSummary.all_targets ? "yes" : "no"}</b>
              {Array.isArray(licenseSummary.sources) && licenseSummary.sources.length > 0 && (
                <>
                  <br />Sources: {licenseSummary.sources.join(", ")}
                </>
              )}
              {Array.isArray(licenseSummary.targets) && licenseSummary.targets.length > 0 && (
                <>
                  <br />Targets: {licenseSummary.targets.join(", ")}
                </>
              )}
            </div>
          )}

          {/* Cleanup */}
          <div>
            <div className="text-base font-semibold mb-2">Cleanup</div>
            <div className="bg-white/80 rounded-2xl border p-4">
              <div className="py-1 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                <label className="text-sm text-slate-700 flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={deleteAlsoServers}
                    onChange={(e) => setDeleteAlsoServers(e.target.checked)}
                  />
                  Also drop this customer's servers
                </label>
                <button
                  onClick={handleDeleteAll}
                  className="rounded-full bg-red-600 hover:bg-red-700 text-white px-4 h-10"
                >
                  Delete ingested data for customer
                </button>
              </div>
            </div>
          </div>

          {/* Footer actions */}
          <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-3">
            <div className="text-xs text-slate-600">
              All functionality preserved — just a modern, Qlik-forward shell.
            </div>
            <div className="flex items-center gap-2">
              <button
                className="rounded-full border px-3 h-10"
                onClick={() => setHelpOpen(true)}
              >
                Help / Quick Guide
              </button>
              <button
                className="rounded-full bg-emerald-600 hover:bg-emerald-700 text-white px-3 h-10"
                onClick={downloadCustomerDocx}
              >
                Download Technical Overview (docx)
              </button>
            </div>
          </div>
        </div>
      </main>

      {/* Simple Help drawer */}
      {helpOpen && (
        <div className="fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/30" onClick={() => setHelpOpen(false)} />
          <aside className="absolute right-0 top-0 h-full w-[520px] max-w-[95vw] bg-white shadow-2xl">
            <div className="flex items-center justify-between border-b px-5 py-3">
              <h2 className="text-lg font-semibold">Quick User Guide</h2>
              <button onClick={() => setHelpOpen(false)} className="rounded p-2 hover:bg-gray-100" aria-label="Close">✕</button>
            </div>
            <div className="px-5 py-4 text-sm space-y-3">
              <ol className="list-decimal pl-5 space-y-1">
                <li>Add customer</li>
                <li>Upload <b>Repository JSON</b> (per server)</li>
                <li>(If no <code>Host</code> in QEM TSV) Upload <b>QEM Servers TSV</b></li>
                <li>Upload <b>QEM Metrics TSV</b></li>
                <li>(Optional) Upload <b>License Log</b></li>
                <li>Download <b>Customer Technical Overview (.docx)</b></li>
              </ol>
              <div className="rounded-lg bg-emerald-50 p-3">
                Tip: Use Repo JSON before QEM to populate servers.
              </div>
            </div>
          </aside>
        </div>
      )}
    </div>
  );
}
