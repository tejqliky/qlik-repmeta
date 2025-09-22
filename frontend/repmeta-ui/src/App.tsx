import { useEffect, useMemo, useRef, useState } from "react";

type Customer = { customer_id: number; customer_name: string };
type Server = { server_id: number; server_name: string; environment?: string };

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8002";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const j = await res.json();
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

export default function App() {
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [servers, setServers] = useState<Server[]>([]);
  const [customerId, setCustomerId] = useState<number | null>(null);
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

  useEffect(() => {
    (async () => {
      try {
        const rows = await fetchJson<Customer[]>(`${API_BASE}/customers`);
        setCustomers(rows.sort((a, b) => a.customer_name.localeCompare(b.customer_name)));
        if (rows.length === 1) setCustomerId(rows[0].customer_id);
      } catch (e: any) {
        toast(`Load customers failed: ${e.message}`, "err");
      }
    })();
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
      const res = await fetch(`${API_BASE}/ingest-qem-servers-file`, { method: "POST", body: fd });
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try { const j = await res.json(); d = j?.detail ? j.detail : JSON.stringify(j); } catch {}
        throw new Error(d);
      }
      const j = await res.json(); // { rows, upserts, ... }
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

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b bg-white">
        <div className="max-w-7xl mx-auto p-4">
          <h1 className="text-2xl font-semibold">Qlik Replicate Metadata Console</h1>
          <p className="text-slate-500 text-sm">
            Ingest repository JSONs &amp; QEM TSVs, then export sleek Word reports.
          </p>
        </div>
      </header>

      <main className="max-w-7xl mx-auto p-6 space-y-5 md:space-y-6">
        {/* Add Customer */}
        <section className="bg-white rounded-2xl shadow-sm p-4">
          <h3 className="text-sm font-semibold mb-2">Add customer</h3>
          <div className="flex flex-col sm:flex-row gap-3">
            <input
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
              <button
                onClick={downloadCustomerDocx}
                disabled={!customerName || busy}
                className="px-4 h-11 rounded-xl bg-violet-700 text-white hover:bg-violet-800 disabled:opacity-50"
              >
                Download Customer Technical Overview (.docx)
              </button>
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
            <input ref={repoFileRef} type="file" accept=".json" className="text-sm" />
            <button
              onClick={handleUploadRepoJson}
              disabled={!customerName || busy}
              className="px-4 h-11 rounded-xl bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50"
            >
              Upload &amp; Ingest
            </button>
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
            <input ref={qemServersFileRef} type="file" accept=".tsv,.txt" className="text-sm" />
            <button
              onClick={handleUploadQemServersTsv}
              disabled={!customerName || busy}
              className="px-4 h-11 rounded-xl bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50"
            >
              Upload Servers TSV
            </button>
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
            <input ref={qemFileRef} type="file" accept=".tsv,.txt" className="text-sm" />
            <button
              onClick={handleUploadQemTsv}
              disabled={!customerName || busy}
              className="px-4 h-11 rounded-xl bg-sky-600 text-white hover:bg-sky-700 disabled:opacity-50"
            >
              Upload QEM
            </button>
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

        {/* NEW: Upload Replicate License Log */}
        <section className="bg-white rounded-2xl shadow-sm p-5 md:p-6">
          <h3 className="text-base font-semibold mb-1">Upload Replicate License Log</h3>
          <p className="text-xs text-slate-500 mb-3">
            Upload a task log for this customer. We parse the second{" "}
            <span className="font-mono">]I: Licensed to …</span> line to detect licensed sources/targets.
          </p>
          <div className="flex flex-col items-start gap-3">
            <input ref={licenseFileRef} type="file" accept=".log,.txt" className="text-sm" />
            <button
              onClick={handleUploadLicenseLog}
              disabled={!customerName || busy}
              className="px-4 h-11 rounded-xl bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
            >
              Upload License Log
            </button>
          </div>

          {licenseSummary && (
            <div className="mt-3 text-sm">
              <div className="font-medium mb-1">Detected license</div>
              <div>
                Sources:&nbsp;
                {licenseSummary.all_sources
                  ? <span>All</span>
                  : (licenseSummary.sources?.length ? licenseSummary.sources.join(", ") : "—")}
              </div>
              <div>
                Targets:&nbsp;
                {licenseSummary.all_targets
                  ? <span>All</span>
                  : (licenseSummary.targets?.length ? licenseSummary.targets.join(", ") : "—")}
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
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: any }) {
  return (
    <div className="rounded-xl border bg-white p-3">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="text-xl font-semibold">{value ?? "—"}</div>
    </div>
  );
}
