import React, { useEffect, useMemo, useRef, useState } from "react";

/** Types */
type Customer = { customer_id: number; customer_name: string };
type Server = { server_id: number; server_name: string; environment?: string };
type Phase = "idle" | "uploading" | "processing" | "done" | "error";

/** Keep your exact API base logic to avoid regressions */
const API_BASE = "http://127.0.0.1:8002";

/** ---- helpers ---- */
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

function toast(msg: string, kind: "ok" | "err" | "warn" = "ok") {
  const el = document.createElement("div");
  el.textContent = msg;
  el.style.position = "fixed";
  el.style.left = "50%";
  el.style.transform = "translateX(-50%)";
  el.style.bottom = "24px";
  el.style.padding = "12px 20px";
  el.style.borderRadius = "4px";
  el.style.fontSize = "14px";
  el.style.fontWeight = "500";
  el.style.color = "#fff";
  el.style.background =
    kind === "ok" ? "#009845" : kind === "warn" ? "#F57C00" : "#D32F2F";
  el.style.boxShadow = "0 4px 12px rgba(0,0,0,.15)";
  el.style.zIndex = "9999";
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 2800);
}

/** Shared XHR uploader with progress */
function xhrPost(
  url: string,
  formData: FormData,
  setPct: (n: number | null) => void,
  setPhase: (p: Phase) => void
): Promise<any> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);

    xhr.upload.onprogress = (evt) => {
      setPhase("uploading");
      if (evt.lengthComputable) {
        const pct = Math.round((evt.loaded / evt.total) * 100);
        setPct(pct);
      }
    };

    xhr.upload.onload = () => {
      setPhase("processing");
      setPct(null);
    };

    xhr.onreadystatechange = () => {
      if (xhr.readyState !== 4) return;
      try {
        if (xhr.status >= 200 && xhr.status < 300) {
          let data: any = null;
          try { data = JSON.parse(xhr.responseText || "{}"); } catch {}
          setPhase("done");
          resolve(data);
        } else {
          let detail: string = xhr.responseText || `${xhr.status}`;
          try {
            const j = JSON.parse(xhr.responseText || "{}");
            detail = j?.detail || JSON.stringify(j);
          } catch {}
          setPhase("error");
          reject(new Error(detail));
        }
      } catch (err) {
        setPhase("error");
        reject(err);
      }
    };

    xhr.onerror = () => {
      setPhase("error");
      reject(new Error("network error"));
    };

    xhr.send(formData);
  });
}

/** Light overlay while exporting */
function LoaderOverlay({
  show,
  title,
  subtitle,
}: {
  show: boolean;
  title: string;
  subtitle?: string;
}) {
  if (!show) return null;
  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/40">
      <div className="rounded bg-white p-8 shadow-2xl max-w-md w-[90vw] text-center">
        <div className="mx-auto mb-4 h-12 w-12 rounded-full bg-gray-100 grid place-items-center animate-spin text-2xl">⏳</div>
        <div className="text-gray-900 font-semibold text-lg mb-1">{title}</div>
        {subtitle && <div className="text-gray-600 text-sm">{subtitle}</div>}
        <div className="mt-6 h-2 rounded-full bg-gray-200 overflow-hidden">
          <div className="h-full w-2/3 animate-progress-bar bg-green-600"/>
        </div>
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
  const metricsFileRef = useRef<HTMLInputElement | null>(null);

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
  const [metricsSummary, setMetricsSummary] = useState<
    | {
        rows?: number;
        inserted?: number;
        matched?: number;
        dup_count?: number;
      }
    | null
  >(null);
  const [metricsServerName, setMetricsServerName] = useState<string>("");
  const [deleteAlsoServers, setDeleteAlsoServers] = useState<boolean>(false);
  const [busy, setBusy] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [currentStep, setCurrentStep] = useState(1);
  const [exporting, setExporting] = useState(false);

  // per-card progress
  const [repoPhase, setRepoPhase] = useState<Phase>("idle");
  const [repoPct, setRepoPct] = useState<number | null>(null);

  const [qemServersPhase, setQemServersPhase] = useState<Phase>("idle");
  const [qemServersPct, setQemServersPct] = useState<number | null>(null);

  const [qemPhase, setQemPhase] = useState<Phase>("idle");
  const [qemPct, setQemPct] = useState<number | null>(null);

  const [licensePhase, setLicensePhase] = useState<Phase>("idle");
  const [licensePct, setLicensePct] = useState<number | null>(null);

  const [metricsPhase, setMetricsPhase] = useState<Phase>("idle");
  const [metricsUploadPct, setMetricsUploadPct] = useState<number | null>(null);

  const customerName = useMemo(
    () => customers.find((c) => c.customer_id === customerId)?.customer_name ?? "",
    [customers, customerId]
  );

  // Auto-progress steps
  useEffect(() => {
    if (customers.length > 0) setCurrentStep((s) => Math.max(s, 2));
    if (customerId) setCurrentStep((s) => Math.max(s, 3));
    if (servers.length > 0) setCurrentStep((s) => Math.max(s, 4));
    if (qemSummary) setCurrentStep((s) => Math.max(s, 5));
    if (metricsSummary) setCurrentStep((s) => Math.max(s, 6));
    if (licenseSummary) setCurrentStep((s) => Math.max(s, 7));
  }, [customers.length, customerId, servers.length, qemSummary, metricsSummary, licenseSummary]);

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

  // Auto-pick server for Metrics Log when only one exists
  useEffect(() => {
    if (servers.length === 1) {
      setMetricsServerName(servers[0].server_name);
    } else if (!servers.find((s) => s.server_name === metricsServerName)) {
      setMetricsServerName("");
    }
  }, [servers, metricsServerName]);

  /** home/reset */
  async function handleHomeReset() {
    try {
      if (repoFileRef.current) repoFileRef.current.value = "";
      if (qemServersFileRef.current) qemServersFileRef.current.value = "";
      if (qemFileRef.current) qemFileRef.current.value = "";
      if (licenseFileRef.current) licenseFileRef.current.value = "";
      if (metricsFileRef.current) metricsFileRef.current.value = "";
      setCustomerId(null);
      setNewCustomerName("");
      setIngestMsg("");
      setServersUpsertMsg("");
      setQemSummary(null);
      setLicenseSummary(null);
      setMetricsSummary(null);
      setMetricsServerName("");
      setDeleteAlsoServers(false);

      setRepoPhase("idle"); setRepoPct(null);
      setQemServersPhase("idle"); setQemServersPct(null);
      setQemPhase("idle"); setQemPct(null);
      setLicensePhase("idle"); setLicensePct(null);
      setMetricsPhase("idle"); setMetricsUploadPct(null);

      await loadCustomers();
      toast("Reset complete.", "ok");
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (e: any) {
      toast(`Reset failed: ${e.message}`, "err");
    }
  }

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

  // ---- Upload Handlers with progress ----
  async function handleUploadRepoJson() {
    if (!customerName) return toast("Pick a customer first.", "err");
    const file = repoFileRef.current?.files?.[0];
    if (!file) return toast("Choose a repository JSON file.", "err");
    try {
      setBusy(true);
      setRepoPhase("idle"); setRepoPct(0);
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);

      await xhrPost(`${API_BASE}/ingest-file`, fd, setRepoPct, setRepoPhase);

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
      setBusy(true);
      setQemServersPhase("idle"); setQemServersPct(0);
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);

      const j = await xhrPost(`${API_BASE}/ingest-qem-servers-file`, fd, setQemServersPct, setQemServersPhase);
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
      setBusy(true);
      setQemPhase("idle"); setQemPct(0);
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);

      const j = await xhrPost(`${API_BASE}/ingest-qem-file`, fd, setQemPct, setQemPhase);

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
      setBusy(true);
      setLicensePhase("idle"); setLicensePct(0);
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);

      const j = await xhrPost(`${API_BASE}/ingest-license-log`, fd, setLicensePct, setLicensePhase);
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

  async function handleUploadMetricsLog() {
    if (!customerName) return toast("Pick a customer first.", "err");
    if (!metricsServerName) return toast("Pick the Replicate server for this Metrics Log.", "err");
    const file = metricsFileRef.current?.files?.[0];
    if (!file) return toast("Choose a Metrics Log (TSV) file.", "err");
    try {
      setBusy(true);
      setMetricsPhase("idle"); setMetricsUploadPct(0);
      const fd = new FormData();
      fd.append("file", file);
      fd.append("customer_name", customerName);
      fd.append("server_name", metricsServerName);

      const j = await xhrPost(`${API_BASE}/ingest-metrics-log`, fd, setMetricsUploadPct, setMetricsPhase);
      const dupCount = Array.isArray(j?.duplicate_uuid_conflicts) ? j.duplicate_uuid_conflicts.length : 0;
      setMetricsSummary({
        rows: j?.rows ?? 0,
        inserted: j?.inserted ?? 0,
        matched: j?.matched_by_uuid ?? 0,
        dup_count: dupCount,
      });
      if (dupCount > 0) toast(`MetricsLog ingested with ${dupCount} duplicate UUID issue(s).`, "warn");
      else toast("Metrics Log ingested.", "ok");
    } catch (e: any) {
      toast(`Metrics Log ingest failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
      if (metricsFileRef.current) metricsFileRef.current.value = "";
    }
  }

  // Export with overlay
  async function downloadCustomerDocx() {
    if (!customerName) return toast("Select a customer first.", "err");
    setExporting(true);
    toast("Generating report…", "ok");
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
    } finally {
      setExporting(false);
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
      setMetricsSummary(null);
      toast("Deleted.", "ok");
    } catch (e: any) {
      toast(`Delete failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
    }
  }

  // Upload card component (generic) with progress
  function UploadCard({ title, icon, description, fileRef, accept, onUpload, phase, pct }: any) {
    const [fileName, setFileName] = useState("");
    const [isDragging, setIsDragging] = useState(false);

    return (
      <div
        className={`relative bg-white border rounded p-6 transition-all ${
          isDragging ? "border-green-500 border-2 shadow-lg" : "border-gray-200 hover:border-gray-300"
        }`}
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setIsDragging(false);
          const file = e.dataTransfer.files[0];
          if (file && fileRef.current) {
            const dt = new DataTransfer();
            dt.items.add(file);
            fileRef.current.files = dt.files;
            setFileName(file.name);
          }
        }}
      >
        <div className="flex items-start gap-4 mb-4">
          <div className="h-12 w-12 rounded bg-gray-100 grid place-items-center text-2xl flex-shrink-0">
            {icon}
          </div>
          <div className="flex-1 min-w-0">
            <h3 className="text-base font-semibold text-gray-900 mb-1">{title}</h3>
            <p className="text-sm text-gray-600">{description}</p>
          </div>
        </div>

        <div className="space-y-3">
          <input
            ref={fileRef}
            type="file"
            className="hidden"
            accept={accept}
            onChange={(e) => setFileName(e.target.files?.[0]?.name || "")}
          />

          <button
            onClick={() => fileRef.current?.click()}
            className="w-full rounded border border-gray-300 bg-white hover:bg-gray-50 px-4 h-10 text-sm text-gray-700 font-medium transition-colors"
          >
            {fileName ? `✓ ${fileName}` : "Choose File"}
          </button>

          <button
            onClick={onUpload}
            disabled={!customerName || busy || phase === "uploading" || phase === "processing"}
            className="w-full rounded bg-green-600 hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-4 h-10 text-sm text-white font-semibold transition-colors"
          >
            {phase === "uploading" || phase === "processing" ? "Processing…" : "Upload & Process"}
          </button>

          {/* Progress feedback */}
          {phase !== "idle" && (
            <div className="rounded bg-gray-50 border border-gray-200 p-3 space-y-2">
              {phase === "uploading" && (
                <>
                  <div className="text-xs text-gray-700 font-medium">Uploading… {pct ?? 0}%</div>
                  <div className="h-2 rounded-full bg-gray-200 overflow-hidden">
                    <div className="h-full bg-green-600 transition-all" style={{ width: `${pct ?? 0}%` }} />
                  </div>
                </>
              )}
              {phase === "processing" && (
                <>
                  <div className="text-xs text-gray-700 font-medium">Processing on server…</div>
                  <div className="h-2 rounded-full bg-gray-200 overflow-hidden">
                    <div className="h-full w-2/3 animate-progress-bar bg-green-600" />
                  </div>
                </>
              )}
              {phase === "done" && <div className="text-xs text-green-700 font-medium">Completed ✓</div>}
              {phase === "error" && <div className="text-xs text-red-700 font-medium">Failed. See toast for details.</div>}
            </div>
          )}
        </div>
      </div>
    );
  }

  // Specialized Metrics Log card (needs server selector)
  function MetricsLogCard() {
    const [fileName, setFileName] = useState("");
    const [isDragging, setIsDragging] = useState(false);

    return (
      <div
        className={`relative bg-white border rounded p-6 transition-all ${
          isDragging ? "border-green-500 border-2 shadow-lg" : "border-gray-200 hover:border-gray-300"
        }`}
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setIsDragging(false);
          const file = e.dataTransfer.files[0];
          if (file && metricsFileRef.current) {
            const dt = new DataTransfer();
            dt.items.add(file);
            metricsFileRef.current.files = dt.files;
            setFileName(file.name);
          }
        }}
      >
        <div className="flex items-start gap-4 mb-4">
          <div className="h-12 w-12 rounded bg-gray-100 grid place-items-center text-2xl flex-shrink-0">
            📈
          </div>
          <div className="flex-1 min-w-0">
            <h3 className="text-base font-semibold text-gray-900 mb-1">Metrics Log (TSV)</h3>
            <p className="text-sm text-gray-600">
              Per-Replicate server metrics; matched by <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">taskID</code> (Task UUID).
            </p>
          </div>
        </div>

        <div className="space-y-3">
          <select
            className="w-full rounded border border-gray-300 bg-white px-3 h-10 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:border-transparent"
            value={metricsServerName}
            onChange={(e) => setMetricsServerName(e.target.value)}
          >
            <option value="">Select server…</option>
            {servers.map((s) => (
              <option key={s.server_id} value={s.server_name}>
                {s.server_name}
              </option>
            ))}
          </select>

          <input
            ref={metricsFileRef}
            type="file"
            className="hidden"
            accept=".tsv,.txt"
            onChange={(e) => setFileName(e.target.files?.[0]?.name || "")}
          />
          <button
            onClick={() => metricsFileRef.current?.click()}
            className="w-full rounded border border-gray-300 bg-white hover:bg-gray-50 px-4 h-10 text-sm text-gray-700 font-medium transition-colors"
          >
            {fileName ? `✓ ${fileName}` : "Choose File"}
          </button>
          <button
            onClick={handleUploadMetricsLog}
            disabled={!customerName || busy || metricsPhase === "uploading" || metricsPhase === "processing"}
            className="w-full rounded bg-green-600 hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-4 h-10 text-sm text-white font-semibold transition-colors"
          >
            {metricsPhase === "uploading" || metricsPhase === "processing" ? "Processing…" : "Upload & Process"}
          </button>

          {/* Progress feedback */}
          {metricsPhase !== "idle" && (
            <div className="rounded bg-gray-50 border border-gray-200 p-3 space-y-2">
              {metricsPhase === "uploading" && (
                <>
                  <div className="text-xs text-gray-700 font-medium">Uploading… {metricsUploadPct ?? 0}%</div>
                  <div className="h-2 rounded-full bg-gray-200 overflow-hidden">
                    <div
                      className="h-full bg-green-600 transition-all"
                      style={{ width: `${metricsUploadPct ?? 0}%` }}
                    />
                  </div>
                </>
              )}
              {metricsPhase === "processing" && (
                <>
                  <div className="text-xs text-gray-700 font-medium">Processing on server…</div>
                  <div className="h-2 rounded-full bg-gray-200 overflow-hidden">
                    <div className="h-full w-2/3 animate-progress-bar bg-green-600" />
                  </div>
                </>
              )}
              {metricsPhase === "done" && <div className="text-xs text-green-700 font-medium">Completed ✓</div>}
              {metricsPhase === "error" && <div className="text-xs text-red-700 font-medium">Failed. See toast for details.</div>}
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-40 shadow-sm">
        <div className="mx-auto max-w-[1400px] px-6 py-4">
          <div className="flex items-center justify-between gap-6">
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-3">
                <svg className="h-8 w-8" viewBox="0 0 32 32" fill="none">
                  <rect width="32" height="32" rx="4" fill="#009845"/>
                  <path d="M16 8L8 12v8l8 4 8-4v-8l-8-4zm0 2.5l5.5 2.75v5.5L16 21.5l-5.5-2.75v-5.5L16 10.5z" fill="white"/>
                </svg>
                <div>
                  <div className="text-xl font-semibold text-gray-900">RepMeta Console</div>
                  <div className="text-xs text-gray-600">Data Pipeline & Analytics Platform</div>
                </div>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <button 
                className="rounded border border-gray-300 bg-white hover:bg-gray-50 px-4 h-9 text-sm text-gray-700 font-medium transition-colors" 
                onClick={handleHomeReset}
                disabled={exporting}
              >
                🏠 Reset
              </button>
              <button 
                className="rounded bg-green-600 hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-5 h-9 text-sm text-white font-semibold transition-colors" 
                onClick={downloadCustomerDocx}
                disabled={exporting || !customerName}
              >
                {exporting ? "⏳ Generating…" : "📥 Download Report"}
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="mx-auto max-w-[1400px] px-6 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          
          {/* Sidebar */}
          <div className="lg:col-span-1 space-y-6">
            {/* Progress Steps */}
            <div className="bg-white border border-gray-200 rounded p-5">
              <div className="flex items-center gap-2 mb-5">
                <div className="h-1 w-1 rounded-full bg-green-600"/>
                <h3 className="text-sm font-semibold text-gray-900 uppercase tracking-wide">Pipeline Progress</h3>
              </div>
              
              <div className="space-y-2">
                {[
                  { num: 1, label: "Add Customer", done: customers.length > 0 },
                  { num: 2, label: "Select Snapshot", done: customerId !== null },
                  { num: 3, label: "Upload Repository", done: servers.length > 0 },
                  { num: 4, label: "Upload Servers TSV", done: !!serversUpsertMsg },
                  { num: 5, label: "Upload QEM Metrics", done: !!qemSummary },
                  { num: 6, label: "Upload Metrics Log", done: !!metricsSummary },
                  { num: 7, label: "Upload License Log", done: !!licenseSummary },
                  { num: 8, label: "Generate Report", done: false },
                ].map((step) => {
                  const isActive = step.num === currentStep;
                  return (
                    <div
                      key={step.num}
                      className={`rounded p-3 transition-all ${
                        step.done 
                          ? "bg-green-50 border border-green-200" 
                          : isActive
                          ? "bg-blue-50 border border-blue-200"
                          : "bg-gray-50 border border-gray-200"
                      }`}
                    >
                      <div className="flex items-center gap-3">
                        <div
                          className={`h-6 w-6 shrink-0 rounded-full grid place-items-center text-xs font-semibold ${
                            step.done
                              ? "bg-green-600 text-white"
                              : isActive
                              ? "bg-blue-600 text-white"
                              : "bg-gray-200 text-gray-600"
                          }`}
                        >
                          {step.done ? "✓" : step.num}
                        </div>
                        <span className={`text-sm font-medium ${step.done || isActive ? "text-gray-900" : "text-gray-600"}`}>
                          {step.label}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Stats Card */}
            <div className="bg-white border border-gray-200 rounded p-5">
              <div className="flex items-center gap-2 mb-5">
                <div className="h-1 w-1 rounded-full bg-green-600"/>
                <h3 className="text-sm font-semibold text-gray-900 uppercase tracking-wide">Analytics</h3>
              </div>
              
              <div className="space-y-4">
                <div className="rounded bg-gradient-to-br from-green-50 to-green-100 border border-green-200 p-4">
                  <div className="text-xs font-semibold text-green-800 uppercase tracking-wide mb-1">Customer</div>
                  <div className="text-lg font-bold text-gray-900 truncate">{customerName || "—"}</div>
                </div>
                
                <div className="grid grid-cols-2 gap-3">
                  <div className="rounded bg-blue-50 border border-blue-200 p-3">
                    <div className="text-xs font-semibold text-blue-800 uppercase tracking-wide mb-1">Servers</div>
                    <div className="text-xl font-bold text-gray-900">{servers.length}</div>
                  </div>
                  
                  <div className="rounded bg-purple-50 border border-purple-200 p-3">
                    <div className="text-xs font-semibold text-purple-800 uppercase tracking-wide mb-1">Tasks</div>
                    <div className="text-xl font-bold text-gray-900">{qemSummary?.matched ?? "—"}</div>
                  </div>
                </div>
                
                <div className="rounded bg-indigo-50 border border-indigo-200 p-3">
                  <div className="text-xs font-semibold text-indigo-800 uppercase tracking-wide mb-1">QEM Events</div>
                  <div className="text-2xl font-bold text-gray-900">{qemSummary?.rows?.toLocaleString() ?? "—"}</div>
                </div>

                {metricsSummary && (
                  <div className="rounded bg-orange-50 border border-orange-200 p-3">
                    <div className="text-xs font-semibold text-orange-800 uppercase tracking-wide mb-1">Metrics Log</div>
                    <div className="text-gray-900">
                      <div className="text-lg font-bold">
                        {metricsSummary.rows?.toLocaleString?.() ?? metricsSummary.rows ?? 0} rows
                      </div>
                      <div className="text-xs text-gray-700 mt-1">
                        Inserted {metricsSummary.inserted ?? 0} · Matched {metricsSummary.matched ?? 0}
                        {typeof metricsSummary.dup_count === "number" && metricsSummary.dup_count > 0 && (
                          <> · Duplicates {metricsSummary.dup_count}</>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Main Workbench */}
          <div className="lg:col-span-3 space-y-6">
            
            {/* Customer Management */}
            <div className="bg-white border border-gray-200 rounded p-6">
              <div className="flex items-center gap-3 mb-6">
                <span className="text-2xl">🏢</span>
                <h2 className="text-lg font-semibold text-gray-900">Customer Management</h2>
              </div>
              
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div className="space-y-2">
                  <label className="block text-sm font-semibold text-gray-700">Create New Customer</label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      placeholder="Enter customer name..."
                      value={newCustomerName}
                      onChange={(e) => setNewCustomerName(e.target.value)}
                      onKeyDown={(e) => e.key === "Enter" && handleAddCustomer()}
                      className="flex-1 rounded border border-gray-300 bg-white px-3 h-10 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    />
                    <button
                      onClick={handleAddCustomer}
                      className="rounded bg-green-600 hover:bg-green-700 px-5 h-10 text-sm text-white font-semibold transition-colors"
                    >
                      Add
                    </button>
                  </div>
                </div>
                
                <div className="space-y-2">
                  <label className="block text-sm font-semibold text-gray-700">Select Active Customer</label>
                  <select
                    className="w-full rounded border border-gray-300 bg-white px-3 h-10 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    value={customerId ?? ""}
                    onChange={(e) => setCustomerId(e.target.value ? Number(e.target.value) : null)}
                  >
                    <option value="">Select a customer...</option>
                    {customers.map((c) => (
                      <option key={c.customer_id} value={c.customer_id}>
                        {c.customer_name}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Server Grid */}
              {customerId && (
                <div className="mt-6 space-y-4">
                  <div className="flex items-center justify-between">
                    <h3 className="text-base font-semibold text-gray-900 flex items-center gap-2">
                      <span className="text-xl">🖥️</span>
                      Connected Servers
                    </h3>
                    <div className="px-3 py-1 rounded-full bg-green-100 border border-green-300">
                      <span className="text-xs font-semibold text-green-800">{servers.length} Active</span>
                    </div>
                  </div>
                  
                  {servers.length === 0 ? (
                    <div className="rounded border-2 border-dashed border-gray-300 bg-gray-50 p-12 text-center">
                      <div className="text-5xl mb-3">📡</div>
                      <div className="text-sm text-gray-600">No servers detected yet. Upload repository JSON to begin.</div>
                    </div>
                  ) : (
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                      {servers.map((s) => (
                        <div
                          key={s.server_id}
                          className="rounded border border-gray-200 bg-white hover:border-gray-300 hover:shadow p-4 transition-all"
                        >
                          <div className="flex items-start justify-between mb-2">
                            <div className="h-8 w-8 rounded bg-gray-100 grid place-items-center text-lg">
                              🖥️
                            </div>
                            <div className="h-2 w-2 rounded-full bg-green-500"/>
                          </div>
                          <div className="font-semibold text-sm text-gray-900 mb-1">{s.server_name}</div>
                          <div className="text-xs text-gray-500">
                            {s.environment ? `Env: ${s.environment}` : "No environment"}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Upload Section */}
            <div className="space-y-4">
              <div className="flex items-center gap-3">
                <span className="text-2xl">📤</span>
                <h2 className="text-lg font-semibold text-gray-900">Data Pipeline</h2>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <UploadCard
                  title="Repository JSON"
                  icon="📋"
                  description="Auto-detect servers from repository exports"
                  fileRef={repoFileRef}
                  accept=".json"
                  onUpload={handleUploadRepoJson}
                  phase={repoPhase}
                  pct={repoPct}
                />
                <UploadCard
                  title="QEM Servers (TSV)"
                  icon="🔗"
                  description="Map ServerName to repository server names"
                  fileRef={qemServersFileRef}
                  accept=".tsv,.csv,.txt"
                  onUpload={handleUploadQemServersTsv}
                  phase={qemServersPhase}
                  pct={qemServersPct}
                />
                <UploadCard
                  title="QEM Metrics (TSV)"
                  icon="📊"
                  description="Upload quality & performance metrics"
                  fileRef={qemFileRef}
                  accept=".tsv,.csv,.txt"
                  onUpload={handleUploadQemTsv}
                  phase={qemPhase}
                  pct={qemPct}
                />
                <UploadCard
                  title="License Log"
                  icon="🔐"
                  description="Parse licensing information for compliance"
                  fileRef={licenseFileRef}
                  accept=".log,.txt"
                  onUpload={handleUploadLicenseLog}
                  phase={licensePhase}
                  pct={licensePct}
                />
                <MetricsLogCard />
              </div>
            </div>

            {/* Status Messages */}
            {(ingestMsg || serversUpsertMsg || qemSummary || licenseSummary || metricsSummary) && (
              <div className="space-y-4">
                {ingestMsg && (
                  <div className="rounded border border-green-200 bg-green-50 p-4">
                    <div className="flex items-center gap-3">
                      <span className="text-xl">✅</span>
                      <span className="text-sm font-medium text-gray-900">{ingestMsg}</span>
                    </div>
                  </div>
                )}
                
                {serversUpsertMsg && (
                  <div className="rounded border border-blue-200 bg-blue-50 p-4">
                    <div className="flex items-center gap-3">
                      <span className="text-xl">🔗</span>
                      <span className="text-sm font-medium text-gray-900">{serversUpsertMsg}</span>
                    </div>
                  </div>
                )}
                
                {qemSummary && (
                  <div className="rounded border border-purple-200 bg-white p-6">
                    <div className="flex items-center gap-3 mb-4">
                      <span className="text-2xl">📊</span>
                      <h3 className="text-base font-semibold text-gray-900">QEM Processing Summary</h3>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Rows</div>
                        <div className="text-xl font-bold text-gray-900">{qemSummary.rows?.toLocaleString()}</div>
                      </div>
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Inserted</div>
                        <div className="text-xl font-bold text-gray-900">{qemSummary.inserted?.toLocaleString()}</div>
                      </div>
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Matched</div>
                        <div className="text-xl font-bold text-gray-900">{qemSummary.matched?.toLocaleString()}</div>
                      </div>
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Mode</div>
                        <div className="text-lg font-bold text-gray-900">{qemSummary.match_mode}</div>
                      </div>
                    </div>
                  </div>
                )}

                {metricsSummary && (
                  <div className="rounded border border-orange-200 bg-white p-6">
                    <div className="flex items-center gap-3 mb-4">
                      <span className="text-2xl">📈</span>
                      <h3 className="text-base font-semibold text-gray-900">Metrics Log Summary</h3>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Rows</div>
                        <div className="text-xl font-bold text-gray-900">{metricsSummary.rows ?? 0}</div>
                      </div>
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Inserted</div>
                        <div className="text-xl font-bold text-gray-900">{metricsSummary.inserted ?? 0}</div>
                      </div>
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Matched</div>
                        <div className="text-xl font-bold text-gray-900">{metricsSummary.matched ?? 0}</div>
                      </div>
                      <div className="rounded bg-gray-50 border border-gray-200 p-3">
                        <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Duplicate UUIDs</div>
                        <div className="text-xl font-bold text-gray-900">{metricsSummary.dup_count ?? 0}</div>
                      </div>
                    </div>
                  </div>
                )}
                
                {licenseSummary && (
                  <div className="rounded border border-pink-200 bg-white p-6">
                    <div className="flex items-center gap-3 mb-4">
                      <span className="text-2xl">🔐</span>
                      <h3 className="text-base font-semibold text-gray-900">License Information</h3>
                    </div>
                    <div className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div className="rounded bg-gray-50 border border-gray-200 p-3">
                          <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">All Sources</div>
                          <div className="text-xl font-bold text-gray-900">{licenseSummary.all_sources ? "✓ Yes" : "✗ No"}</div>
                        </div>
                        <div className="rounded bg-gray-50 border border-gray-200 p-3">
                          <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">All Targets</div>
                          <div className="text-xl font-bold text-gray-900">{licenseSummary.all_targets ? "✓ Yes" : "✗ No"}</div>
                        </div>
                      </div>
                      {Array.isArray(licenseSummary.sources) && licenseSummary.sources.length > 0 && (
                        <div className="rounded bg-gray-50 border border-gray-200 p-4">
                          <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-2">Licensed Sources</div>
                          <div className="flex flex-wrap gap-2">
                            {licenseSummary.sources.map((src, i) => (
                              <span key={i} className="px-3 py-1 rounded-full bg-green-100 border border-green-300 text-xs font-medium text-gray-900">
                                {src}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                      {Array.isArray(licenseSummary.targets) && licenseSummary.targets.length > 0 && (
                        <div className="rounded bg-gray-50 border border-gray-200 p-4">
                          <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-2">Licensed Targets</div>
                          <div className="flex flex-wrap gap-2">
                            {licenseSummary.targets.map((tgt, i) => (
                              <span key={i} className="px-3 py-1 rounded-full bg-green-100 border border-green-300 text-xs font-medium text-gray-900">
                                {tgt}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Cleanup Section */}
            <div className="bg-white border border-red-200 rounded p-6">
              <div className="flex items-center gap-3 mb-6">
                <span className="text-2xl">🗑️</span>
                <h2 className="text-lg font-semibold text-gray-900">Data Cleanup</h2>
              </div>
              
              <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-6">
                <label className="flex items-center gap-3 text-gray-700 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={deleteAlsoServers}
                    onChange={(e) => setDeleteAlsoServers(e.target.checked)}
                    className="h-4 w-4 rounded border-gray-300 text-green-600 focus:ring-green-500"
                  />
                  <span className="text-sm font-medium">Also delete server configurations</span>
                </label>
                
                <button
                  onClick={handleDeleteAll}
                  disabled={!customerId || busy}
                  className="rounded bg-red-600 hover:bg-red-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-5 h-10 text-sm text-white font-semibold transition-colors"
                >
                  ⚠️ Delete All Data
                </button>
              </div>
            </div>

            {/* Action Bar */}
            <div className="flex flex-col md:flex-row items-center justify-between gap-6 pt-4 border-t border-gray-200">
              <div className="text-gray-600 text-sm">
                Built with 💚 for seamless data pipeline management
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={() => setHelpOpen(true)}
                  className="rounded border border-gray-300 bg-white hover:bg-gray-50 px-4 h-9 text-sm text-gray-700 font-medium transition-colors"
                >
                  💡 Quick Guide
                </button>
                <button
                  onClick={downloadCustomerDocx}
                  disabled={!customerName || exporting}
                  className="rounded bg-green-600 hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-5 h-9 text-sm text-white font-semibold transition-colors"
                >
                  {exporting ? "⏳ Generating…" : "📥 Generate Report"}
                </button>
              </div>
            </div>
          </div>
        </div>
      </main>

      {/* Help Drawer */}
      {helpOpen && (
        <div className="fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/40" onClick={() => setHelpOpen(false)} />
          <aside className="absolute right-0 top-0 h-full w-[600px] max-w-[95vw] bg-white shadow-2xl overflow-y-auto">
            <div className="flex items-center justify-between border-b border-gray-200 px-6 py-5 bg-gray-50 sticky top-0">
              <div className="flex items-center gap-3">
                <span className="text-2xl">💡</span>
                <h2 className="text-lg font-semibold text-gray-900">Quick Start Guide</h2>
              </div>
              <button 
                onClick={() => setHelpOpen(false)} 
                className="h-8 w-8 rounded hover:bg-gray-200 transition-colors grid place-items-center"
              >
                <span className="text-gray-600 text-xl">✕</span>
              </button>
            </div>
            
            <div className="p-6 space-y-4">
              <div className="space-y-3">
                {[
                  { num: 1, title: "Create Customer", desc: "Start by adding a new customer or selecting an existing one from the dropdown.", icon: "🏢" },
                  { num: 2, title: "Upload Repository", desc: "Upload the JSON export from your repository. This auto-detects server configurations.", icon: "📋" },
                  { num: 3, title: "Map QEM Servers", desc: "If your QEM data lacks Host information, upload the Servers TSV to create mappings.", icon: "🔗" },
                  { num: 4, title: "Import Metrics", desc: "Upload the QEM Metrics TSV file to process quality and performance data.", icon: "📊" },
                  { num: 5, title: "Upload Metrics Log", desc: "Optional: ingest per-server Metrics Log to capture rows/bytes by task UUID.", icon: "📈" },
                  { num: 6, title: "Add License Info", desc: "Optionally upload the license log to track compliance and entitlements.", icon: "🔐" },
                  { num: 7, title: "Generate Report", desc: "Click 'Generate Report' to create a polished Word document with all insights.", icon: "📥" },
                ].map((step) => (
                  <div key={step.num} className="border border-gray-200 rounded p-4 hover:border-gray-300 hover:shadow transition-all">
                    <div className="flex gap-4">
                      <div className="shrink-0">
                        <div className="h-10 w-10 rounded bg-gray-100 grid place-items-center text-xl">
                          {step.icon}
                        </div>
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-2">
                          <div className="h-5 w-5 rounded-full bg-green-600 grid place-items-center text-xs font-semibold text-white">
                            {step.num}
                          </div>
                          <h3 className="text-sm font-semibold text-gray-900">{step.title}</h3>
                        </div>
                        <p className="text-sm text-gray-600">{step.desc}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              
              <div className="rounded border border-green-200 bg-green-50 p-5">
                <div className="flex items-center gap-3 mb-3">
                  <span className="text-xl">💡</span>
                  <h3 className="text-sm font-semibold text-gray-900">Pro Tips</h3>
                </div>
                <ul className="space-y-2 text-sm text-gray-700">
                  <li className="flex items-start gap-2">
                    <span className="text-green-600 mt-0.5">▸</span>
                    <span>Always upload Repository JSON first to establish server baseline</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-green-600 mt-0.5">▸</span>
                    <span>Use drag-and-drop for faster file uploads</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-green-600 mt-0.5">▸</span>
                    <span>Watch the Progress sidebar to track your pipeline status</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-green-600 mt-0.5">▸</span>
                    <span>Export reports anytime after uploading core data</span>
                  </li>
                </ul>
              </div>
            </div>
          </aside>
        </div>
      )}

      {/* Export overlay */}
      <LoaderOverlay show={exporting} title="Generating report…" subtitle="Packaging insights into a Word document" />

      <style>{`
        @keyframes progress-move { 
          0% { transform: translateX(-100%);} 
          100% { transform: translateX(100%);} 
        }
        .animate-progress-bar { 
          animation: progress-move 1.2s linear infinite; 
        }
      `}</style>
    </div>
  );
}