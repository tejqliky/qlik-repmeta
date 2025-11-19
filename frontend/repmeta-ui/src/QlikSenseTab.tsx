import React, { useEffect, useMemo, useRef, useState } from "react";

/** Types */
type Customer = { customer_id: number; customer_name: string };
type Phase = "idle" | "uploading" | "processing" | "done" | "error";

type SenseSummary = {
  environment?: {
    product_name?: string;
    product_version?: string;
  };
  counts?: { apps: number; streams: number; users: number; reload_tasks: number };
  governance?: {
    apps_without_tasks?: number;
    disabled_tasks_count?: number;
  };
};

/** Keep API base identical to Replicate tab to avoid surprises */
const API_BASE = "http://127.0.0.1:8002";

/** ---- helpers (mirrored from Replicate lane) ---- */
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
  el.style.background = kind === "ok" ? "#009845" : kind === "warn" ? "#F57C00" : "#D32F2F";
  el.style.boxShadow = "0 4px 12px rgba(0,0,0,.15)";
  el.style.zIndex = "9999";
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 2800);
}

/** Shared XHR uploader with progress (same pattern as Replicate lane) */
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
          try {
            data = JSON.parse(xhr.responseText || "{}");
          } catch {}
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

/** Light overlay while exporting/deleting */
function LoaderOverlay({
  show,
  title,
  subtitle,
  icon = "⏳",
}: {
  show: boolean;
  title: string;
  subtitle?: string;
  icon?: string;
}) {
  if (!show) return null;
  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/40 backdrop-blur-[1px]">
      <div className="rounded-xl bg-white p-8 shadow-2xl max-w-md w-[90vw] text-center animate-modal-in border border-gray-100">
        <div className="mx-auto mb-4 h-12 w-12 rounded-full bg-gray-100 grid place-items-center text-2xl">
          {icon}
        </div>
        <div className="text-gray-900 font-semibold text-lg mb-1">{title}</div>
        {subtitle && <div className="text-gray-600 text-sm">{subtitle}</div>}
        <div className="mt-6 h-2 rounded-full bg-gray-200 overflow-hidden">
          <div className="h-full w-2/3 animate-progress-bar bg-green-600" />
        </div>
      </div>
    </div>
  );
}

/** ---- Qlik Sense tab ---- */
const QlikSenseTab: React.FC = () => {
  // Data sets
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [customerId, setCustomerId] = useState<number | null>(null);

  // Customer creation
  const [newCustomerName, setNewCustomerName] = useState("");

  // Upload refs
  const zipRef = useRef<HTMLInputElement | null>(null);
  const jsonsRef = useRef<HTMLInputElement | null>(null);

  // UI / flow
  const [busy, setBusy] = useState(false);
  const [currentStep, setCurrentStep] = useState(1);
  const [ingestPhase, setIngestPhase] = useState<Phase>("idle");
  const [ingestPct, setIngestPct] = useState<number | null>(null);
  const [snapshotId, setSnapshotId] = useState<string | null>(null);
  const [summary, setSummary] = useState<SenseSummary | null>(null);
  const [exporting, setExporting] = useState(false);

  // Notes (optional metadata for snapshot)
  const [notes, setNotes] = useState<string>("");

  const customerName = useMemo(
    () => customers.find((c) => c.customer_id === customerId)?.customer_name ?? "",
    [customers, customerId]
  );

  // Smart duplicate detection for "Create New Customer"
  const duplicateCustomer = useMemo(() => {
    const name = newCustomerName.trim().toLowerCase();
    if (!name) return null;
    return customers.find(
      (c) => c.customer_name.trim().toLowerCase() === name
    ) || null;
  }, [newCustomerName, customers]);

  // Step auto-advance (mirror Replicate behavior)
  useEffect(() => {
    if (customers.length > 0) setCurrentStep((s) => Math.max(s, 2));
    if (customerId) setCurrentStep((s) => Math.max(s, 3));
    if (snapshotId) setCurrentStep((s) => Math.max(s, 4));
  }, [customers.length, customerId, snapshotId]);

  /** loaders */
  async function loadCustomers() {
    const rows = await fetchJson<Customer[]>(`${API_BASE}/customers`);
    setCustomers(rows.sort((a, b) => a.customer_name.localeCompare(b.customer_name)));
    if (rows.length === 1) setCustomerId(rows[0].customer_id);
  }
  useEffect(() => {
    (async () => {
      try {
        await loadCustomers();
      } catch (e: any) {
        toast(`Load customers failed: ${e.message}`, "err");
      }
    })();
  }, []);

  /** actions */
  async function handleAddCustomer() {
    const name = newCustomerName.trim();
    if (!name) return toast("Enter a customer name.", "err");
    if (duplicateCustomer) {
      setCustomerId(duplicateCustomer.customer_id);
      setNewCustomerName("");
      toast(`Switched to existing customer "${duplicateCustomer.customer_name}".`, "ok");
      return;
    }
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

  function resetFiles() {
    if (zipRef.current) zipRef.current.value = "";
    if (jsonsRef.current) jsonsRef.current.value = "";
  }

  async function handleIngest() {
    if (!customerId) return toast("Pick a customer first.", "err");
    const zip = zipRef.current?.files?.[0] ?? null;
    const jsons = jsonsRef.current?.files ?? null;
    if (!zip && !(jsons && jsons.length > 0)) {
      return toast("Choose a ZIP or one or more Qlik*.json files.", "err");
    }

    try {
      setBusy(true);
      setIngestPhase("idle");
      setIngestPct(0);

      const fd = new FormData();
      fd.append("customer_id", String(customerId));
      if (notes?.trim()) fd.append("notes", notes.trim());
      if (zip) {
        fd.append("file", zip); // backend expects "file" for ZIP
      } else if (jsons && jsons.length > 0) {
        Array.from(jsons).forEach((f) => fd.append("files", f)); // backend expects "files" for multiple JSONs
      }

      // Upload + ingest (progress UI identical to Replicate)
      const j = await xhrPost(`${API_BASE}/qliksense/ingest`, fd, setIngestPct, setIngestPhase);

      const sid = `${j?.snapshot_id ?? ""}`.trim();
      if (!sid) throw new Error("No snapshot_id returned from server.");

      setSnapshotId(sid);
      toast("Qlik Sense artifacts ingested.", "ok");

      // Fetch summary for info cards
      try {
        const sum = await fetchJson<SenseSummary>(`${API_BASE}/qliksense/summary?snapshot_id=${encodeURIComponent(sid)}`);
        setSummary(sum || {});
      } catch (e: any) {
        toast(`Summary fetch failed: ${e.message}`, "warn");
      }

      setIngestPhase("done");
      setIngestPct(null);
    } catch (e: any) {
      setIngestPhase("error");
      toast(`Ingest failed: ${e.message}`, "err");
    } finally {
      setBusy(false);
      resetFiles();
    }
  }

  async function downloadSenseDocx() {
    if (!snapshotId) return toast("Ingest first to create a snapshot.", "err");
    setExporting(true);
    toast("Generating Qlik Sense report…", "ok");
    try {
      const url = `${API_BASE}/qliksense/report?snapshot_id=${encodeURIComponent(snapshotId)}`;
      const res = await fetch(url);
      if (!res.ok) {
        let d = `${res.status} ${res.statusText}`;
        try {
          const j = await res.json();
          d = j?.detail ? j.detail : JSON.stringify(j);
        } catch {}
        throw new Error(d);
      }
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `QlikSense_Technical_Overview_${customerName || "Snapshot"}.docx`.replace(/\s+/g, "_");
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e: any) {
      toast(`Export failed: ${e.message}`, "err");
    } finally {
      setExporting(false);
    }
  }

  /** Generic Upload Card (drag & drop + progress) */
  function UploadCard({
    title,
    icon,
    description,
    fileControl,
    accept,
    onUpload,
    phase,
    pct,
  }: {
    title: string;
    icon: string;
    description: string;
    fileControl: React.RefObject<HTMLInputElement>;
    accept: string;
    onUpload: () => void;
    phase: Phase;
    pct: number | null;
  }) {
    const [fileName, setFileName] = useState("");
    const [isDragging, setIsDragging] = useState(false);
    return (
      <div
        className={`relative bg-white border rounded p-6 transition-all ${
          isDragging ? "border-green-500 border-2 shadow-lg" : "border-gray-200 hover:border-gray-300"
        }`}
        onDragOver={(e) => {
          e.preventDefault();
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setIsDragging(false);
          const file = e.dataTransfer.files[0];
          if (file && fileControl.current) {
            const dt = new DataTransfer();
            dt.items.add(file);
            fileControl.current.files = dt.files;
            setFileName(file.name);
          }
        }}
      >
        <div className="flex items-start gap-4 mb-4">
          <div className="h-12 w-12 rounded bg-gray-100 grid place-items-center text-2xl flex-shrink-0">{icon}</div>
          <div className="flex-1 min-w-0">
            <h3 className="text-base font-semibold text-gray-900 mb-1">{title}</h3>
            <p className="text-sm text-gray-600">{description}</p>
          </div>
        </div>

        <div className="space-y-3">
          <input
            ref={fileControl}
            type="file"
            className="hidden"
            accept={accept}
            onChange={(e) => setFileName(e.target.files?.[0]?.name || (e.target.files && e.target.files.length > 1 ? `${e.target.files.length} files` : ""))}
            multiple={accept.includes(".json")} // enable multiple for jsons control
          />

          <button
            onClick={() => fileControl.current?.click()}
            className="w-full rounded border border-gray-300 bg-white hover:bg-gray-50 px-4 h-10 text-sm text-gray-700 font-medium transition-colors"
          >
            {fileName ? `✓ ${fileName}` : "Choose File(s)"}
          </button>

          <button
            onClick={onUpload}
            disabled={!customerName || busy || phase === "uploading" || phase === "processing"}
            className="w-full rounded bg-green-600 hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-4 h-10 text-sm text-white font-semibold transition-colors"
          >
            {phase === "uploading" || phase === "processing" ? "Processing…" : "Upload & Ingest"}
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

  return (
    <div className="mx-auto max-w-[1400px] px-6 py-8">
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Sidebar: Progress + Analytics */}
        <div className="lg:col-span-1 space-y-6">
          {/* Progress Steps (mirrors Replicate) */}
          <div className="bg-white border border-gray-200 rounded p-5">
            <div className="flex items-center gap-2 mb-5">
              <div className="h-1 w-1 rounded-full bg-green-600" />
              <h3 className="text-sm font-semibold text-gray-900 uppercase tracking-wide">Pipeline Progress</h3>
            </div>

            <div className="space-y-2">
              {[
                { num: 1, label: "Add Customer", done: customers.length > 0 },
                { num: 2, label: "Select Snapshot", done: customerId !== null },
                { num: 3, label: "Upload Qlik Sense", done: ingestPhase === "done" || !!snapshotId },
                { num: 4, label: "Generate Report", done: false },
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

          {/* Analytics Card (Sense-specific) */}
          <div className="bg-white border border-gray-200 rounded p-5">
            <div className="flex items-center gap-2 mb-5">
              <div className="h-1 w-1 rounded-full bg-green-600" />
              <h3 className="text-sm font-semibold text-gray-900 uppercase tracking-wide">Analytics</h3>
            </div>

            <div className="space-y-4">
              <div className="rounded bg-gradient-to-br from-green-50 to-green-100 border border-green-200 p-4">
                <div className="text-xs font-semibold text-green-800 uppercase tracking-wide mb-1">Customer</div>
                <div className="text-lg font-bold text-gray-900 truncate">{customerName || "—"}</div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div className="rounded bg-blue-50 border border-blue-200 p-3">
                  <div className="text-xs font-semibold text-blue-800 uppercase tracking-wide mb-1">Apps</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.apps ?? "—"}</div>
                </div>
                <div className="rounded bg-purple-50 border border-purple-200 p-3">
                  <div className="text-xs font-semibold text-purple-800 uppercase tracking-wide mb-1">Streams</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.streams ?? "—"}</div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div className="rounded bg-orange-50 border border-orange-200 p-3">
                  <div className="text-xs font-semibold text-orange-800 uppercase tracking-wide mb-1">Users</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.users ?? "—"}</div>
                </div>
                <div className="rounded bg-indigo-50 border border-indigo-200 p-3">
                  <div className="text-xs font-semibold text-indigo-800 uppercase tracking-wide mb-1">Reload Tasks</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.reload_tasks ?? "—"}</div>
                </div>
              </div>

              {summary && (
                <div className="rounded bg-gray-50 border border-gray-200 p-3">
                  <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Environment</div>
                  <div className="text-sm text-gray-900">
                    <div>Product: {summary.environment?.product_name ?? "—"}</div>
                    <div>Version: {summary.environment?.product_version ?? "—"}</div>
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
            <div className="flex items-center gap-3 mb-4">
              <span className="text-2xl">🏢</span>
              <h2 className="text-lg font-semibold text-gray-900">Customer Management</h2>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="space-y-2">
                <label className="block text-sm font-semibold text-gray-700">Create New Customer</label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    placeholder="Add only if not listed…"
                    value={newCustomerName}
                    onChange={(e) => setNewCustomerName(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleAddCustomer()}
                    className={`flex-1 rounded border bg-white px-3 h-10 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-green-500 focus:border-transparent ${
                      duplicateCustomer ? "border-amber-400" : "border-gray-300"
                    }`}
                    aria-describedby="sense-customer-add-hint"
                  />
                  <button
                    onClick={handleAddCustomer}
                    disabled={!!duplicateCustomer}
                    className={`rounded px-5 h-10 text-sm text-white font-semibold transition-colors ${
                      duplicateCustomer ? "bg-gray-300 cursor-not-allowed" : "bg-green-600 hover:bg-green-700"
                    }`}
                    title={duplicateCustomer ? "This customer already exists — select it instead" : "Add customer"}
                  >
                    Add
                  </button>
                </div>

                {duplicateCustomer ? (
                  <div
                    id="sense-customer-add-hint"
                    className="mt-2 flex items-center justify-between rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900"
                  >
                    <span>Looks like “{duplicateCustomer.customer_name}” already exists.</span>
                    <button
                      className="ml-3 rounded bg-amber-600 hover:bg-amber-700 text-white px-2.5 py-1 text-xs font-semibold"
                      onClick={() => {
                        setCustomerId(duplicateCustomer.customer_id);
                        setNewCustomerName("");
                        toast(`Switched to existing customer "${duplicateCustomer.customer_name}".`, "ok");
                      }}
                    >
                      Select existing
                    </button>
                  </div>
                ) : (
                  <div id="sense-customer-add-hint" className="text-xs text-gray-500 mt-1">
                    Enter only if the customer isn’t in the list.
                  </div>
                )}
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
          </div>

          {/* Upload Section */}
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <span className="text-2xl">📤</span>
              <h2 className="text-lg font-semibold text-gray-900">Qlik Sense Artifacts</h2>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Snapshot Notes */}
              <div className="bg-white border rounded p-6">
                <div className="flex items-start gap-4 mb-4">
                  <div className="h-12 w-12 rounded bg-gray-100 grid place-items-center text-2xl flex-shrink-0">🗒️</div>
                  <div className="flex-1 min-w-0">
                    <h3 className="text-base font-semibold text-gray-900 mb-1">Snapshot Notes (optional)</h3>
                    <p className="text-sm text-gray-600">E.g., “Pre-upgrade export”, “Prod baseline”, etc.</p>
                  </div>
                </div>
                <input
                  className="w-full rounded border border-gray-300 bg-white px-3 h-10 text-sm text-gray-900"
                  placeholder="Short note to tag this snapshot…"
                  value={notes}
                  onChange={(e) => setNotes(e.target.value)}
                />
              </div>

              <UploadCard
                title="Upload ZIP"
                icon="📦"
                description="Preferred: a single ZIP containing Qlik*.json files."
                fileControl={zipRef}
                accept=".zip"
                onUpload={handleIngest}
                phase={ingestPhase}
                pct={ingestPct}
              />

              <UploadCard
                title="Or Upload JSONs"
                icon="📄"
                description="Select one or more Qlik*.json files."
                fileControl={jsonsRef}
                accept=".json"
                onUpload={handleIngest}
                phase={ingestPhase}
                pct={ingestPct}
              />
            </div>
          </div>

          {/* Post-ingest summary */}
          {(snapshotId || summary) && (
            <div className="rounded border border-gray-200 bg-white p-6">
              <div className="flex items-center gap-3 mb-4">
                <span className="text-2xl">📊</span>
                <h3 className="text-base font-semibold text-gray-900">Ingest Summary</h3>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="rounded bg-gray-50 border border-gray-200 p-3">
                  <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Snapshot</div>
                  <div className="text-sm font-mono text-gray-900 truncate">{snapshotId ?? "—"}</div>
                </div>
                <div className="rounded bg-gray-50 border border-gray-200 p-3">
                  <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Apps</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.apps ?? "—"}</div>
                </div>
                <div className="rounded bg-gray-50 border border-gray-200 p-3">
                  <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Streams</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.streams ?? "—"}</div>
                </div>
                <div className="rounded bg-gray-50 border border-gray-200 p-3">
                  <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Reload Tasks</div>
                  <div className="text-xl font-bold text-gray-900">{summary?.counts?.reload_tasks ?? "—"}</div>
                </div>
              </div>

              {summary && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
                  <div className="rounded bg-gray-50 border border-gray-200 p-3">
                    <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Governance</div>
                    <div className="text-sm text-gray-900">
                      Apps w/o Tasks: {summary.governance?.apps_without_tasks ?? "—"}
                      <br />
                      Disabled Tasks: {summary.governance?.disabled_tasks_count ?? "—"}
                    </div>
                  </div>
                  <div className="rounded bg-gray-50 border border-gray-200 p-3">
                    <div className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-1">Environment</div>
                    <div className="text-sm text-gray-900">
                      Product: {summary.environment?.product_name ?? "—"}
                      <br />
                      Version: {summary.environment?.product_version ?? "—"}
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Action Bar */}
          <div className="flex flex-col md:flex-row items-center justify-between gap-6 pt-4 border-t border-gray-200">
            <div className="text-gray-600 text-sm">Insight, distilled from impact</div>
            <div className="flex items-center gap-3">
              <button
                onClick={downloadSenseDocx}
                disabled={!snapshotId || exporting}
                className="rounded bg-green-600 hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed px-5 h-9 text-sm text-white font-semibold transition-colors"
              >
                {exporting ? "⏳ Generating…" : "📥 Generate Report"}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Export overlay */}
      <LoaderOverlay
        show={exporting}
        title="Generating report…"
        subtitle="Packaging Qlik Sense insights into a Word document"
      />

      {/* local styles (same small animation helpers as Replicate) */}
      <style>{`
        @keyframes progress-move { 
          0% { transform: translateX(-100%);} 
          100% { transform: translateX(100%);} 
        }
        .animate-progress-bar { 
          animation: progress-move 1.2s linear infinite; 
        }
        @keyframes modal-in {
          0% { opacity: 0; transform: scale(.96); }
          100% { opacity: 1; transform: scale(1); }
        }
        .animate-modal-in {
          animation: modal-in .14s ease-out;
        }
      `}</style>
    </div>
  );
};

export default QlikSenseTab;
