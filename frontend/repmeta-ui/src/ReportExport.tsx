import { useEffect, useState } from "react";

type Customer = { customer_id: number; customer_name: string };
type Server   = { server_id: number; server_name: string };

const API_BASE =
  (import.meta.env.VITE_API_BASE_URL?.replace(/\/+$/, "") || "http://127.0.0.1:8002");

async function fetchJson<T = any>(url: string): Promise<T> {
  const res = await fetch(url);
  const txt = await res.text();
  let body: any = null;
  try { body = txt ? JSON.parse(txt) : null; } catch { body = { raw: txt }; }
  if (!res.ok) {
    const msg = body?.detail || body?.error || body?.message || body?.raw || `HTTP ${res.status}`;
    throw new Error(typeof msg === "string" ? msg : JSON.stringify(msg));
  }
  return body;
}

function normalizeCustomers(raw: any): Customer[] {
  const arr = Array.isArray(raw) ? raw : raw?.customers ?? [];
  return (arr as any[]).map((c) => ({
    customer_id: c.customer_id ?? c.id ?? c.customerId ?? Number(c?.value),
    customer_name: c.customer_name ?? c.name ?? c.customerName ?? c?.label,
  }));
}

function normalizeServers(raw: any): Server[] {
  const arr = Array.isArray(raw) ? raw : raw?.servers ?? [];
  return (arr as any[]).map((s) => ({
    server_id: s.server_id ?? s.id ?? s.serverId ?? Number(s?.value),
    server_name: s.server_name ?? s.name ?? s.serverName ?? s?.label,
  }));
}

export default function ReportExport() {
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [servers, setServers] = useState<Server[]>([]);
  const [custId, setCustId] = useState<number | "">("");
  const [serverId, setServerId] = useState<number | "">("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try { setCustomers(normalizeCustomers(await fetchJson(`${API_BASE}/customers`))); }
      catch (e: any) { setError(`Failed to load customers: ${e?.message || e}`); }
    })();
  }, []);

  useEffect(() => {
    setServers([]); setServerId("");
    if (custId === "") return;
    (async () => {
      try { setServers(normalizeServers(await fetchJson(`${API_BASE}/customers/${custId}/servers`))); }
      catch (e: any) { setError(`Failed to load servers: ${e?.message || e}`); }
    })();
  }, [custId]);

  const selectedCustomer = customers.find(c => c.customer_id === custId);
  const selectedServer   = servers.find(s => s.server_id === serverId);
  const canDownload = !!selectedCustomer && !!selectedServer && !busy;

  async function handleDownload() {
    if (!selectedCustomer || !selectedServer) return;
    setBusy(true); setError(null); setOk(null);

    const url = `${API_BASE}/export/summary-docx?customer=${encodeURIComponent(selectedCustomer.customer_name)}&server=${encodeURIComponent(selectedServer.server_name)}`;

    try {
      // Preferred path: fetch -> blob -> set filename
      const resp = await fetch(url, { method: "GET" });
      if (!resp.ok) throw new Error(await resp.text());

      const blob = await resp.blob();
      // Try to read filename from Content-Disposition (may be blocked if not exposed)
      let filename = `Replicate_Technical_Review_${selectedCustomer.customer_name}_${selectedServer.server_name}.docx`.replace(/\s+/g, "_");
      const dispo = resp.headers.get("Content-Disposition");
      const m = dispo && dispo.match(/filename="?([^"]+)"?/i);
      if (m && m[1]) filename = m[1];

      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();

      setOk(`Downloaded ${filename}`);
    } catch (e: any) {
      console.error("[ReportExport] fetch download failed, falling back to direct link:", e);
      // Fallback: open direct link (bypasses CORS; lets the browser download natively)
      try {
        window.open(url, "_blank");
        setOk("Opened direct download in a new tab.");
      } catch (e2: any) {
        setError(`Export failed: ${e2?.message || e2}`);
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="rounded-xl border bg-white/70 p-4 shadow-sm">
      <h2 className="text-lg font-semibold mb-3 text-center">Export Technical Review (.docx)</h2>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <div>
          <label className="block text-sm font-medium">Customer</label>
          <select
            className="mt-1 w-full border rounded-lg p-2"
            value={custId}
            onChange={(e) => setCustId(e.target.value ? Number(e.target.value) : "")}
          >
            <option value="">Select customer...</option>
            {customers.map((c) => (
              <option key={c.customer_id} value={c.customer_id}>{c.customer_name}</option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-sm font-medium">Server</label>
          <select
            className="mt-1 w-full border rounded-lg p-2"
            value={serverId}
            disabled={custId === "" || servers.length === 0}
            onChange={(e) => setServerId(e.target.value ? Number(e.target.value) : "")}
          >
            <option value="">{custId === "" ? "Select customer first" : "Select server..."}</option>
            {servers.map((s) => (
              <option key={s.server_id} value={s.server_id}>{s.server_name}</option>
            ))}
          </select>
        </div>

        <div className="flex items-end">
          <button
            onClick={handleDownload}
            disabled={!canDownload}
            className="inline-flex items-center gap-2 rounded-lg bg-indigo-600 text-white px-4 py-2 disabled:opacity-50"
          >
            {busy ? "Generating…" : "Generate Word Doc"}
          </button>
        </div>
      </div>

      {/* Direct link helper (visible when ready) */}
      {canDownload && (
        <div className="mt-2 text-xs text-gray-500">
          Trouble downloading?{" "}
          <a
            href={`${API_BASE}/export/summary-docx?customer=${encodeURIComponent(selectedCustomer!.customer_name)}&server=${encodeURIComponent(selectedServer!.server_name)}`}
            target="_blank" rel="noreferrer" className="underline"
          >
            Click here to open direct link
          </a>
          .
        </div>
      )}

      {error && <p className="text-red-600 mt-2">Export failed: {error}</p>}
      {ok && <p className="text-green-700 mt-2">{ok}</p>}
    </section>
  );
}
