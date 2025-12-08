import React, { useCallback, useEffect, useMemo, useState } from "react";

interface Account {
  account_id: string;
  tenant_id: string;
  account_name: string;
}

interface RunHistoryEntry {
  run_id: number;
  created_at: string;
  finished_at: string | null;
  account_id: string;
  tenant_id: string;
  account_name: string;
  artifact_name: string;
  status: string;
  exit_code: number | null;
}

interface RunDetail extends RunHistoryEntry {
  raw_stdout?: string | null;
  raw_stderr?: string | null;
}

type PipelineStepId = 1 | 2 | 3 | 4;
type RunStatus = "idle" | "running" | "success" | "error";
type HistoryFilter = "all" | "success" | "failed";

const API_BASE = "http://127.0.0.1:8002";

const TalendTab: React.FC = () => {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [selectedAccount, setSelectedAccount] = useState<Account | null>(null);

  const [cseatFiles, setCseatFiles] = useState<File[]>([]);
  const [qtcmtFile, setQtcmtFile] = useState<File | null>(null);

  const [isLoadingAccounts, setIsLoadingAccounts] = useState(false);

  const [runStatus, setRunStatus] = useState<RunStatus>("idle");
  const isRunning = runStatus === "running";

  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [historyFilter, setHistoryFilter] = useState<HistoryFilter>("all");

  const [isDetailOpen, setIsDetailOpen] = useState(false);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [runDetail, setRunDetail] = useState<RunDetail | null>(null);

  // ------------------------------------------------------------
  // Fetch accounts on mount
  // ------------------------------------------------------------
  useEffect(() => {
    const fetchAccounts = async () => {
      setIsLoadingAccounts(true);
      setError(null);
      try {
        const res = await fetch(`${API_BASE}/talend/accounts`);
        if (!res.ok) {
          const text = await res.text();
          throw new Error(text || `Failed to load accounts (${res.status})`);
        }
        const data = (await res.json()) as Account[];
        setAccounts(data || []);
        if (data && data.length > 0) {
          setSelectedAccount((prev) => prev ?? data[0]);
        }
      } catch (e: any) {
        console.error("Failed to load accounts", e);
        setError(
          "Failed to load accounts. Please verify the qtcmeta.ACCOUNT table."
        );
      } finally {
        setIsLoadingAccounts(false);
      }
    };

    fetchAccounts();
  }, []);

  // ------------------------------------------------------------
  // Load persisted run history for the selected account
  // ------------------------------------------------------------
  const loadRunHistory = useCallback(async (accountId: string) => {
    try {
      const res = await fetch(`${API_BASE}/talend/runs/${accountId}`);
      if (!res.ok) {
        console.warn("Failed to load Talend run history, status=", res.status);
        return;
      }
      const data = (await res.json()) as RunHistoryEntry[];
      setRunHistory(data || []);
    } catch (err) {
      console.warn("Failed to load Talend run history", err);
    }
  }, []);

  useEffect(() => {
    if (!selectedAccount) {
      setRunHistory([]);
      return;
    }
    void loadRunHistory(selectedAccount.account_id);
  }, [selectedAccount, loadRunHistory]);

  // ------------------------------------------------------------
  // Derived state for pipeline + analytics
  // ------------------------------------------------------------
  const currentStep: PipelineStepId = useMemo(() => {
    if (!selectedAccount) return 1;
    if (!cseatFiles.length && !qtcmtFile) return 2;
    if (!runHistory.length) return 3;
    return 4;
  }, [selectedAccount, cseatFiles.length, qtcmtFile, runHistory.length]);

  const totalArtifacts = useMemo(
    () => cseatFiles.length + (qtcmtFile ? 1 : 0),
    [cseatFiles.length, qtcmtFile]
  );

  const lastRun = runHistory[0] ?? null;

  const successCount = useMemo(
    () =>
      runHistory.filter(
        (r) => r.status.toLowerCase() === "success" && r.exit_code === 0
      ).length,
    [runHistory]
  );
  const failedCount = useMemo(
    () =>
      runHistory.filter(
        (r) => r.status.toLowerCase() !== "success" || r.exit_code !== 0
      ).length,
    [runHistory]
  );

  const filteredRuns = useMemo(() => {
    if (historyFilter === "all") return runHistory;
    if (historyFilter === "success") {
      return runHistory.filter(
        (r) => r.status.toLowerCase() === "success" && r.exit_code === 0
      );
    }
    // failed
    return runHistory.filter(
      (r) => r.status.toLowerCase() !== "success" || r.exit_code !== 0
    );
  }, [runHistory, historyFilter]);

  const lastRunDuration = useMemo(() => {
    if (!lastRun || !lastRun.finished_at) return null;
    const start = new Date(lastRun.created_at).getTime();
    const end = new Date(lastRun.finished_at).getTime();
    if (Number.isNaN(start) || Number.isNaN(end)) return null;
    const seconds = Math.max(0, Math.round((end - start) / 1000));
    if (seconds < 60) return `${seconds}s`;
    const mins = Math.floor(seconds / 60);
    const rem = seconds % 60;
    return `${mins}m ${rem}s`;
  }, [lastRun]);

  // ------------------------------------------------------------
  // Handlers
  // ------------------------------------------------------------

  const handleAccountChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const acc = accounts.find((a) => a.account_id === e.target.value) || null;
    setSelectedAccount(acc);
    setStatusMessage(null);
  };

  const handleCseatChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files) {
      setCseatFiles(Array.from(e.target.files));
    }
  };

  const handleQtcmtChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setQtcmtFile(e.target.files[0]);
    } else {
      setQtcmtFile(null);
    }
  };

  const handleClearArtifacts = () => {
    setCseatFiles([]);
    setQtcmtFile(null);
    setStatusMessage(null);
  };

  const closeDetail = () => {
    setIsDetailOpen(false);
    setRunDetail(null);
    setDetailError(null);
    setIsLoadingDetail(false);
  };

  const openRunDetail = async (runId: number) => {
    setIsDetailOpen(true);
    setIsLoadingDetail(true);
    setDetailError(null);
    setRunDetail(null);

    try {
      const res = await fetch(`${API_BASE}/talend/run/${runId}`);
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Failed to load run detail (${res.status})`);
      }
      const data = (await res.json()) as RunDetail;
      setRunDetail(data);
    } catch (err: any) {
      console.error("Failed to load Talend run detail", err);
      setDetailError(err?.message || "Failed to load Talend run detail.");
    } finally {
      setIsLoadingDetail(false);
    }
  };

  // ------------------------------------------------------------
  // Run Talend Job
  // ------------------------------------------------------------

  const runTalend = async () => {
    if (!selectedAccount) {
      setError("Please select an Account before running.");
      return;
    }

    if (!cseatFiles.length && !qtcmtFile) {
      setError("Please attach at least one CSEAT CSV or a QTCMT H2 file.");
      return;
    }

    setError(null);
    setStatusMessage(null);
    setRunStatus("running");

    try {
      const formData = new FormData();
      formData.append("account_id", selectedAccount.account_id);
      formData.append("tenant_id", selectedAccount.tenant_id);

      cseatFiles.forEach((file) => {
        formData.append("cseat_files", file);
      });

      if (qtcmtFile) {
        formData.append("qtcmt_file", qtcmtFile);
      }

      const res = await fetch(`${API_BASE}/talend/run`, {
        method: "POST",
        body: formData,
      });

      let data: any = null;
      try {
        data = await res.json();
      } catch {
        data = null;
      }

      if (!res.ok) {
        const msg =
          (data && data.detail) ||
          `Failed to execute Talend job (${res.status})`;
        throw new Error(msg);
      }

      const logicalStatus: "success" | "error" =
        data && data.status === "success" && data.exit_code === 0
          ? "success"
          : "error";

      if (logicalStatus === "success") {
        setRunStatus("success");
        setStatusMessage(
          `Talend job completed successfully (exit ${data.exit_code}).`
        );
      } else {
        setRunStatus("error");
        setError(
          data?.stderr ||
            "Talend job reported an error. See Talend logs for details."
        );
      }

      if (selectedAccount) {
        void loadRunHistory(selectedAccount.account_id);
      }
    } catch (e: any) {
      console.error(e);
      setRunStatus("error");
      setError(e?.message || "Failed to execute Talend job.");
    }
  };

  // ------------------------------------------------------------
  // Small helper renderers
  // ------------------------------------------------------------

  const renderPipelineStep = (
    id: PipelineStepId,
    label: string,
    description: string
  ) => {
    const active = id === currentStep;
    const completed = id < currentStep;

    const circleClasses = [
      "flex h-7 w-7 items-center justify-center rounded-full border text-xs font-semibold",
      active
        ? "border-green-600 bg-green-50 text-green-700"
        : completed
        ? "border-green-600 bg-green-600 text-white"
        : "border-gray-300 bg-white text-gray-500",
    ].join(" ");

    const textClasses = [
      "flex flex-col",
      active ? "text-gray-900" : "text-gray-600",
    ].join(" ");

    return (
      <div
        key={id}
        className={`relative flex items-start gap-3 rounded-xl border px-3 py-2.5 transition ${
          active
            ? "border-green-500 bg-green-50 shadow-sm"
            : "border-gray-200 bg-white"
        }`}
      >
        {/* vertical connector */}
        {id < 4 && (
          <div className="pointer-events-none absolute left-[13px] top-7 h-6 w-px bg-gradient-to-b from-gray-200 to-gray-100" />
        )}
        <div className={circleClasses}>{id}</div>
        <div className={textClasses}>
          <span className="text-sm font-semibold">{label}</span>
          <span className="text-xs text-gray-500">{description}</span>
        </div>
      </div>
    );
  };

  const formatDateTime = (iso: string | null) => {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleString();
  };

  // ------------------------------------------------------------
  // Render
  // ------------------------------------------------------------

  return (
    <div className="relative flex h-full flex-col gap-4 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-green-600 text-white shadow-sm">
            {/* simple Talend-ish glyph */}
            <span className="text-xl font-semibold">T</span>
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-2xl font-semibold text-gray-900">
                Talend Orchestration
              </h2>
              <span className="rounded-full bg-green-50 px-2 py-0.5 text-[11px] font-medium text-green-700 ring-1 ring-green-100">
                Talend pipeline (beta)
              </span>
            </div>
            <p className="text-sm text-gray-500">
              Select an account, upload artifacts, and orchestrate a Talend job
              run directly from RepMeta.
            </p>
          </div>
        </div>

        {lastRun && (
          <div
            className={`rounded-lg border px-4 py-2 text-xs shadow-sm ${
              lastRun.status.toLowerCase() === "success" &&
              lastRun.exit_code === 0
                ? "border-green-200 bg-green-50 text-green-700"
                : "border-red-200 bg-red-50 text-red-700"
            }`}
          >
            <div className="flex items-center gap-2">
              <span className="rounded-full bg-white/70 px-2 py-0.5 text-[10px] font-semibold">
                Last run
              </span>
              <span className="font-semibold">
                {lastRun.status.toUpperCase()} (exit {lastRun.exit_code ?? ""})
              </span>
            </div>
            <div className="mt-0.5 flex items-center gap-3 text-[11px] text-gray-600">
              <span>{formatDateTime(lastRun.created_at)}</span>
              <span>•</span>
              <span>{lastRun.account_name}</span>
              {lastRunDuration && (
                <>
                  <span>•</span>
                  <span>Duration: {lastRunDuration}</span>
                </>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Alerts */}
      {error && (
        <div className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {!error && statusMessage && (
        <div className="rounded-md border border-green-200 bg-green-50 px-4 py-3 text-sm text-green-700">
          {statusMessage}
        </div>
      )}

      <div className="grid gap-6 lg:grid-cols-[260px,1fr]">
        {/* Left column: pipeline + analytics */}
        <div className="space-y-6">
          <section>
            <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-500">
              Talend Pipeline
            </h3>
            <div className="space-y-2">
              {renderPipelineStep(
                1,
                "Select Account",
                "Choose the Talend account from qtcmeta.ACCOUNT."
              )}
              {renderPipelineStep(
                2,
                "Attach Artifacts",
                "Upload CSEAT CSV exports and the QTCMT H2 database."
              )}
              {renderPipelineStep(
                3,
                "Run Talend Job",
                "RepMeta triggers run_artifact.py with the selected account."
              )}
              {renderPipelineStep(
                4,
                "Review Run History",
                "Inspect recent Talend runs initiated from this console."
              )}
            </div>
          </section>

          <section>
            <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-500">
              Analytics
            </h3>
            <div className="space-y-3 rounded-xl bg-white p-4 shadow-sm ring-1 ring-gray-100">
              <div className="mb-3 flex items-center justify-between text-xs">
                <span className="text-gray-500">CURRENT CONTEXT</span>
                {selectedAccount && (
                  <span className="rounded-full bg-gray-100 px-2 py-0.5 text-[10px] text-gray-600">
                    Per account / tenant
                  </span>
                )}
              </div>
              <dl className="grid grid-cols-3 gap-3 text-xs">
                <div className="rounded-lg bg-green-50 px-3 py-2">
                  <dt className="text-[11px] font-semibold text-green-700">
                    Account
                  </dt>
                  <dd className="mt-1 text-[11px] text-gray-800">
                    {selectedAccount?.account_name ?? "Not selected"}
                  </dd>
                </div>
                <div className="rounded-lg bg-blue-50 px-3 py-2">
                  <dt className="text-[11px] font-semibold text-blue-700">
                    Artifacts
                  </dt>
                  <dd className="mt-1 text-[11px] text-gray-800">
                    {totalArtifacts} selected
                  </dd>
                </div>
                <div className="rounded-lg bg-purple-50 px-3 py-2">
                  <dt className="text-[11px] font-semibold text-purple-700">
                    Runs (recent)
                  </dt>
                  <dd className="mt-1 text-[11px] text-gray-800">
                    {runHistory.length}
                  </dd>
                </div>
              </dl>
            </div>
          </section>
        </div>

        {/* Right column: main content */}
        <div className="space-y-6">
          {/* Account selection */}
          <section className="rounded-xl bg-white p-5 shadow-sm ring-1 ring-gray-100">
            <div className="mb-3 flex items-start justify-between gap-4">
              <div>
                <h3 className="text-sm font-semibold text-gray-900">
                  Talend Account &amp; Tenant
                </h3>
                <p className="text-xs text-gray-500">
                  Accounts and tenant IDs are managed in the{" "}
                  <code className="rounded bg-gray-100 px-1 py-0.5 text-[11px]">
                    qtcmeta.ACCOUNT
                  </code>{" "}
                  table. Pick the account you want to orchestrate.
                </p>
              </div>
              {isLoadingAccounts && (
                <span className="text-[11px] text-gray-500">Loading…</span>
              )}
            </div>

            <div className="space-y-3">
              <div className="space-y-1.5">
                <label className="block text-xs font-medium text-gray-700">
                  Select Account
                </label>
                <select
                  className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-green-500 focus:ring-1 focus:ring-green-500"
                  value={selectedAccount?.account_id ?? ""}
                  onChange={handleAccountChange}
                  disabled={isLoadingAccounts || !accounts.length || isRunning}
                >
                  {!accounts.length && (
                    <option value="">
                      No accounts found (check qtcmeta.ACCOUNT)
                    </option>
                  )}
                  {accounts.length > 0 && (
                    <option value="">-- Choose an account --</option>
                  )}
                  {accounts.map((acc) => (
                    <option key={acc.account_id} value={acc.account_id}>
                      {acc.account_name}
                    </option>
                  ))}
                </select>
              </div>

              {selectedAccount && (
                <div className="grid gap-3 text-xs text-gray-600 sm:grid-cols-3">
                  <div className="rounded-lg bg-gray-50 px-3 py-2">
                    <div className="font-semibold text-gray-700">
                      Account ID
                    </div>
                    <div className="mt-0.5 break-all">
                      {selectedAccount.account_id}
                    </div>
                  </div>
                  <div className="rounded-lg bg-gray-50 px-3 py-2">
                    <div className="font-semibold text-gray-700">
                      Tenant ID
                    </div>
                    <div className="mt-0.5 break-all">
                      {selectedAccount.tenant_id}
                    </div>
                  </div>
                  <div className="rounded-lg bg-gray-50 px-3 py-2">
                    <div className="font-semibold text-gray-700">
                      Connected Tenants
                    </div>
                    <div className="mt-0.5 text-[11px] text-gray-500">
                      Using tenant from qtcmeta.ACCOUNT. Future versions can
                      surface Talend Cloud metadata here.
                    </div>
                  </div>
                </div>
              )}
            </div>
          </section>

          {/* Artifacts & Run */}
          <section className="rounded-xl bg-white p-5 shadow-sm ring-1 ring-gray-100">
            <h3 className="text-sm font-semibold text-gray-900">
              Talend Artifacts &amp; Job
            </h3>
            <p className="mb-4 text-xs text-gray-500">
              CSEAT exports and the QTCMT database will be staged to the Talend
              job locations (for local dev:{" "}
              <code className="rounded bg-gray-100 px-1 py-0.5 text-[11px]">
                C:\qtcmt
              </code>{" "}
              and{" "}
              <code className="rounded bg-gray-100 px-1 py-0.5 text-[11px]">
                C:\tmp\CS_AUTO
              </code>
              ), before calling{" "}
              <code className="rounded bg-gray-100 px-1 py-0.5 text-[11px]">
                run_artifact.py
              </code>
              .
            </p>

            <div className="grid gap-4 md:grid-cols-2">
              {/* CSEAT */}
              <div className="rounded-lg border border-dashed border-gray-300 bg-gray-50 p-4">
                <div className="mb-2 flex items-center justify-between text-xs">
                  <span className="font-semibold text-gray-700">
                    CSEAT CSV file(s)
                  </span>
                  {cseatFiles.length > 0 && (
                    <span className="rounded-full bg-green-100 px-2 py-0.5 text-[10px] font-medium text-green-700">
                      {cseatFiles.length} selected
                    </span>
                  )}
                </div>
                <p className="mb-2 text-[11px] text-gray-500">
                  One or more CSEAT exports. Multiple CSVs can be uploaded for a
                  single run.
                </p>
                <input
                  type="file"
                  accept=".csv"
                  multiple
                  onChange={handleCseatChange}
                  disabled={isRunning}
                  className="block w-full cursor-pointer text-xs text-gray-700 file:mr-3 file:rounded-md file:border-0 file:bg-green-600 file:px-3 file:py-1.5 file:text-xs file:font-medium file:text-white hover:file:bg-green-700 disabled:cursor-not-allowed disabled:opacity-60"
                />
                {cseatFiles.length > 0 && (
                  <ul className="mt-2 space-y-1 text-[11px] text-gray-600">
                    {cseatFiles.map((f) => (
                      <li key={f.name} className="truncate">
                        • {f.name}
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              {/* QTCMT */}
              <div className="rounded-lg border border-dashed border-gray-300 bg-gray-50 p-4">
                <div className="mb-2 flex items-center justify-between text-xs">
                  <span className="font-semibold text-gray-700">
                    QTCMT H2 database
                  </span>
                  {qtcmtFile && (
                    <span className="rounded-full bg-indigo-100 px-2 py-0.5 text-[10px] font-medium text-indigo-700">
                      1 file selected
                    </span>
                  )}
                </div>
                <p className="mb-2 text-[11px] text-gray-500">
                  The{" "}
                  <code className="bg-gray-100 px-1 py-0.5 rounded">
                    qtcmt.mv.db
                  </code>{" "}
                  file (or archive) for this tenant. Only one file per run.
                </p>
                <input
                  type="file"
                  accept=".h2,.zip,.db"
                  onChange={handleQtcmtChange}
                  disabled={isRunning}
                  className="block w-full cursor-pointer text-xs text-gray-700 file:mr-3 file:rounded-md file:border-0 file:bg-indigo-600 file:px-3 file:py-1.5 file:text-xs file:font-medium file:text-white hover:file:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-60"
                />
                {qtcmtFile && (
                  <div className="mt-2 truncate text-[11px] text-gray-600">
                    Selected: {qtcmtFile.name}
                  </div>
                )}
              </div>
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={runTalend}
                disabled={
                  !selectedAccount ||
                  isRunning ||
                  (!cseatFiles.length && !qtcmtFile)
                }
                className={`inline-flex items-center rounded-md px-4 py-2 text-sm font-medium text-white shadow-sm ${
                  !selectedAccount ||
                  isRunning ||
                  (!cseatFiles.length && !qtcmtFile)
                    ? "bg-gray-400 cursor-not-allowed"
                    : "bg-green-600 hover:bg-green-700"
                }`}
              >
                {isRunning ? "Running…" : "Run Talend Job"}
              </button>

              <button
                type="button"
                onClick={handleClearArtifacts}
                disabled={isRunning || (!cseatFiles.length && !qtcmtFile)}
                className="inline-flex items-center rounded-md border border-gray-300 bg-white px-3 py-1.5 text-xs font-medium text-gray-700 shadow-sm hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Clear selection
              </button>

              <div className="text-[11px] text-gray-500">
                Runs are executed per account/tenant. Artifacts are not
                persisted between sessions.
              </div>
            </div>
          </section>

          {/* Run history */}
          <section className="rounded-xl bg-white p-5 shadow-sm ring-1 ring-gray-100">
            <div className="mb-3 flex items-center justify-between">
              <div>
                <h3 className="text-sm font-semibold text-gray-900">
                  Talend Run History
                </h3>
                <p className="text-xs text-gray-500">
                  Persisted in{" "}
                  <code className="bg-gray-100 px-1 py-0.5 rounded text-[11px]">
                    qtcmeta.talend_run
                  </code>{" "}
                  for the selected account.
                </p>
              </div>
              <div className="flex items-center gap-2 rounded-full bg-gray-50 p-1 text-[11px]">
                <button
                  type="button"
                  onClick={() => setHistoryFilter("all")}
                  className={`rounded-full px-2 py-0.5 ${
                    historyFilter === "all"
                      ? "bg-white text-gray-900 shadow-sm"
                      : "text-gray-600"
                  }`}
                >
                  All ({runHistory.length})
                </button>
                <button
                  type="button"
                  onClick={() => setHistoryFilter("success")}
                  className={`rounded-full px-2 py-0.5 ${
                    historyFilter === "success"
                      ? "bg-green-600 text-white shadow-sm"
                      : "text-green-600"
                  }`}
                >
                  Success ({successCount})
                </button>
                <button
                  type="button"
                  onClick={() => setHistoryFilter("failed")}
                  className={`rounded-full px-2 py-0.5 ${
                    historyFilter === "failed"
                      ? "bg-red-600 text-white shadow-sm"
                      : "text-red-600"
                  }`}
                >
                  Failed ({failedCount})
                </button>
              </div>
            </div>

            {runHistory.length === 0 ? (
              <p className="text-xs text-gray-500">
                Once jobs are executed, you&apos;ll see the last 20 runs for the
                selected account here.
              </p>
            ) : (
              <>
                {/* quick summary strip */}
                <div className="mb-3 flex flex-wrap items-center gap-3 text-[11px] text-gray-600">
                  <span className="rounded-full bg-gray-50 px-2 py-0.5">
                    Total runs: {runHistory.length}
                  </span>
                  <span className="rounded-full bg-green-50 px-2 py-0.5 text-green-700">
                    Success: {successCount}
                  </span>
                  <span className="rounded-full bg-red-50 px-2 py-0.5 text-red-700">
                    Failed: {failedCount}
                  </span>
                  {historyFilter !== "all" && (
                    <span className="rounded-full bg-blue-50 px-2 py-0.5 text-blue-700">
                      Filter: {historyFilter}
                    </span>
                  )}
                </div>

                <div className="overflow-x-auto">
                  <table className="min-w-full border-separate border-spacing-y-1 text-xs">
                    <thead>
                      <tr className="text-left text-[11px] text-gray-500">
                        <th className="px-2 py-1">Started</th>
                        <th className="px-2 py-1">Account</th>
                        <th className="px-2 py-1">Tenant</th>
                        <th className="px-2 py-1">Status</th>
                        <th className="px-2 py-1 text-right">Exit</th>
                        <th className="px-2 py-1 text-right">Details</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRuns.map((r) => (
                        <tr
                          key={r.run_id}
                          className="rounded-lg bg-gray-50 text-gray-800"
                        >
                          <td className="px-2 py-1 align-top">
                            {formatDateTime(r.created_at)}
                          </td>
                          <td className="px-2 py-1 align-top">
                            <div className="font-medium">{r.account_name}</div>
                            <div className="text-[10px] text-gray-500">
                              {r.account_id}
                            </div>
                          </td>
                          <td className="px-2 py-1 align-top text-[11px]">
                            {r.tenant_id}
                          </td>
                          <td className="px-2 py-1 align-top">
                            <span
                              className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium ${
                                r.status.toLowerCase() === "success" &&
                                r.exit_code === 0
                                  ? "bg-green-100 text-green-700"
                                  : "bg-red-100 text-red-700"
                              }`}
                            >
                              {r.status.toUpperCase()}
                            </span>
                          </td>
                          <td className="px-2 py-1 align-top text-right font-mono text-[11px]">
                            {r.exit_code ?? ""}
                          </td>
                          <td className="px-2 py-1 align-top text-right">
                            <button
                              type="button"
                              onClick={() => openRunDetail(r.run_id)}
                              className="inline-flex items-center rounded-md border border-gray-300 bg-white px-2 py-1 text-[11px] font-medium text-gray-700 shadow-sm hover:bg-gray-50"
                            >
                              View
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </section>
        </div>
      </div>

      {/* Detail drawer */}
      {isDetailOpen && (
        <div className="fixed inset-0 z-40 flex justify-end bg-black/30">
          <div className="flex h-full w-full max-w-xl flex-col bg-white shadow-xl">
            <div className="flex items-center justify-between border-b px-4 py-3">
              <div>
                <h3 className="text-sm font-semibold text-gray-900">
                  Run details
                </h3>
                {runDetail && (
                  <p className="text-xs text-gray-500">
                    Run ID #{runDetail.run_id} — {runDetail.account_name}
                  </p>
                )}
              </div>
              <button
                type="button"
                onClick={closeDetail}
                className="rounded-md border border-gray-300 bg-white px-2 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50"
              >
                Close
              </button>
            </div>

            <div className="flex-1 overflow-auto px-4 py-3 text-xs text-gray-800">
              {isLoadingDetail && (
                <p className="text-xs text-gray-500">Loading run details…</p>
              )}

              {detailError && (
                <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                  {detailError}
                </div>
              )}

              {runDetail && !isLoadingDetail && !detailError && (
                <>
                  <div className="mb-4 grid grid-cols-2 gap-3">
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Account
                      </div>
                      <div className="mt-0.5 text-[11px] text-gray-900">
                        {runDetail.account_name}
                      </div>
                      <div className="text-[10px] text-gray-500">
                        {runDetail.account_id}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Tenant
                      </div>
                      <div className="mt-0.5 text-[11px] text-gray-900">
                        {runDetail.tenant_id}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Artifact
                      </div>
                      <div className="mt-0.5 text-[11px] text-gray-900">
                        {runDetail.artifact_name}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Status
                      </div>
                      <div className="mt-0.5">
                        <span
                          className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium ${
                            runDetail.status.toLowerCase() === "success" &&
                            runDetail.exit_code === 0
                              ? "bg-green-100 text-green-700"
                              : "bg-red-100 text-red-700"
                          }`}
                        >
                          {runDetail.status.toUpperCase()}
                        </span>
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Exit code
                      </div>
                      <div className="mt-0.5 font-mono text-[11px] text-gray-900">
                        {runDetail.exit_code ?? ""}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Started
                      </div>
                      <div className="mt-0.5 text-[11px] text-gray-900">
                        {formatDateTime(runDetail.created_at)}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] font-semibold text-gray-600">
                        Finished
                      </div>
                      <div className="mt-0.5 text-[11px] text-gray-900">
                        {runDetail.finished_at
                          ? formatDateTime(runDetail.finished_at)
                          : "—"}
                      </div>
                    </div>
                  </div>

                  <div className="mb-4">
                    <div className="mb-1 text-[11px] font-semibold text-gray-700">
                      Stdout
                    </div>
                    <pre className="max-h-56 overflow-auto rounded-md bg-gray-900 p-2 text-[11px] text-gray-100">
                      {runDetail.raw_stdout || "<empty>"}
                    </pre>
                  </div>

                  <div>
                    <div className="mb-1 text-[11px] font-semibold text-gray-700">
                      Stderr
                    </div>
                    <pre className="max-h-56 overflow-auto rounded-md bg-gray-900 p-2 text-[11px] text-red-100">
                      {runDetail.raw_stderr || "<empty>"}
                    </pre>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default TalendTab;
