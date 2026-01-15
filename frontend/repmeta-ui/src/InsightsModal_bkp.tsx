// InsightsModal.tsx (render from stored JSON)
import React from "react";

export function InsightsModal({ open, onClose, data, job, automation, onStart, onRetry }: any) {
  if (!open) return null;
  const insights = data?.result?.json;

  return (
    <div className="fixed inset-0 z-50 bg-black/30 p-6" onClick={onClose}>
      <div className="mx-auto max-w-4xl rounded-2xl bg-white shadow-xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between border-b px-6 py-4">
          <div>
            <div className="text-lg font-semibold">AI Insights</div>
            <div className="text-xs text-slate-500">
              Status: <span className="font-medium">{job?.status ?? "not created"}</span>
              {automation?.execution_id ? (
                <span className="ml-3">
                  n8n: <span className="font-medium">{automation.execution_id}</span> ({automation.status})
                </span>
              ) : null}
            </div>
          </div>

          <div className="flex gap-2">
            {(job?.status === "created" || !job) && (
              <button className="rounded-lg bg-slate-900 px-3 py-2 text-xs font-semibold text-white" onClick={onStart}>
                Start
              </button>
            )}
            {job?.status === "failed" && (
              <button className="rounded-lg bg-rose-600 px-3 py-2 text-xs font-semibold text-white" onClick={onRetry}>
                Retry
              </button>
            )}
            <button className="rounded-lg border px-3 py-2 text-xs font-semibold" onClick={onClose}>
              Close
            </button>
          </div>
        </div>

        <div className="space-y-6 px-6 py-5">
          {!insights ? (
            <div className="text-sm text-slate-600">No insights JSON yet. Once completed, it will appear here.</div>
          ) : (
            <>
              <section>
                <div className="text-sm font-semibold">Summary</div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
                  {insights.summary?.map((s: string, i: number) => <li key={i}>{s}</li>)}
                </ul>
              </section>

              <section>
                <div className="text-sm font-semibold">Findings</div>
                <div className="mt-2 grid gap-3 md:grid-cols-2">
                  {insights.findings?.map((f: any, i: number) => (
                    <div key={i} className="rounded-xl border p-4">
                      <div className="flex items-start justify-between gap-2">
                        <div className="font-semibold text-slate-900">{f.title}</div>
                        <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs">sev {f.severity}</span>
                      </div>
                      <div className="mt-2 text-xs text-slate-500">What happened</div>
                      <div className="text-sm text-slate-700">{f.what_happened}</div>
                      <div className="mt-2 text-xs text-slate-500">Why it matters</div>
                      <div className="text-sm text-slate-700">{f.why_it_matters}</div>
                      <div className="mt-3 text-xs text-slate-500">Next steps</div>
                      <ul className="mt-1 list-disc pl-5 text-sm text-slate-700">
                        {(f.next_steps ?? []).map((x: string, k: number) => <li key={k}>{x}</li>)}
                      </ul>
                    </div>
                  ))}
                </div>
              </section>

              <section>
                <div className="text-sm font-semibold">Confidence</div>
                <div className="mt-2 flex items-center gap-3">
                  <div className="h-2 w-full rounded-full bg-slate-100">
                    <div className="h-2 rounded-full bg-slate-900" style={{ width: `${Math.round((insights.confidence ?? 0) * 100)}%` }} />
                  </div>
                  <div className="w-12 text-right text-sm">{Math.round((insights.confidence ?? 0) * 100)}%</div>
                </div>
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
