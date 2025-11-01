import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App';

// Give the app a clear title in the browser/tab
document.title = 'RepMeta Console';

// Light sanity check for SSE support (used by Repository JSON/ZIP progress)
if (!('EventSource' in window)) {
  // If this ever fires on a target browser, the UI will still upload,
  // but progress streaming won't work. We simply log a warning.
  // (Optional: you could render a banner in App if needed.)
  // eslint-disable-next-line no-console
  console.warn(
    '[RepMeta] EventSource (SSE) is not supported by this browser. Progress streaming will be disabled.'
  );
}

// Bootstrap React
const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('Root element #root not found');
}

createRoot(rootEl).render(
  <StrictMode>
    <App />
  </StrictMode>
);

// Optional: gentle guardrails for unexpected runtime errors.
// (Keeps console noise understandable during local dev.)
window.addEventListener('error', (e) => {
  // eslint-disable-next-line no-console
  console.error('[RepMeta] Uncaught error:', e.error || e.message || e);
});
window.addEventListener('unhandledrejection', (e) => {
  // eslint-disable-next-line no-console
  console.error('[RepMeta] Unhandled promise rejection:', e.reason || e);
});
