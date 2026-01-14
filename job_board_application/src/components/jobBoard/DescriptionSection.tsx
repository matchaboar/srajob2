import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import remarkBreaks from "remark-breaks";

const markdownComponents: Components = {
  a: ({ node, children, ...props }) => (
    <a {...props} target="_blank" rel="noreferrer" className="text-blue-300 hover:text-blue-200 underline">
      {children}
    </a>
  ),
  ul: ({ node, children, ...props }) => (
    <ul {...props} className="list-disc list-inside space-y-1">
      {children}
    </ul>
  ),
  ol: ({ node, children, ...props }) => (
    <ol {...props} className="list-decimal list-inside space-y-1">
      {children}
    </ol>
  ),
  li: ({ node, children, ...props }) => (
    <li {...props} className="text-slate-300">
      {children}
    </li>
  ),
  p: ({ node, children, ...props }) => (
    <p {...props} className="text-slate-300">
      {children}
    </p>
  ),
  h1: ({ node, children, ...props }) => (
    <h1 {...props} className="text-lg font-bold text-white mt-4 mb-2">
      {children}
    </h1>
  ),
  h2: ({ node, children, ...props }) => (
    <h2 {...props} className="text-base font-bold text-white mt-3 mb-2">
      {children}
    </h2>
  ),
  h3: ({ node, children, ...props }) => (
    <h3 {...props} className="text-sm font-bold text-white mt-2 mb-1">
      {children}
    </h3>
  ),
  strong: ({ node, children, ...props }) => (
    <strong {...props} className="font-semibold text-slate-200">
      {children}
    </strong>
  ),
};

export interface DescriptionState {
  hasFull: boolean;
  loading: boolean;
  loaded: boolean;
  error: string | null;
}

interface DescriptionSectionProps {
  description: string;
  wordCount?: number | null;
  descriptionState?: DescriptionState;
  onReadMore?: () => void;
  onCopyLink?: () => void;
  jobId: string;
  maxHeight?: string;
}

export function DescriptionSection({
  description,
  wordCount,
  descriptionState,
  onReadMore,
  onCopyLink,
  maxHeight = "max-h-72",
}: DescriptionSectionProps) {
  return (
    <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Description</h3>
        <div className="flex items-center gap-2 text-[11px] text-slate-500">
          {wordCount !== null && wordCount !== undefined && <span>{`${wordCount} words`}</span>}
          {descriptionState?.hasFull && !descriptionState.loaded && onReadMore && (
            <button
              type="button"
              onClick={onReadMore}
              disabled={descriptionState.loading}
              className="px-2 py-1 rounded border border-slate-700 text-slate-300 hover:text-white hover:border-slate-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {descriptionState.loading ? "Loading..." : "Read more"}
            </button>
          )}
          {descriptionState?.loaded && <span className="text-emerald-300">Full</span>}
          {onCopyLink && (
            <button
              type="button"
              onClick={onCopyLink}
              className="inline-flex h-6 w-6 items-center justify-center rounded border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 hover:bg-slate-800 transition-colors"
              aria-label="Copy job link"
              title="Copy job link"
            >
              <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M10 13a5 5 0 0 1 0-7l1.5-1.5a5 5 0 0 1 7 7L17 12"
                />
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M14 11a5 5 0 0 1 0 7l-1.5 1.5a5 5 0 0 1-7-7L7 12"
                />
              </svg>
            </button>
          )}
        </div>
      </div>
      {descriptionState?.error && (
        <div className="text-[11px] text-amber-300 mb-2">{descriptionState.error}</div>
      )}
      <div
        className={`text-sm leading-relaxed text-slate-300 font-sans ${maxHeight} overflow-y-auto pr-1 space-y-3`}
      >
        <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
          {description}
        </ReactMarkdown>
      </div>
    </div>
  );
}
