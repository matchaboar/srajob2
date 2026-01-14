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
  strong: ({ node, children, ...props }) => (
    <strong {...props} className="font-semibold text-slate-200">
      {children}
    </strong>
  ),
};

interface MetadataSectionProps {
  metadata: string;
}

export function MetadataSection({ metadata }: MetadataSectionProps) {
  if (!metadata) return null;

  return (
    <div className="rounded-lg border border-slate-800/70 bg-slate-900/35 p-2">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500 mb-2">
        Metadata
      </div>
      <div className="text-sm leading-relaxed text-slate-300 font-sans max-h-56 overflow-y-auto pr-1 space-y-3">
        <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
          {metadata}
        </ReactMarkdown>
      </div>
    </div>
  );
}
