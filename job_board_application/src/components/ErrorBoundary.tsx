import React from "react";

type ErrorBoundaryProps = {
  children: React.ReactNode;
  onRetry?: () => void;
  onError?: (error: Error) => void;
  title?: string;
  message?: string;
};

type ErrorBoundaryState = {
  error: Error | null;
};

export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error("UI error boundary caught an error", error, info);
    this.props.onError?.(error);
  }

  private handleRetry = () => {
    this.setState({ error: null });
    this.props.onRetry?.();
  };

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;

    const title = this.props.title ?? "Something went wrong";
    const message =
      this.props.message ??
      "We hit a loading error while fetching data. Please try again in a moment.";

    return (
      <div className="flex flex-1 items-center justify-center p-6">
        <div className="max-w-lg w-full rounded-lg border border-slate-800 bg-slate-900 p-6 text-center">
          <h2 className="text-lg font-semibold text-white">{title}</h2>
          <p className="mt-2 text-sm text-slate-300">{message}</p>
          <div className="mt-4 flex flex-wrap justify-center gap-3">
            <button
              onClick={this.handleRetry}
              className="rounded-md bg-emerald-500 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-400"
            >
              Try again
            </button>
            <button
              onClick={() => window.location.reload()}
              className="rounded-md border border-slate-700 px-4 py-2 text-sm text-slate-200 hover:border-slate-500"
            >
              Reload page
            </button>
          </div>
          <details className="mt-4 text-left text-xs text-slate-400">
            <summary className="cursor-pointer text-slate-500">Error details</summary>
            <pre className="mt-2 whitespace-pre-wrap">{error.message}</pre>
          </details>
        </div>
      </div>
    );
  }
}
