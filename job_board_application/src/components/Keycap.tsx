import React from "react";

interface KeycapProps {
    label: string;
    className?: string;
}

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace JSX {
    interface IntrinsicElements {
      "k-cap": React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
      "k-legend": React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    }
  }
}

export const Keycap: React.FC<KeycapProps> = ({ label, className }) => {
    return React.createElement(
      "k-cap",
      { className: `custom-theme ${className || ""}` },
      React.createElement("k-legend", { className: "center medium" }, label),
    );
};
