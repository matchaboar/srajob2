import type { CSSProperties } from "react";

interface DiagonalFractionProps {
  numerator: number;
  denominator: number;
  color?: string;
}

// Self-contained styles to avoid inheriting parent CSS.
const baseFont = `"Times New Roman", "Times", serif`;

export function DiagonalFraction({ numerator, denominator, color = "#d9ae52" }: DiagonalFractionProps) {
  const slashStyle: CSSProperties = {
    position: "absolute",
    left: "100%",
    top: "50%",
    bottom: "-50%",
    width: "3px",
    background: color,
    transform: "translate(-40%, 8%) rotate(45deg)",
    borderRadius: "999px",
    opacity: 0.85,
  };

  const containerStyle: CSSProperties = {
    display: "inline-grid",
    gridTemplateColumns: "repeat(2, auto)",
    gridTemplateRows: "repeat(2, auto)",
    position: "relative",
    fontFamily: baseFont,
    color,
    lineHeight: 1.1,
  };

  const numeratorStyle: CSSProperties = {
    position: "relative",
    fontSize: "14px",
    fontWeight: 700,
    paddingRight: "6px",
    paddingBottom: "2px",
  };

  const denominatorStyle: CSSProperties = {
    gridColumn: 2,
    gridRow: 2,
    fontSize: "15px",
    fontWeight: 700,
    paddingLeft: "6px",
    paddingTop: "2px",
  };

  return (
    <span style={containerStyle}>
      <span style={numeratorStyle}>
        {numerator}
        <span aria-hidden="true" style={slashStyle} />
      </span>
      <span style={denominatorStyle}>{denominator}</span>
    </span>
  );
}
