// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useCallback, useEffect, useRef, useState } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { formatCompensationDisplay, parseCompensationInput } from "./compensation";

type Filters = {
  minCompensation: number | null;
};

const MIN_SALARY = 50000;
const MAX_SALARY = 800000;
const DEFAULT_SLIDER_VALUE = 200000;

const clampToSliderRange = (value: number) => Math.min(Math.max(value, MIN_SALARY), MAX_SALARY);

function MinCompensationHarness({ initial }: { initial: number | null }) {
  const [filters, setFilters] = useState<Filters>({ minCompensation: initial });
  const [minCompensationInput, setMinCompensationInput] = useState("");
  const [, setSliderValue] = useState(DEFAULT_SLIDER_VALUE);
  const minCompInputFocusedRef = useRef(false);

  const updateFilters = useCallback((partial: Partial<Filters>) => {
    setFilters((prev) => ({ ...prev, ...partial }));
  }, []);

  useEffect(() => {
    if (!minCompInputFocusedRef.current) {
      setMinCompensationInput(formatCompensationDisplay(filters.minCompensation));
    }
    if (filters.minCompensation === null) {
      setSliderValue(DEFAULT_SLIDER_VALUE);
    } else {
      setSliderValue(clampToSliderRange(filters.minCompensation));
    }
  }, [filters.minCompensation]);

  return (
    <div>
      <label htmlFor="min-salary">Min Salary</label>
      <input
        id="min-salary"
        value={minCompensationInput}
        onChange={(e) => {
          setMinCompensationInput(e.target.value);
        }}
        onFocus={() => {
          minCompInputFocusedRef.current = true;
        }}
        onBlur={() => {
          minCompInputFocusedRef.current = false;
          const parsed = parseCompensationInput(minCompensationInput, { max: MAX_SALARY });
          setMinCompensationInput(parsed === null ? "" : formatCompensationDisplay(parsed));
          setSliderValue(clampToSliderRange(parsed ?? DEFAULT_SLIDER_VALUE));
          updateFilters({ minCompensation: parsed });
        }}
      />
      <output data-testid="min-compensation-value">{filters.minCompensation ?? "null"}</output>
    </div>
  );
}

afterEach(() => cleanup());

describe("min compensation input interactions", () => {
  it("lets a user grow 10k into 100k with backspaces and typing", async () => {
    const user = userEvent.setup();
    render(<MinCompensationHarness initial={10000} />);

    const input = screen.getByLabelText("Min Salary");
    await user.click(input);
    await user.keyboard("{backspace}{backspace}00k");
    await user.tab(); // blur

    expect(input).toHaveValue("$100k");
    expect(screen.getByTestId("min-compensation-value")).toHaveTextContent("100000");
  });

  it("replaces an existing value when the field is cleared and retyped", async () => {
    const user = userEvent.setup();
    render(<MinCompensationHarness initial={100000} />);

    const input = screen.getByLabelText("Min Salary");
    await user.click(input);
    await user.keyboard("{Control>}{A}{/Control}{Delete}30k");
    await user.tab(); // blur

    expect(input).toHaveValue("$30k");
    expect(screen.getByTestId("min-compensation-value")).toHaveTextContent("30000");
  });
});
