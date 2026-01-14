import type { CSSProperties, RefObject } from "react";

type Level = "junior" | "mid" | "senior" | "staff";
type TargetState = "Washington" | "New York" | "California" | "Arizona";
type SavedFilterId = string;

interface Filters {
  search: string;
  includeRemote: boolean;
  state: TargetState | null;
  country: string;
  level: Level | null;
  minCompensation: number | null;
  maxCompensation: number | null;
  hideUnknownCompensation: boolean;
  engineer: boolean;
  companies: string[];
}

interface SavedFilter {
  _id: SavedFilterId;
  name: string;
  search?: string;
  useSearch?: boolean;
  remote?: boolean;
  includeRemote?: boolean;
  state?: TargetState | null;
  country?: string | null;
  level?: Level | null;
  minCompensation?: number;
  maxCompensation?: number;
  hideUnknownCompensation?: boolean;
  engineer?: boolean;
  isSelected: boolean;
  companies?: string[];
}

interface CompanySuggestion {
  name: string;
  count: number;
}

const TARGET_STATES: readonly TargetState[] = ["Washington", "New York", "California", "Arizona"];

const MIN_SALARY = 50000;
const MAX_SALARY = 800000;
const SALARY_STEP = 20000;

const selectSurfaceStyle: CSSProperties = {
  colorScheme: "dark",
  backgroundColor: "#0f172a",
  color: "#e2e8f0",
  borderColor: "#1f2937",
};

const selectOptionStyle: CSSProperties = {
  backgroundColor: "#0f172a",
  color: "#e2e8f0",
};

const DeleteXIcon = ({ className }: { className?: string }) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.7"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    aria-hidden="true"
  >
    <path d="m6 6 12 12M18 6 6 18" />
  </svg>
);

export interface FilterSidebarProps {
  filtersOpen: boolean;
  onClose: () => void;
  filters: Filters;
  updateFilters: (updates: Partial<Filters>, options?: { forceImmediate?: boolean }) => void;
  resetFilters: () => void;

  // Company filter props
  companyInput: string;
  setCompanyInput: (value: string) => void;
  companyInputFocused: boolean;
  setCompanyInputFocused: (focused: boolean) => void;
  companyBlurTimeoutRef: RefObject<ReturnType<typeof setTimeout> | null>;
  filteredCompanySuggestions: CompanySuggestion[];
  addCompanyFilter: (company: string) => void;
  removeCompanyFilter: (company: string) => void;

  // Select IDs for accessibility
  countrySelectId: string;
  stateSelectId: string;
  levelSelectId: string;
  engineerCheckboxId: string;

  // Country options
  countryOptions: string[];

  // Compensation props
  minCompensationInput: string;
  setMinCompensationInput: (value: string) => void;
  minCompInputFocusedRef: RefObject<boolean>;
  commitMinCompensation: (value: string) => void;
  sliderValue: number;
  setSliderValue: (value: number) => void;
  formatCompensationDisplay: (value: number) => string;

  // Saved filters props
  handleSaveCurrentFilter: () => Promise<void>;
  generatedFilterName: string;
  savedFilterList: SavedFilter[];
  selectedSavedFilterId: SavedFilterId | null;
  noFilterActive: boolean;
  buildFilterLabel: (filter: Partial<Filters & { remote?: boolean }>) => string;
  handleSelectSavedFilter: (id: SavedFilterId | null) => Promise<void>;
  handleDeleteSavedFilter: (id: SavedFilterId) => Promise<void>;
}

export function FilterSidebar({
  filtersOpen,
  onClose,
  filters,
  updateFilters,
  resetFilters,
  companyInput,
  setCompanyInput,
  companyInputFocused,
  setCompanyInputFocused,
  companyBlurTimeoutRef,
  filteredCompanySuggestions,
  addCompanyFilter,
  removeCompanyFilter,
  countrySelectId,
  stateSelectId,
  levelSelectId,
  engineerCheckboxId,
  countryOptions,
  minCompensationInput,
  setMinCompensationInput,
  minCompInputFocusedRef,
  commitMinCompensation,
  sliderValue,
  setSliderValue,
  formatCompensationDisplay,
  handleSaveCurrentFilter,
  generatedFilterName,
  savedFilterList,
  selectedSavedFilterId,
  noFilterActive,
  buildFilterLabel,
  handleSelectSavedFilter,
  handleDeleteSavedFilter,
}: FilterSidebarProps) {
  return (
    <>
      {/* Overlay */}
      {filtersOpen && (
        <div
          data-testid="filters-overlay"
          onClick={onClose}
          className="fixed inset-0 z-20 bg-slate-950/40 backdrop-blur-[1px]"
        />
      )}
      {/* Sidebar Filters */}
      <div
        className={`w-full sm:w-80 bg-slate-900/95 border-r border-slate-800 p-4 flex flex-col gap-6 overflow-y-auto transition-transform duration-200 ${filtersOpen ? "translate-x-0" : "-translate-x-full"} fixed inset-y-[64px] left-0 z-30 shadow-2xl backdrop-blur-sm`}
        role="complementary"
        aria-label="Job filters"
        data-testid="filters-panel"
      >
        <div
          className="sticky top-0 z-10 -mx-4 -mt-4 px-4 pt-4 pb-2 border-b border-slate-800 bg-slate-900/95 backdrop-blur-sm flex items-center justify-between"
          data-testid="filters-header"
        >
          <h3 className="text-sm font-semibold text-white">Filters</h3>
          <button
            onClick={onClose}
            className="p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
            aria-label="Close filters"
            data-testid="filters-close"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div className="space-y-3">
          <div className="space-y-2">
            <label className="block text-xs font-semibold text-slate-500 uppercase">Search</label>
            <input
              type="text"
              value={filters.search}
              onChange={(e) => updateFilters({ search: e.target.value })}
              placeholder="Search titles..."
              className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 placeholder-slate-600"
            />
            <p className="text-[11px] text-slate-500">
              Full-text search returns up to 100 of the most recent matching jobs.
            </p>
          </div>
        </div>

        <div>
          <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Companies</label>
          <div className="space-y-2">
            <div className="flex flex-wrap gap-2">
              {filters.companies.map((company) => (
                <span
                  key={company}
                  className="inline-flex items-center gap-1.5 px-2 py-1 rounded-full bg-slate-800/70 text-xs text-slate-100"
                >
                  <span className="truncate max-w-[8rem]">{company}</span>
                  <button
                    type="button"
                    onClick={() => removeCompanyFilter(company)}
                    className="text-slate-400 hover:text-white transition-colors"
                    aria-label={`Remove company filter ${company}`}
                  >
                    <DeleteXIcon className="w-3 h-3" />
                  </button>
                </span>
              ))}
            </div>
            <div className="relative">
              <input
                type="text"
                value={companyInput}
                onChange={(e) => setCompanyInput(e.target.value)}
                onFocus={() => {
                  if (companyBlurTimeoutRef.current) {
                    clearTimeout(companyBlurTimeoutRef.current);
                  }
                  setCompanyInputFocused(true);
                }}
                onBlur={() => {
                  if (companyBlurTimeoutRef.current) {
                    clearTimeout(companyBlurTimeoutRef.current);
                  }
                  companyBlurTimeoutRef.current = setTimeout(() => {
                    setCompanyInputFocused(false);
                  }, 120);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    if (filteredCompanySuggestions.length > 0) {
                      addCompanyFilter(filteredCompanySuggestions[0].name);
                    } else {
                      addCompanyFilter(companyInput);
                    }
                  }
                  if (e.key === "Backspace" && !companyInput && filters.companies.length > 0) {
                    removeCompanyFilter(filters.companies[filters.companies.length - 1]);
                  }
                }}
                placeholder="Add a company..."
                className="w-full bg-slate-950 border-b border-slate-700 px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 focus:ring-0 placeholder-slate-600"
              />
              {companyInputFocused && filteredCompanySuggestions.length > 0 && (
                <div className="absolute left-0 right-0 mt-1 bg-slate-900 border border-slate-800 rounded-md shadow-xl overflow-hidden z-10">
                  {filteredCompanySuggestions.map((suggestion) => (
                    <button
                      key={suggestion.name}
                      type="button"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => addCompanyFilter(suggestion.name)}
                      className="w-full text-left px-3 py-2 hover:bg-slate-800 text-sm text-slate-200 flex items-center justify-between"
                    >
                      <span className="truncate">{suggestion.name}</span>
                      <span className="text-[11px] text-slate-500 ml-3">{suggestion.count} roles</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>

        <div>
          <label
            htmlFor={countrySelectId}
            className="block text-xs font-semibold text-slate-500 uppercase mb-2"
          >
            Country
          </label>
          <select
            id={countrySelectId}
            aria-label="Location"
            value={filters.country}
            onChange={(e) => {
              const next = (e.target.value || "").trim();
              updateFilters(
                {
                  country: next,
                  state: next === "United States" ? filters.state : null,
                },
                { forceImmediate: true },
              );
            }}
            style={selectSurfaceStyle}
            className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
          >
            {countryOptions.map((country) => (
              <option key={country} style={selectOptionStyle} value={country}>
                {country === ""
                  ? "Any country"
                  : country === "Other"
                    ? "Other (non-US)"
                    : country}
              </option>
            ))}
          </select>
          <p className="text-[11px] text-slate-500 mt-1">
            Choose "Any country" to show everything or "Other" to focus on non-US roles.
          </p>
        </div>

        <div>
          <label
            htmlFor={stateSelectId}
            className="block text-xs font-semibold text-slate-500 uppercase mb-2"
          >
            State
          </label>
          <select
            id={stateSelectId}
            value={filters.state ?? ""}
            onChange={(e) =>
              updateFilters(
                {
                  state: (e.target.value || null) as TargetState | null,
                },
                { forceImmediate: true },
              )
            }
            style={selectSurfaceStyle}
            className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
          >
            <option style={selectOptionStyle} value="">Any Target State</option>
            {TARGET_STATES.map((state) => (
              <option key={state} style={selectOptionStyle} value={state}>
                {state}
              </option>
            ))}
          </select>
        </div>

        <div className="flex items-center justify-between gap-3 rounded border border-slate-800 bg-slate-900/40 px-3 py-2">
          <div className="text-xs font-semibold text-slate-500 uppercase">Remote</div>
          <button
            type="button"
            role="switch"
            aria-checked={filters.includeRemote}
            onClick={() => updateFilters({ includeRemote: !filters.includeRemote }, { forceImmediate: true })}
            className={`relative h-6 w-11 rounded-full border transition-colors duration-150 overflow-hidden ${filters.includeRemote ? "bg-emerald-500/40 border-emerald-400" : "bg-slate-800 border-slate-700"
              }`}
            aria-label={filters.includeRemote ? "Remote on" : "Remote off"}
          >
            <span
              className={`absolute left-0.5 top-0.5 h-5 w-5 rounded-full bg-white shadow-sm transition-transform duration-150 ${filters.includeRemote ? "translate-x-5" : "translate-x-0"
                }`}
            />
          </button>
        </div>

        <div>
          <label
            htmlFor={levelSelectId}
            className="block text-xs font-semibold text-slate-500 uppercase mb-2"
          >
            Level
          </label>
          <select
            id={levelSelectId}
            value={filters.level || ""}
            onChange={(e) =>
              updateFilters({
                level: e.target.value === "" ? null : (e.target.value as Level),
              })
            }
            style={selectSurfaceStyle}
            className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
          >
            <option style={selectOptionStyle} value="">Any Level</option>
            <option style={selectOptionStyle} value="staff">Staff</option>
            <option style={selectOptionStyle} value="senior">Senior</option>
            <option style={selectOptionStyle} value="mid">Mid</option>
            <option style={selectOptionStyle} value="junior">Junior</option>
          </select>
        </div>

        <label
          htmlFor={engineerCheckboxId}
          className="flex items-center justify-between gap-3 rounded border border-slate-800 bg-slate-900/40 px-3 py-2 cursor-pointer"
        >
          <span className="text-[11px] font-semibold uppercase text-slate-500">Engineer titles only</span>
          <input
            id={engineerCheckboxId}
            type="checkbox"
            className="h-4 w-4 rounded border-slate-700 bg-slate-900 text-blue-500 focus:ring-blue-500"
            checked={filters.engineer}
            onChange={(e) => updateFilters({ engineer: e.target.checked }, { forceImmediate: true })}
          />
        </label>

        <div>
          <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Min Salary</label>
          <input
            type="text"
            value={minCompensationInput}
            onChange={(e) => {
              const rawValue = e.target.value;
              setMinCompensationInput(rawValue);
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                minCompInputFocusedRef.current = false;
                commitMinCompensation((e.currentTarget as HTMLInputElement).value);
              }
            }}
            onFocus={() => {
              minCompInputFocusedRef.current = true;
            }}
            onBlur={(e) => {
              minCompInputFocusedRef.current = false;
              commitMinCompensation(e.currentTarget.value);
            }}
            placeholder="$50k"
            className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500 placeholder-slate-600"
          />
          <div className="mt-3">
            <div className="flex items-center justify-between text-[11px] text-slate-500 mb-1">
              <span>$50k</span>
              <span>$800k</span>
            </div>
            <input
              type="range"
              min={MIN_SALARY}
              max={MAX_SALARY}
              step={SALARY_STEP}
              value={sliderValue}
              onChange={(e) => {
                const value = parseInt(e.target.value, 10);
                setSliderValue(value);
                setMinCompensationInput(formatCompensationDisplay(value));
                updateFilters({ minCompensation: value });
              }}
              className="salary-slider w-full"
            />
          </div>
          <label className="mt-3 flex items-center justify-between gap-3 rounded border border-slate-800 bg-slate-900/40 px-3 py-2 cursor-pointer">
            <span className="text-[11px] font-semibold uppercase text-slate-500">Hide unknown compensation</span>
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-slate-700 bg-slate-900 text-blue-500 focus:ring-blue-500"
              checked={filters.hideUnknownCompensation}
              onChange={(e) => updateFilters({ hideUnknownCompensation: e.target.checked }, { forceImmediate: true })}
            />
          </label>
        </div>

        <div className="space-y-2">
          <button
            onClick={() => { void handleSaveCurrentFilter(); }}
            className="w-full px-3 py-2 text-xs bg-blue-600 text-white rounded hover:bg-blue-500 transition-colors"
          >
            Save as filter
          </button>
          <p className="text-[11px] text-slate-500 leading-tight">
            Saves as "{generatedFilterName}" based on the fields above.
          </p>
        </div>

        <div className="border-t border-slate-800 pt-4 space-y-3">
          <div>
            <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Saved Filters</label>
            <div className="flex flex-col gap-2">
              <div className="min-w-0">
                <button
                  onClick={() => { void handleSelectSavedFilter(null); }}
                  className={`w-full px-3 py-1.5 rounded-md border text-xs transition-colors overflow-hidden min-w-0 ${noFilterActive
                    ? "border-blue-500/60 bg-blue-900/40 text-blue-100"
                    : "border-slate-700 text-slate-300 hover:border-slate-500"
                    }`}
                >
                  No filter
                </button>
              </div>
              {savedFilterList.map((filter) => {
                const isActive = filter._id === selectedSavedFilterId || filter.isSelected;
                const filterLabel = buildFilterLabel({
                  search: filter.search ?? "",
                  state: (filter.state as TargetState | null) ?? null,
                  country: filter.country ?? "United States",
                  includeRemote: filter.includeRemote ?? (filter.remote !== false),
                  level: (filter.level as Level | null) ?? null,
                  minCompensation: filter.minCompensation ?? null,
                  maxCompensation: filter.maxCompensation ?? null,
                  hideUnknownCompensation: filter.hideUnknownCompensation ?? false,
                  engineer: filter.engineer ?? false,
                  companies: filter.companies ?? [],
                });
                return (
                  <div key={filter._id} className="min-w-0">
                    <div className="flex items-stretch gap-1 w-full">
                      <button
                        onClick={() => { void handleSelectSavedFilter(filter._id as SavedFilterId); }}
                        className={`flex-1 px-3 py-1.5 rounded-md border text-xs transition-colors text-left overflow-hidden min-w-0 ${isActive
                          ? "border-blue-500/60 bg-blue-900/40 text-blue-100"
                          : "border-slate-700 text-slate-300 hover:border-slate-500"
                          }`}
                      >
                        <div className="font-medium truncate">{filterLabel}</div>
                      </button>
                      <button
                        onClick={() => { void handleDeleteSavedFilter(filter._id as SavedFilterId); }}
                        className="px-2 py-1.5 rounded-md border border-red-500/40 bg-red-500/5 text-[11px] text-red-200 hover:border-red-400 hover:bg-red-500/10 transition-colors flex items-center justify-center w-9 shrink-0"
                        title="Delete saved filter"
                        aria-label={`Delete saved filter ${filterLabel}`}
                      >
                        <DeleteXIcon className="w-3.5 h-3.5" />
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        <div className="mt-auto pt-6 border-t border-slate-800">
          <button
            onClick={() => resetFilters()}
            className="w-full py-2 text-xs text-slate-400 hover:text-white hover:bg-slate-800 rounded transition-colors"
          >
            Reset Filters
          </button>
        </div>
      </div>
    </>
  );
}
