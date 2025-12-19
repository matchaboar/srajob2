import { describe, expect, it } from "vitest";
import { getHandler } from "./getHandler";

describe("getHandler", () => {
  it("prefers _handler when both handler keys exist", () => {
    const handler = () => "from _handler";
    const fallback = () => "from handler";
    const wrapped = { _handler: handler, handler: fallback };

    const result = getHandler<typeof handler>(wrapped);

    expect(result).toBe(handler);
    expect(result()).toBe("from _handler");
  });

  it("uses handler when _handler is not a function", () => {
    const handler = () => "from handler";
    const wrapped = { _handler: "nope", handler };

    const result = getHandler<typeof handler>(wrapped);

    expect(result).toBe(handler);
    expect(result()).toBe("from handler");
  });

  it("returns the original function when no handler keys exist", () => {
    const fn = () => "raw";

    const result = getHandler<typeof fn>(fn);

    expect(result).toBe(fn);
    expect(result()).toBe("raw");
  });
});
