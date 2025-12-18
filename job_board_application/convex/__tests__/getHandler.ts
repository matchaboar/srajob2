type HandlerFn = (...args: any[]) => any;

export const getHandler = <T extends HandlerFn>(fn: unknown): T => {
  const candidate = fn as { _handler?: HandlerFn; handler?: HandlerFn };
  if (typeof candidate._handler === "function") return candidate._handler as T;
  if (typeof candidate.handler === "function") return candidate.handler as T;
  return fn as T;
};
