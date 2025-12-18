type HandlerFn = (...args: any[]) => any;

export const getHandler = <T extends HandlerFn>(
  fn: T | { _handler?: T; handler?: T }
): T => {
  if (typeof fn._handler === "function") return fn._handler;
  if (typeof fn.handler === "function") return fn.handler;
  if (typeof fn === "function") return fn;
  return fn as unknown as T;
};
