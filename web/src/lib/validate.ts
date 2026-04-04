const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isValidUUID(s: string): boolean {
  return UUID_RE.test(s);
}

export function sanitizeZip(s: string): string {
  return s.replace(/[^0-9]/g, "").slice(0, 5);
}

export function sanitizeState(s: string): string {
  return s.replace(/[^A-Za-z]/g, "").slice(0, 2).toUpperCase();
}
