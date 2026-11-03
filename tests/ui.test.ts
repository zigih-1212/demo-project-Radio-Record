import { test } from "node:test";
import assert from "node:assert/strict";
import { APP_HTML } from "../src/ui.js";

// Regression for C2: the dashboard inline script used to contain an
// over-escaped quote sequence (backslash-backslash-quote) inside a
// String.raw template literal. The served HTML then had a JavaScript
// SyntaxError that killed the ENTIRE script block — login, table, filters,
// drawer and modal were all dead in the browser (API was unaffected).

test("dashboard inline script parses without SyntaxError", () => {
  const match = APP_HTML.match(/<script>([\s\S]*)<\/script>/);
  assert.ok(match, "APP_HTML must contain a script block");
  assert.doesNotThrow(() => new Function(match[1]), "dashboard script must parse as valid JavaScript");
});

test("dashboard row template emits valid onclick quotes", () => {
  const bs = String.fromCharCode(92); // backslash
  assert.ok(!APP_HTML.includes("openDetail(" + bs + bs), "onclick must not contain a double-backslash escape");
  assert.ok(APP_HTML.includes("openDetail(" + bs + "'"), "onclick must contain an escaped single quote");
});
