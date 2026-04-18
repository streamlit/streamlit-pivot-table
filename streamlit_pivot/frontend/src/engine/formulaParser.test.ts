/**
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, it, expect } from "vitest";
import {
  compileFormula,
  extractFieldRefs,
  FormulaParseError,
} from "./formulaParser";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const grammarCases = JSON.parse(
  readFileSync(
    resolve(__dirname, "../../../../tests/formula_grammar_cases.json"),
    "utf-8",
  ),
);

describe("formulaParser", () => {
  describe("compileFormula", () => {
    it("respects arithmetic precedence", () => {
      const f = compileFormula('"A" + "B" * "C"');
      expect(f.evaluate({ A: 2, B: 3, C: 4 })).toBe(14);
    });

    it("handles nested parentheses", () => {
      const f = compileFormula('("A" + "B") * "C"');
      expect(f.evaluate({ A: 2, B: 3, C: 4 })).toBe(20);
    });

    it("evaluates abs()", () => {
      const f = compileFormula('abs("A" - "B")');
      expect(f.evaluate({ A: 3, B: 7 })).toBe(4);
    });

    it("evaluates min()", () => {
      const f = compileFormula('min("A", "B")');
      expect(f.evaluate({ A: 3, B: 7 })).toBe(3);
    });

    it("evaluates max()", () => {
      const f = compileFormula('max("A", "B")');
      expect(f.evaluate({ A: 3, B: 7 })).toBe(7);
    });

    it("evaluates round() with decimals", () => {
      const f = compileFormula('round("A", 2)');
      expect(f.evaluate({ A: 3.14159 })).toBe(3.14);
    });

    it("evaluates round() without decimals", () => {
      const f = compileFormula('round("A")');
      expect(f.evaluate({ A: 3.7 })).toBe(4);
    });

    it("evaluates if() — true branch", () => {
      const f = compileFormula('if("Cost" > 0, "Revenue" / "Cost", 0)');
      expect(f.evaluate({ Revenue: 100, Cost: 50 })).toBe(2);
    });

    it("evaluates if() — false branch", () => {
      const f = compileFormula('if("Cost" > 0, "Revenue" / "Cost", 0)');
      expect(f.evaluate({ Revenue: 100, Cost: 0 })).toBe(0);
    });

    it("evaluates comparison operators (>, >=, <, <=, ==, !=)", () => {
      expect(compileFormula('"A" > "B"').evaluate({ A: 5, B: 3 })).toBe(1);
      expect(compileFormula('"A" > "B"').evaluate({ A: 3, B: 5 })).toBe(0);
      expect(compileFormula('"A" >= "B"').evaluate({ A: 5, B: 5 })).toBe(1);
      expect(compileFormula('"A" < "B"').evaluate({ A: 3, B: 5 })).toBe(1);
      expect(compileFormula('"A" <= "B"').evaluate({ A: 5, B: 5 })).toBe(1);
      expect(compileFormula('"A" == "B"').evaluate({ A: 5, B: 5 })).toBe(1);
      expect(compileFormula('"A" != "B"').evaluate({ A: 5, B: 3 })).toBe(1);
    });

    it("evaluates logical operators (and/&&, or/||)", () => {
      const f = compileFormula('"A" > 0 and "B" > 0 ? 1 : 0');
      expect(f.evaluate({ A: 1, B: 1 })).toBe(1);
      expect(f.evaluate({ A: 1, B: -1 })).toBe(0);
      expect(f.evaluate({ A: -1, B: 1 })).toBe(0);
    });

    it("accepts field refs as quoted strings only", () => {
      const f = compileFormula('"Revenue" / "Cost"');
      expect(f.fieldRefs).toEqual(expect.arrayContaining(["Revenue", "Cost"]));
      expect(f.evaluate({ Revenue: 100, Cost: 25 })).toBe(4);
    });

    it("handles field names with spaces", () => {
      const f = compileFormula('"Gross Revenue" - "Net Cost"');
      expect(f.evaluate({ "Gross Revenue": 1000, "Net Cost": 300 })).toBe(700);
      expect(f.fieldRefs).toContain("Gross Revenue");
      expect(f.fieldRefs).toContain("Net Cost");
    });

    it("extracts field refs correctly", () => {
      const f = compileFormula('"Revenue" + "Cost" * "Tax"');
      expect(f.fieldRefs).toHaveLength(3);
      expect(f.fieldRefs).toContain("Revenue");
      expect(f.fieldRefs).toContain("Cost");
      expect(f.fieldRefs).toContain("Tax");
    });

    it("rejects aggregate wrapper syntax with helpful message", () => {
      expect(() => compileFormula('sum("Revenue")')).toThrow(FormulaParseError);
      expect(() => compileFormula('sum("Revenue")')).toThrow(
        /Unknown function 'sum'/,
      );
      expect(() => compileFormula('avg("Cost")')).toThrow(
        /Unknown function 'avg'/,
      );
    });

    it("rejects bare identifiers", () => {
      expect(() => compileFormula("Revenue + Cost")).toThrow(FormulaParseError);
      expect(() => compileFormula("Revenue + Cost")).toThrow(
        /Bare identifier "Revenue"/,
      );
    });

    it("propagates null when a scope field is null", () => {
      const f = compileFormula('"A" + "B"');
      expect(f.evaluate({ A: 1, B: null })).toBeNull();
      expect(f.evaluate({ A: null, B: 1 })).toBeNull();
    });

    it("returns null for division by zero", () => {
      const f = compileFormula('"A" / "B"');
      expect(f.evaluate({ A: 100, B: 0 })).toBeNull();
    });

    it("returns null for missing scope field (undefined)", () => {
      const f = compileFormula('"A" + "B"');
      expect(f.evaluate({ A: 1 })).toBeNull();
    });

    it("evaluates deeply nested expressions", () => {
      const f = compileFormula('("A" + "B") * ("C" - "D") / ("E" + 1)');
      // (1+2)*(10-3)/(2+1) = 3*7/3 = 7
      expect(f.evaluate({ A: 1, B: 2, C: 10, D: 3, E: 2 })).toBe(7);
    });

    it("rejects constant-only formula (no field refs)", () => {
      expect(() => compileFormula("42")).toThrow(FormulaParseError);
      expect(() => compileFormula("42")).toThrow(
        /must reference at least one field/,
      );
    });

    it("if() can rescue null by short-circuiting the null branch", () => {
      const f = compileFormula('if("Cost" > 0, "Revenue" / "Cost", 0)');
      expect(f.evaluate({ Revenue: 100, Cost: 0 })).toBe(0);
    });

    it("returns null for non-finite results (Infinity)", () => {
      const f = compileFormula('"A" * 1e308 * 1e308');
      expect(f.evaluate({ A: 1 })).toBeNull();
    });

    it("preserves the source formula string", () => {
      const formula = '"Revenue" / "Cost"';
      const f = compileFormula(formula);
      expect(f.source).toBe(formula);
    });

    it("evaluates modulo operator (%)", () => {
      const f = compileFormula('"A" % "B"');
      expect(f.evaluate({ A: 10, B: 3 })).toBe(1);
      expect(f.evaluate({ A: 7, B: 2 })).toBe(1);
      expect(f.evaluate({ A: 6, B: 3 })).toBe(0);
    });

    it("returns null for modulo by zero", () => {
      const f = compileFormula('"A" % "B"');
      expect(f.evaluate({ A: 10, B: 0 })).toBeNull();
    });

    it("evaluates or operator independently", () => {
      const f = compileFormula('"A" > 0 or "B" > 0 ? 1 : 0');
      expect(f.evaluate({ A: 1, B: -1 })).toBe(1);
      expect(f.evaluate({ A: -1, B: 1 })).toBe(1);
      expect(f.evaluate({ A: -1, B: -1 })).toBe(0);
    });

    it("and short-circuits: null left → null", () => {
      const f = compileFormula('"A" > 0 and "B" > 0 ? 1 : 0');
      expect(f.evaluate({ A: null, B: 1 })).toBeNull();
    });

    it("and short-circuits: falsy left skips right", () => {
      const f = compileFormula('"A" > 0 and "B" > 0 ? 1 : 0');
      expect(f.evaluate({ A: -1, B: null })).toBe(0);
    });

    it("or short-circuits: truthy left skips right", () => {
      const f = compileFormula('"A" > 0 or "B" > 0 ? 1 : 0');
      expect(f.evaluate({ A: 1, B: null })).toBe(1);
    });

    it("or short-circuits: null left → null", () => {
      const f = compileFormula('"A" > 0 or "B" > 0 ? 1 : 0');
      expect(f.evaluate({ A: null, B: 1 })).toBeNull();
    });

    it("evaluates 'not' as logical negation", () => {
      const f = compileFormula('if(not "A", "B", "A")');
      expect(f.evaluate({ A: 0, B: 99 })).toBe(99);
      expect(f.evaluate({ A: 5, B: 99 })).toBe(5);
    });

    it("null propagates through 'not'", () => {
      const f = compileFormula('if(not "A", 1, 0)');
      expect(f.evaluate({ A: null })).toBeNull();
    });

    it("null propagates through abs()", () => {
      const f = compileFormula('abs("A")');
      expect(f.evaluate({ A: null })).toBeNull();
    });

    it("null propagates through min()", () => {
      const f = compileFormula('min("A", "B")');
      expect(f.evaluate({ A: null, B: 5 })).toBeNull();
      expect(f.evaluate({ A: 5, B: null })).toBeNull();
    });

    it("null propagates through max()", () => {
      const f = compileFormula('max("A", "B")');
      expect(f.evaluate({ A: null, B: 5 })).toBeNull();
      expect(f.evaluate({ A: 5, B: null })).toBeNull();
    });

    it("null propagates through round()", () => {
      const f = compileFormula('round("A", 2)');
      expect(f.evaluate({ A: null })).toBeNull();
    });

    it("if() with null condition returns null", () => {
      const f = compileFormula('if("A", "B", "C")');
      expect(f.evaluate({ A: null, B: 10, C: 20 })).toBeNull();
    });

    it("handles power operator", () => {
      const f = compileFormula('"A" ^ 2');
      expect(f.evaluate({ A: 3 })).toBe(9);
    });

    it("handles negation", () => {
      const f = compileFormula('-"A"');
      expect(f.evaluate({ A: 5 })).toBe(-5);
    });

    it("throws FormulaParseError on invalid syntax", () => {
      expect(() => compileFormula('"A" + + *')).toThrow(FormulaParseError);
    });

    it("throws FormulaParseError on empty string", () => {
      expect(() => compileFormula("")).toThrow(FormulaParseError);
      expect(() => compileFormula("")).toThrow(/Formula is empty/);
    });
  });

  describe("function arity validation", () => {
    it("rejects abs() with 0 arguments", () => {
      expect(() => compileFormula('abs() + "A"')).toThrow(FormulaParseError);
    });

    it("rejects abs() with 2 arguments", () => {
      expect(() => compileFormula('abs("A", "B")')).toThrow(FormulaParseError);
    });

    it("rejects min() with 1 argument", () => {
      expect(() => compileFormula('min("A")')).toThrow(
        /min\(\) expects exactly 2 argument\(s\), got 1/,
      );
    });

    it("rejects min() with 3 arguments", () => {
      expect(() => compileFormula('min("A", "B", "C")')).toThrow(
        /min\(\) expects exactly 2 argument\(s\), got 3/,
      );
    });

    it("rejects max() with 3 arguments", () => {
      expect(() => compileFormula('max("A", "B", "C")')).toThrow(
        /max\(\) expects exactly 2 argument\(s\), got 3/,
      );
    });

    it("rejects if() with 2 arguments", () => {
      expect(() => compileFormula('if("A" > 0, "B")')).toThrow(
        /if\(\) expects exactly 3 argument\(s\), got 2/,
      );
    });

    it("rejects if() with 4 arguments", () => {
      expect(() => compileFormula('if("A" > 0, "B", "C", "D")')).toThrow(
        /if\(\) expects exactly 3 argument\(s\), got 4/,
      );
    });

    it("accepts round() with 1 argument", () => {
      const f = compileFormula('round("A")');
      expect(f.evaluate({ A: 3.7 })).toBe(4);
    });

    it("accepts round() with 2 arguments", () => {
      const f = compileFormula('round("A", 2)');
      expect(f.evaluate({ A: 3.756 })).toBe(3.76);
    });

    it("rejects round() with 3 arguments", () => {
      expect(() => compileFormula('round("A", 2, 3)')).toThrow(
        /round\(\) expects 1–2 argument\(s\), got 3/,
      );
    });
  });

  describe("extractFieldRefs", () => {
    it("returns field refs from a valid formula", () => {
      const refs = extractFieldRefs('"Revenue" / "Cost"');
      expect(refs).toEqual(expect.arrayContaining(["Revenue", "Cost"]));
    });

    it("returns empty array for empty formula", () => {
      expect(extractFieldRefs("")).toEqual([]);
      expect(extractFieldRefs("  ")).toEqual([]);
    });

    it("throws on syntactically invalid formulas instead of falling back", () => {
      expect(() => extractFieldRefs('"Revenue" + * "Cost"')).toThrow(
        FormulaParseError,
      );
    });

    it("deduplicates field refs", () => {
      const refs = extractFieldRefs('"A" + "A" + "B"');
      expect(refs.filter((r) => r === "A")).toHaveLength(1);
    });
  });

  describe("shared grammar parity (formula_grammar_cases.json)", () => {
    const cases = grammarCases as Array<{
      formula: string;
      valid: boolean;
      note: string;
    }>;
    for (const { formula, valid, note } of cases) {
      it(`${valid ? "accepts" : "rejects"}: ${note} — ${JSON.stringify(formula)}`, () => {
        if (valid) {
          expect(() => compileFormula(formula)).not.toThrow();
        } else {
          expect(() => compileFormula(formula)).toThrow();
        }
      });
    }
  });
});
