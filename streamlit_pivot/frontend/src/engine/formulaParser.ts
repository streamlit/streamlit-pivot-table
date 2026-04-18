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

import { Parser } from "expr-eval";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface CompiledFormula {
  evaluate: (scope: Record<string, number | null>) => number | null;
  fieldRefs: readonly string[];
  source: string;
}

export class FormulaParseError extends Error {
  constructor(
    message: string,
    public readonly position?: number,
  ) {
    super(message);
    this.name = "FormulaParseError";
  }
}

// ---------------------------------------------------------------------------
// Known functions — anything else is rejected
// ---------------------------------------------------------------------------

const KNOWN_FUNCTIONS = new Set(["abs", "min", "max", "round", "if"]);

// ---------------------------------------------------------------------------
// Shared parser instance (stateless after creation)
// ---------------------------------------------------------------------------

const exprParser = new Parser({
  operators: {
    logical: true,
    comparison: true,
    concatenate: false,
    conditional: true,
    assignment: false,
  } as Record<string, boolean>,
});

// Move `round` from unary to multi-arg function so round(x, decimals) parses
delete (exprParser as unknown as Record<string, Record<string, unknown>>)
  .unaryOps["round"];
(exprParser as unknown as Record<string, Record<string, unknown>>).functions[
  "round"
] = (x: number, d?: number) => {
  if (d === undefined) return Math.round(x);
  const f = Math.pow(10, d);
  return Math.round(x * f) / f;
};

// ---------------------------------------------------------------------------
// expr-eval token types (postfix / RPN stack machine)
// ---------------------------------------------------------------------------

interface Token {
  type: string;
  value: unknown;
}

// ---------------------------------------------------------------------------
// Token walking for field ref extraction and validation
// ---------------------------------------------------------------------------

function walkTokens(tokens: Token[], refs: Set<string>): void {
  for (const token of tokens) {
    switch (token.type) {
      case "INUMBER":
        if (typeof token.value === "string") {
          refs.add(token.value);
        }
        break;

      case "IVAR": {
        const name = token.value as string;
        if (KNOWN_FUNCTIONS.has(name)) break;
        if (
          name === "E" ||
          name === "PI" ||
          name === "true" ||
          name === "false"
        )
          break;
        throw new FormulaParseError(
          `Bare identifier "${name}" is not allowed. ` +
            `Wrap field names in quotes: "${name}"`,
        );
      }

      case "IFUNCALL": {
        break;
      }

      case "IEXPR": {
        const subTokens = token.value as Token[];
        walkTokens(subTokens, refs);
        break;
      }

      default:
        break;
    }
  }
}

function checkUnknownFunctions(tokens: Token[]): void {
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.type === "IFUNCALL") {
      let fnIdx = -1;
      let argCount = token.value as number;
      let skip = 0;
      for (let j = i - 1; j >= 0; j--) {
        if (skip > 0) {
          skip--;
          continue;
        }
        const t = tokens[j];
        if (t.type === "IVAR") {
          if (argCount <= 0) {
            fnIdx = j;
            break;
          }
          argCount--;
        } else if (t.type === "INUMBER") {
          argCount--;
        } else if (t.type === "IOP2") {
          skip += 1;
        } else if (t.type === "IOP1") {
          // skip
        } else if (t.type === "IEXPR") {
          argCount--;
        } else if (t.type === "IFUNCALL") {
          skip += t.value as number;
        }
      }
      if (fnIdx >= 0) {
        const fnName = tokens[fnIdx].value as string;
        if (!KNOWN_FUNCTIONS.has(fnName)) {
          throw new FormulaParseError(
            `Unknown function '${fnName}'. ` +
              `Use "${fnName}" directly — aggregation is controlled by the field's aggregation setting.`,
          );
        }
      }
    } else if (token.type === "IEXPR") {
      checkUnknownFunctions(token.value as Token[]);
    }
  }
}

// ---------------------------------------------------------------------------
// Arity validation — reject wrong argument counts at compile time
// ---------------------------------------------------------------------------

const FUNCTION_ARITY: Record<string, { min: number; max: number }> = {
  abs: { min: 1, max: 1 },
  min: { min: 2, max: 2 },
  max: { min: 2, max: 2 },
  round: { min: 1, max: 2 },
  if: { min: 3, max: 3 },
};

function checkFunctionArity(tokens: Token[]): void {
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.type === "IFUNCALL") {
      const argCount = token.value as number;
      let fnIdx = -1;
      let remaining = argCount;
      let skip = 0;
      for (let j = i - 1; j >= 0; j--) {
        if (skip > 0) {
          skip--;
          continue;
        }
        const t = tokens[j];
        if (t.type === "IVAR") {
          if (remaining <= 0) {
            fnIdx = j;
            break;
          }
          remaining--;
        } else if (t.type === "INUMBER") {
          remaining--;
        } else if (t.type === "IOP2") {
          skip += 1;
        } else if (t.type === "IOP1") {
          // unary op consumes nothing extra
        } else if (t.type === "IEXPR") {
          remaining--;
        } else if (t.type === "IFUNCALL") {
          skip += t.value as number;
        }
      }
      if (fnIdx >= 0) {
        const fnName = tokens[fnIdx].value as string;
        const arity = FUNCTION_ARITY[fnName];
        if (arity) {
          if (argCount < arity.min || argCount > arity.max) {
            const expected =
              arity.min === arity.max
                ? `exactly ${arity.min}`
                : `${arity.min}–${arity.max}`;
            throw new FormulaParseError(
              `${fnName}() expects ${expected} argument(s), got ${argCount}`,
            );
          }
        }
      }
    } else if (token.type === "IEXPR") {
      checkFunctionArity(token.value as Token[]);
    }
  }
}

// ---------------------------------------------------------------------------
// Stack-based evaluator (CSP-safe — no Function()/eval())
// ---------------------------------------------------------------------------

type StackValue = number | null | string | Token[];

function evalTokens(
  tokens: Token[],
  scope: Record<string, number | null>,
): number | null {
  const stack: StackValue[] = [];

  for (const token of tokens) {
    switch (token.type) {
      case "INUMBER": {
        const v = token.value;
        if (typeof v === "string") {
          const resolved = scope[v];
          stack.push(resolved === undefined ? null : resolved);
        } else {
          stack.push(typeof v === "number" ? v : null);
        }
        break;
      }

      case "IVAR": {
        const name = token.value as string;
        if (name === "true") {
          stack.push(1);
          break;
        }
        if (name === "false") {
          stack.push(0);
          break;
        }
        if (name === "E") {
          stack.push(Math.E);
          break;
        }
        if (name === "PI") {
          stack.push(Math.PI);
          break;
        }
        stack.push(name);
        break;
      }

      case "IOP1": {
        const op = token.value as string;
        const a = stack.pop() as number | null;
        if (a === null) {
          stack.push(null);
          break;
        }
        switch (op) {
          case "-":
            stack.push(-a);
            break;
          case "+":
            stack.push(a);
            break;
          case "!":
          case "not":
            stack.push(a ? 0 : 1);
            break;
          case "abs":
            stack.push(Math.abs(a));
            break;
          case "ceil":
            stack.push(Math.ceil(a));
            break;
          case "floor":
            stack.push(Math.floor(a));
            break;
          case "round":
            stack.push(Math.round(a));
            break;
          case "sqrt": {
            stack.push(a < 0 ? null : Math.sqrt(a));
            break;
          }
          default:
            stack.push(null);
            break;
        }
        break;
      }

      case "IOP2": {
        const op = token.value as string;

        if (op === "and" || op === "&&") {
          const rExpr = stack.pop() as number | null | Token[];
          const lVal = stack.pop() as number | null;
          if (lVal === null) {
            stack.push(null);
            break;
          }
          if (!lVal) {
            stack.push(0);
            break;
          }
          const rVal = Array.isArray(rExpr)
            ? evalTokens(rExpr as Token[], scope)
            : rExpr;
          if (rVal === null) {
            stack.push(null);
            break;
          }
          stack.push(rVal ? 1 : 0);
          break;
        }
        if (op === "or" || op === "||") {
          const rExpr = stack.pop() as number | null | Token[];
          const lVal = stack.pop() as number | null;
          if (lVal === null) {
            stack.push(null);
            break;
          }
          if (lVal) {
            stack.push(1);
            break;
          }
          const rVal = Array.isArray(rExpr)
            ? evalTokens(rExpr as Token[], scope)
            : rExpr;
          if (rVal === null) {
            stack.push(null);
            break;
          }
          stack.push(rVal ? 1 : 0);
          break;
        }

        const right = stack.pop() as number | null;
        const left = stack.pop() as number | null;
        if (left === null || right === null) {
          stack.push(null);
          break;
        }
        switch (op) {
          case "+":
            stack.push(left + right);
            break;
          case "-":
            stack.push(left - right);
            break;
          case "*":
            stack.push(left * right);
            break;
          case "/":
            stack.push(right === 0 ? null : left / right);
            break;
          case "^":
            stack.push(Math.pow(left, right));
            break;
          case "%":
            stack.push(right === 0 ? null : left % right);
            break;
          case ">":
            stack.push(left > right ? 1 : 0);
            break;
          case ">=":
            stack.push(left >= right ? 1 : 0);
            break;
          case "<":
            stack.push(left < right ? 1 : 0);
            break;
          case "<=":
            stack.push(left <= right ? 1 : 0);
            break;
          case "==":
            stack.push(left === right ? 1 : 0);
            break;
          case "!=":
            stack.push(left !== right ? 1 : 0);
            break;
          default:
            stack.push(null);
            break;
        }
        break;
      }

      case "IOP3": {
        const op = token.value as string;
        if (op === "?") {
          const falseExpr = stack.pop() as number | null | Token[];
          const trueExpr = stack.pop() as number | null | Token[];
          const cond = stack.pop() as number | null;
          if (cond === null) {
            stack.push(null);
            break;
          }
          const branch = cond ? trueExpr : falseExpr;
          const result = Array.isArray(branch)
            ? evalTokens(branch as Token[], scope)
            : branch;
          stack.push(result);
        }
        break;
      }

      case "IFUNCALL": {
        const argCount = token.value as number;
        const args: (number | null)[] = [];
        for (let i = 0; i < argCount; i++) {
          args.unshift(stack.pop() as number | null);
        }
        const fnName = stack.pop() as unknown as string;

        switch (fnName) {
          case "abs": {
            if (args[0] === null) {
              stack.push(null);
              break;
            }
            stack.push(Math.abs(args[0]));
            break;
          }
          case "min": {
            if (args[0] === null || args[1] === null) {
              stack.push(null);
              break;
            }
            stack.push(Math.min(args[0], args[1]));
            break;
          }
          case "max": {
            if (args[0] === null || args[1] === null) {
              stack.push(null);
              break;
            }
            stack.push(Math.max(args[0], args[1]));
            break;
          }
          case "round": {
            if (args[0] === null) {
              stack.push(null);
              break;
            }
            const decimals = args.length > 1 && args[1] !== null ? args[1] : 0;
            const factor = Math.pow(10, decimals);
            stack.push(Math.round(args[0] * factor) / factor);
            break;
          }
          case "if": {
            if (args[0] === null) {
              stack.push(null);
              break;
            }
            stack.push(args[0] ? args[1] : args[2]);
            break;
          }
          default:
            stack.push(null);
            break;
        }
        break;
      }

      case "IEXPR": {
        stack.push(token.value as Token[]);
        break;
      }

      default:
        break;
    }
  }

  const top = stack.pop();
  if (top === null || top === undefined) return null;
  if (typeof top === "number") return top;
  if (Array.isArray(top)) return evalTokens(top as Token[], scope);
  return null;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function compileFormula(formula: string): CompiledFormula {
  if (!formula || formula.trim() === "") {
    throw new FormulaParseError("Formula is empty");
  }

  let parsed: { tokens: Token[] };
  try {
    parsed = exprParser.parse(formula) as unknown as { tokens: Token[] };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new FormulaParseError(`Parse error: ${msg}`);
  }

  const tokens = parsed.tokens;

  checkUnknownFunctions(tokens);
  checkFunctionArity(tokens);

  const refSet = new Set<string>();
  walkTokens(tokens, refSet);

  if (refSet.size === 0) {
    throw new FormulaParseError(
      'Formula must reference at least one field (e.g. \'"Revenue" / "Cost"\')',
    );
  }

  const fieldRefs = [...refSet];

  return {
    evaluate: (scope: Record<string, number | null>): number | null => {
      const result = evalTokens(tokens, scope);
      if (result === null) return null;
      if (typeof result !== "number" || !isFinite(result)) return null;
      return result;
    },
    fieldRefs,
    source: formula,
  };
}

export function extractFieldRefs(formula: string): string[] {
  if (!formula || formula.trim() === "") return [];
  const compiled = compileFormula(formula);
  return [...compiled.fieldRefs];
}
