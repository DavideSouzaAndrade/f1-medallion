import { describe, expect, it } from "vitest";
import { corComposto, fmtHa, fmtMs, fmtVolta } from "./format";

describe("fmtVolta", () => {
  it("formata como cronômetro", () => {
    expect(fmtVolta(83.412)).toBe("1:23.412");
    expect(fmtVolta(65.0)).toBe("1:05.000");
  });
  it("mostra traço quando não há tempo", () => {
    expect(fmtVolta(null)).toBe("—");
  });
});

describe("fmtMs", () => {
  it("usa segundos a partir de 1 s", () => {
    expect(fmtMs(117)).toBe("117 ms");
    expect(fmtMs(2200)).toBe("2,2 s");
    expect(fmtMs(172233)).toBe("2,9 min");
    expect(fmtMs(164231495)).toBe("1,9 d");
    expect(fmtMs(null)).toBe("—");
  });
});

describe("fmtHa", () => {
  it("escolhe a unidade", () => {
    expect(fmtHa(1000, 13000)).toBe("há 12 s");
    expect(fmtHa(0, 180000)).toBe("há 3 min");
    expect(fmtHa(null, 0)).toBe("—");
  });
});

describe("corComposto", () => {
  it("mantém a cor do composto fixa", () => {
    expect(corComposto("SOFT")).toBe("var(--s1)");
    expect(corComposto("HARD")).toBe("var(--s3)");
  });
});
