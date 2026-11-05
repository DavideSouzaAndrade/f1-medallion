import { describe, expect, it } from "vitest";
import { corComposto, escalaComum, fmtHa, fmtMs, fmtVolta } from "./format";

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

describe("escalaComum", () => {
  it("cobre real e previsto com a mesma escala, extremos incluídos", () => {
    // Monza inteira: previsto de 84,9 a 87,7 s, real de 83,5 a 98,8 s (safety car).
    const e = escalaComum([84.9, 87.7, 83.5, 98.8])!;
    expect(e.dominio).toEqual([80, 100]);
    expect(e.ticks).toEqual([80, 85, 90, 95, 100]);
  });
  it("abre a escala quando todos os valores são iguais", () => {
    expect(escalaComum([86, 86])!.dominio).toEqual([85, 87]);
  });
  it("não inventa escala sem dados", () => {
    expect(escalaComum([])).toBeUndefined();
  });
});
