import { describe, expect, it } from "vitest";
import type { Replay } from "./api";
import { completa, execucaoPadrao, rotuloExecucao } from "./execucoes";

const exec = (replay_id: string, eventos: number, extra: Partial<Replay> = {}): Replay => ({
  replay_id, session_key: 11253, circuito: "Suzuka", eventos, ultimo_evento: "2026-11-01T12:00:00Z",
  aptos_modelo: eventos, com_predicao: eventos, voltas_na_corrida: 1108, fonte: "replay", em_curso: false, ...extra,
});

describe("rotuloExecucao", () => {
  it("mostra quantas voltas da corrida a execução tem", () => {
    expect(rotuloExecucao(exec("r1", 151))).toBe("r1 · Suzuka · 151 de 1.108 voltas");
    expect(rotuloExecucao(exec("r2", 40, { em_curso: true }))).toBe("r2 · Suzuka · 40 de 1.108 voltas · em curso");
    expect(rotuloExecucao(exec("r3", 7, { voltas_na_corrida: null, circuito: null }))).toBe("r3 · 11253 · 7 voltas");
  });
});

describe("completa", () => {
  it("tolera a fração que vai para a DLQ, mas não uma execução cortada", () => {
    expect(completa(exec("r", 1097))).toBe(true);   // 99%
    expect(completa(exec("r", 151))).toBe(false);
    expect(completa(exec("r", 500, { voltas_na_corrida: null }))).toBe(false);
  });
});

describe("execucaoPadrao", () => {
  const teste = exec("teste-6-voltas", 120);
  const inteira = exec("corrida-inteira", 1100);

  it("pula um teste curto já encerrado e fica com a completa", () => {
    expect(execucaoPadrao([teste, inteira])).toBe("corrida-inteira");
  });

  it("fica com a execução em curso, mesmo com poucas voltas", () => {
    const aoVivo = exec("ao-vivo", 151, { em_curso: true });
    expect(execucaoPadrao([aoVivo, inteira])).toBe("ao-vivo");
  });

  it("não sai da corrida ao vivo quando ela passa um tempo sem volta", () => {
    const pausada = exec("ao-vivo", 300);   // em_curso virou false por um instante
    expect(execucaoPadrao([pausada, inteira], "ao-vivo")).toBe("ao-vivo");
  });

  it("troca para uma execução nova que começou com a tela aberta", () => {
    const nova = exec("nova", 20, { em_curso: true });
    expect(execucaoPadrao([nova, inteira], "corrida-inteira")).toBe("nova");
  });

  it("sem nenhuma completa ou em curso, mostra a mais recente", () => {
    expect(execucaoPadrao([teste, exec("outro-teste", 50)])).toBe("teste-6-voltas");
    expect(execucaoPadrao([])).toBeUndefined();
  });

  it("devolve a mesma resposta quando recebe a própria resposta", () => {
    for (const lista of [[teste, inteira], [teste], [exec("ao-vivo", 9, { em_curso: true }), inteira]]) {
      const r = execucaoPadrao(lista);
      expect(execucaoPadrao(lista, r)).toBe(r);
    }
  });
});
