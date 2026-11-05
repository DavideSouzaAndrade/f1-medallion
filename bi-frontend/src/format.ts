const nf = (casas: number) => new Intl.NumberFormat("pt-BR", { minimumFractionDigits: casas, maximumFractionDigits: casas });

export const fmtInt = (n: number | null | undefined) => (n == null ? "—" : nf(0).format(n));
export const fmtDec = (n: number | null | undefined, casas = 3) => (n == null ? "—" : nf(casas).format(n));

/** 83.412 -> "1:23.412", como o cronômetro mostra. */
export function fmtVolta(s: number | null | undefined): string {
  if (s == null) return "—";
  const m = Math.floor(s / 60);
  return `${m}:${(s - m * 60).toFixed(3).padStart(6, "0")}`;
}

/** 117 -> "117 ms", 2200 -> "2,2 s". Latências de fila acumulada podem passar de horas. */
export function fmtMs(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  if (s < 60) return `${nf(1).format(s)} s`;
  if (s < 3600) return `${nf(1).format(s / 60)} min`;
  if (s < 86400) return `${nf(1).format(s / 3600)} h`;
  return `${nf(1).format(s / 86400)} d`;
}

export function fmtHora(iso: string | null | undefined): string {
  return iso ? new Date(iso).toLocaleTimeString("pt-BR") : "—";
}

/**
 * "há 12 s" a partir do TIMESTAMP DO EVENTO, não de quando ele chegou aqui.
 *
 * A diferença não é sutil: o SSE reenvia o buffer a cada reconexão, então
 * medir desde a chegada fazia o contador reiniciar a cada F5 e um evento de
 * uma hora atrás aparecer como "chegou há 6 s" — a tela afirmava que o dado
 * era fresco quando não era.
 */
export function fmtHaData(iso: string | null | undefined, agoraMs: number): string {
  if (!iso) return "—";
  return fmtHa(new Date(iso).getTime(), agoraMs);
}

/** "há 12 s" / "há 3 min". */
export function fmtHa(desdeMs: number | null, agoraMs: number): string {
  if (desdeMs == null) return "—";
  const s = Math.max(0, Math.round((agoraMs - desdeMs) / 1000));
  return s < 60 ? `há ${s} s` : s < 3600 ? `há ${Math.floor(s / 60)} min` : `há ${Math.floor(s / 3600)} h`;
}

export const rotuloPiloto = (sigla: string | null, numero: number) => sigla ?? String(numero);

/** Cor por posição na lista de pilotos da sessão: o piloto mantém a cor ao filtrar. */
export const corSerie = (i: number) => `var(--s${(i % 5) + 1})`;

export const COMPOSTOS = ["SOFT", "MEDIUM", "HARD"] as const;
export const corComposto = (c: string) => `var(--s${Math.max(0, COMPOSTOS.indexOf(c as never)) + 1})`;

/** Escala do eixo de tempo ignorando voltas extremas (safety car, bandeira vermelha), que achatariam
 *  o ritmo normal. Os pontos fora da escala continuam nos dados e no tooltip. */
export function dominioRobusto(tempos: number[]): [number, number] | undefined {
  if (tempos.length === 0) return undefined;
  const ordenado = [...tempos].sort((a, b) => a - b);
  const mediana = ordenado[Math.floor(ordenado.length / 2)];
  const normais = ordenado.filter((t) => t >= mediana * 0.9 && t <= mediana * 1.12);
  return [Math.floor(normais[0] - 0.5), Math.ceil(normais[normais.length - 1] + 1)];
}

/**
 * Uma escala só para os DOIS eixos do gráfico previsto × real.
 *
 * Com escalas independentes, cada eixo se ajustava à própria faixa (real de 1:33 a 1:50, previsto de
 * 1:35 a 1:39): a diagonal y = x saía da área visível e um modelo que prevê quase sempre o mesmo
 * valor parecia razoável. Com a mesma escala, a diagonal vai de canto a canto e esse modelo aparece
 * como uma faixa horizontal, que é o que ele é. Não corta extremos (safety car): aqui eles são
 * justamente o erro que o gráfico precisa mostrar.
 */
export function escalaComum(valores: number[]): { dominio: [number, number]; ticks: number[] } | undefined {
  const v = valores.filter(Number.isFinite);
  if (v.length === 0) return undefined;
  const min = Math.min(...v), max = Math.max(...v);
  const passo = [1, 2, 5, 10, 15, 30, 60].find((p) => (max - min) / p <= 5) ?? 120;
  let lo = Math.floor(min / passo) * passo, hi = Math.ceil(max / passo) * passo;
  if (lo === hi) { lo -= passo; hi += passo; }
  const ticks: number[] = [];
  for (let t = lo; t <= hi; t += passo) ticks.push(t);
  return { dominio: [lo, hi], ticks };
}
