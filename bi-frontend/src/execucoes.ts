// Qual execução do gerador mostrar, e como descrevê-la no seletor.
import type { Replay } from "./api";
import { fmtInt } from "./format";

/** Parte das voltas da corrida que uma execução precisa ter gravado para contar como completa.
 *  Não é 100%: o gerador manda ~1% para a DLQ de propósito. */
export const FRACAO_COMPLETA = 0.9;

export function completa(r: Replay): boolean {
  return !!r.voltas_na_corrida && r.eventos >= FRACAO_COMPLETA * r.voltas_na_corrida;
}

/** "replay-x · Suzuka · 151 de 1.108 voltas · em curso": dá para ver qual execução é a completa. */
export function rotuloExecucao(r: Replay): string {
  const voltas = r.voltas_na_corrida
    ? `${fmtInt(r.eventos)} de ${fmtInt(r.voltas_na_corrida)} voltas`
    : `${fmtInt(r.eventos)} voltas`;
  return [r.replay_id, r.circuito ?? String(r.session_key), voltas, r.em_curso ? "em curso" : null]
    .filter(Boolean).join(" · ");
}

/**
 * A execução que a tela mostra quando ninguém escolheu outra.
 *
 * Antes era sempre a mais recente. Se a mais recente fosse um teste de 6 voltas, a previsão saía quase
 * constante (as entradas do modelo mal variam em 6 voltas) e o modelo parecia quebrado. Agora a tela
 * pula execuções que TERMINARAM pela metade. Uma em curso nunca é pulada: na apresentação, a corrida
 * em tempo real ainda está no meio quando a tela é aberta, e é ela que interessa.
 *
 * `atual` é a que já está na tela. Ela só é trocada por uma execução MAIS RECENTE que sirva. Sem isso,
 * a corrida ao vivo, ao passar 2 min sem volta e deixar de constar como em curso por um instante,
 * faria a tela pular para uma corrida antiga no meio da apresentação.
 *
 * `replays` vem do mais recente para o mais antigo, como a API devolve.
 */
export function execucaoPadrao(replays: Replay[], atual?: string | null): string | undefined {
  const melhor = replays.findIndex((r) => r.em_curso || completa(r));
  const iAtual = atual ? replays.findIndex((r) => r.replay_id === atual) : -1;
  if (iAtual >= 0 && (melhor < 0 || iAtual <= melhor)) return replays[iAtual].replay_id;
  return replays[Math.max(melhor, 0)]?.replay_id;
}
