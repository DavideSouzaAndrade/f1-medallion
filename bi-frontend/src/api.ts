// Tipos espelham bi-api/schemas.py. O navegador só conhece esta API, nunca o BigQuery.

export const API_URL: string = import.meta.env.VITE_API_URL ?? "";

export type Origem = "batch" | "stream";

export interface Sessao {
  session_key: number;
  year: number;
  session_name: string | null;
  nome_evento: string | null;
  circuito: string | null;
  circuit_type: string | null;
  pais: string | null;
  eh_corrida: boolean;
}

export interface Piloto {
  driver_number: number;
  sigla: string | null;
  nome_completo: string | null;
  equipe: string | null;
}

export interface Volta {
  origem: Origem;
  session_key: number;
  driver_number: number;
  lap_number: number;
  duracao_s: number | null;
  composto: string | null;
  idade_pneu: number | null;
  eh_saida_box: boolean | null;
  eh_entrada_box: boolean | null;
}

export interface PontoDegradacao {
  composto: string;
  idade_pneu: number;
  tempo_medio_s: number;
  voltas: number;
}

export interface ResumoOrigem {
  origem: Origem;
  voltas: number;
  sessoes: number;
  primeira_volta: string | null;
  ultima_volta: string | null;
}

export interface ResumoStream {
  total_eventos: number;
  ultimo_evento: string | null;
  aptos_modelo: number;
  com_predicao: number;
  latencia_p50_ms: number | null;
  latencia_p95_ms: number | null;
  dlq_total: number;
  dlq_por_motivo: { motivo: string; eventos: number }[];
  por_minuto: { minuto: string; eventos: number }[];
}

export interface Replay {
  replay_id: string;
  session_key: number;
  circuito: string | null;
  eventos: number;
  ultimo_evento: string;
}

export interface EventoStream {
  evento_id: string;
  replay_id: string;
  ts_evento: string;
  ts_processamento: string;
  session_key: number;
  driver_number: number;
  piloto: string;
  lap_number: number;
  circuito: string | null;
  composto: string | null;
  duracao_s: number | null;
  eh_saida_box: boolean;
  eh_entrada_box: boolean;
  apto_modelo: boolean;
  latencia_ms: number | null;
  tempo_previsto_s: number | null;
  modelo_versao: string | null;
}

export interface ParPrevistoReal {
  evento_id: string;
  replay_id: string;
  session_key: number;
  driver_number: number;
  piloto: string;
  lap_number: number;
  composto: string | null;
  duracao_s: number;
  tempo_previsto_s: number;
  erro_s: number;
}

export interface PrevistoReal {
  metricas: { voltas: number; mae_s: number | null; vies_s: number | null };
  pares: ParPrevistoReal[];
  aguardando_predicao: number;
}

export interface StatusPipeline {
  status: "ok" | "falha";
  verificado_em: string;
  checagens: { nome: string; estado: "ok" | "falha" | "pendente"; detalhe: string }[];
}

export interface Saude {
  status: string;
  modo: "mock" | "bigquery";
}

type Params = Record<string, string | number | undefined | null>;

export class ErroApi extends Error {}

export async function buscar<T>(caminho: string, params: Params = {}, sinal?: AbortSignal): Promise<T> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== "") qs.set(k, String(v));
  const url = `${API_URL}${caminho}${qs.size ? `?${qs}` : ""}`;
  let resp: Response;
  try {
    resp = await fetch(url, { signal: sinal });
  } catch (e) {
    if ((e as Error).name === "AbortError") throw e;
    throw new ErroApi("Não foi possível falar com a API de BI. Ela pode estar fora do ar.");
  }
  if (!resp.ok) {
    const detalhe = await resp.json().then((j) => j.detail).catch(() => null);
    if (typeof detalhe === "string") throw new ErroApi(detalhe);
    // Sem corpo da API: quem respondeu foi um proxy/gateway, ou ela caiu.
    throw new ErroApi(resp.status >= 500
      ? `A API de BI não respondeu (HTTP ${resp.status}). Ela pode estar fora do ar.`
      : `A API recusou a consulta (HTTP ${resp.status}).`);
  }
  return resp.json() as Promise<T>;
}
