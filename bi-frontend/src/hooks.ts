import { useCallback, useEffect, useRef, useState } from "react";
import { API_URL, buscar, EventoStream } from "./api";

export interface Consulta<T> {
  dados: T | null;
  erro: string | null;
  carregando: boolean;
  recarregar: () => void;
}

/** Busca na API e, se `atualizarMs` for dado, repete. Mantém o dado antigo enquanto recarrega
 *  e se o refresh falhar (o erro aparece, mas a tela não esvazia). `null` em params = não buscar. */
export function useApi<T>(caminho: string, params: Record<string, string | number | undefined | null> | null,
                          atualizarMs?: number): Consulta<T> {
  const [dados, setDados] = useState<T | null>(null);
  const [erro, setErro] = useState<string | null>(null);
  const [carregando, setCarregando] = useState(params !== null);
  const [tick, setTick] = useState(0);
  const chave = JSON.stringify(params);

  useEffect(() => {
    if (params === null) {
      setDados(null);
      setCarregando(false);
      return;
    }
    const ctl = new AbortController();
    let vivo = true;
    const rodar = (primeira: boolean) => {
      if (primeira) setCarregando(true);
      buscar<T>(caminho, params, ctl.signal)
        .then((d) => vivo && (setDados(d), setErro(null)))
        .catch((e: Error) => vivo && e.name !== "AbortError" && setErro(e.message))
        .finally(() => vivo && setCarregando(false));
    };
    rodar(true);
    const id = atualizarMs ? setInterval(() => rodar(false), atualizarMs) : undefined;
    return () => {
      vivo = false;
      ctl.abort();
      if (id) clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [caminho, chave, atualizarMs, tick]);

  return { dados, erro, carregando, recarregar: useCallback(() => setTick((t) => t + 1), []) };
}

export type EstadoConexao = "conectando" | "conectado" | "reconectando" | "fora";

export interface EstadoAoVivo {
  eventos: EventoStream[];          // do mais antigo para o mais novo; sem repetidos
  conexao: EstadoConexao;
  erroServidor: string | null;      // a API está de pé, mas a consulta ao BigQuery falhou
  ultimoBatimento: number | null;   // epoch ms do último "ok" (ou volta) recebido
  ultimaVolta: number | null;       // epoch ms em que a última volta CHEGOU aqui
}

const MAX_EVENTOS = 600;
const TENTATIVAS_ATE_FORA = 3;

/** Uma conexão SSE com a API de BI. Reconecta sozinho; depois de algumas falhas seguidas
 *  declara "fora" mas continua tentando. Eventos repetidos (reconexão) são descartados por id. */
export function useAoVivo(): EstadoAoVivo {
  const [estado, setEstado] = useState<EstadoAoVivo>({
    eventos: [], conexao: "conectando", erroServidor: null, ultimoBatimento: null, ultimaVolta: null,
  });
  const vistos = useRef(new Set<string>());

  useEffect(() => {
    let es: EventSource | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let falhas = 0;
    let fechado = false;

    const conectar = () => {
      es = new EventSource(`${API_URL}/api/stream/sse`);
      es.onopen = () => {
        falhas = 0;
        setEstado((s) => ({ ...s, conexao: "conectado" }));
      };
      es.addEventListener("volta", (m) => {
        const ev = JSON.parse((m as MessageEvent).data) as EventoStream;
        if (vistos.current.has(ev.evento_id)) return;
        vistos.current.add(ev.evento_id);
        const agora = Date.now();
        setEstado((s) => ({
          ...s, conexao: "conectado", erroServidor: null, ultimoBatimento: agora, ultimaVolta: agora,
          eventos: [...s.eventos, ev].slice(-MAX_EVENTOS),
        }));
      });
      es.addEventListener("ok", () =>
        setEstado((s) => ({ ...s, conexao: "conectado", erroServidor: null, ultimoBatimento: Date.now() })));
      es.addEventListener("erro", (m) => {
        const msg = JSON.parse((m as MessageEvent).data).mensagem as string;
        setEstado((s) => ({ ...s, erroServidor: msg }));
      });
      es.onerror = () => {
        falhas += 1;
        setEstado((s) => ({ ...s, conexao: falhas >= TENTATIVAS_ATE_FORA ? "fora" : "reconectando" }));
        // CLOSED = o navegador desistiu (ex.: HTTP 5xx). Recriamos nós mesmos.
        if (es && es.readyState === EventSource.CLOSED && !fechado) {
          es.close();
          timer = setTimeout(conectar, 3000);
        }
      };
    };
    conectar();
    return () => {
      fechado = true;
      clearTimeout(timer);
      es?.close();
    };
  }, []);

  return estado;
}

/** Re-renderiza a cada `ms`, para textos como "há 12 s". */
export function useRelogio(ms = 1000): number {
  const [agora, setAgora] = useState(Date.now());
  useEffect(() => {
    const id = setInterval(() => setAgora(Date.now()), ms);
    return () => clearInterval(id);
  }, [ms]);
  return agora;
}
