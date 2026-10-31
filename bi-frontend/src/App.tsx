import { useEffect, useState } from "react";
import { Saude as DadosSaude } from "./api";
import { Faixa, Selo } from "./components/ui";
import { EstadoConexao, useApi, useAoVivo } from "./hooks";
import AoVivo from "./pages/AoVivo";
import Historico from "./pages/Historico";
import PrevistoReal from "./pages/PrevistoReal";
import Saude from "./pages/Saude";

const ABAS = [
  { id: "historico", rotulo: "Histórico" },
  { id: "aovivo", rotulo: "Ao vivo" },
  { id: "previsto", rotulo: "Previsto × Real" },
  { id: "saude", rotulo: "Saúde do pipeline" },
] as const;
type Aba = (typeof ABAS)[number]["id"];

const ROTULO_CONEXAO: Record<EstadoConexao, { tipo: "ok" | "espera" | "falha"; texto: string }> = {
  conectado: { tipo: "ok", texto: "Tempo real conectado" },
  conectando: { tipo: "espera", texto: "Conectando…" },
  reconectando: { tipo: "espera", texto: "Reconectando…" },
  fora: { tipo: "falha", texto: "Tempo real fora do ar" },
};

function abaInicial(): Aba {
  const h = window.location.hash.slice(1);
  return ABAS.some((a) => a.id === h) ? (h as Aba) : "historico";
}

function useTema() {
  const [tema, setTema] = useState<"light" | "dark" | null>(() => {
    try { return (localStorage.getItem("tema") as "light" | "dark" | null) ?? null; } catch { return null; }
  });
  useEffect(() => {
    if (tema) document.documentElement.dataset.theme = tema;
    else delete document.documentElement.dataset.theme;
    try { tema ? localStorage.setItem("tema", tema) : localStorage.removeItem("tema"); } catch { /* sem storage: segue o sistema */ }
  }, [tema]);
  const escuro = tema ? tema === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  return { escuro, alternar: () => setTema(escuro ? "light" : "dark") };
}

export default function App() {
  const [aba, setAba] = useState<Aba>(abaInicial);
  const vivo = useAoVivo();                 // uma conexão SSE para o app todo
  const saude = useApi<DadosSaude>("/api/health", {}, 15000);
  const tema = useTema();

  useEffect(() => { window.location.hash = aba; }, [aba]);
  const conexao = ROTULO_CONEXAO[vivo.conexao];

  return (
    <div className="app">
      <div className="topo">
        <h1>F1 · BI de voltas</h1>
        <div className="direita">
          <Selo tipo={conexao.tipo}>{conexao.texto}</Selo>
          <button className="btn" onClick={tema.alternar} aria-label="Alternar tema claro/escuro">
            {tema.escuro ? "☀ Claro" : "☾ Escuro"}
          </button>
        </div>
      </div>

      <div role="tablist" className="abas">
        {ABAS.map((a) => (
          <button key={a.id} role="tab" className="aba" aria-selected={aba === a.id} onClick={() => setAba(a.id)}>
            {a.rotulo}
          </button>
        ))}
      </div>

      {saude.erro && !saude.dados && <Faixa tipo="erro" onRetry={saude.recarregar}>{saude.erro}</Faixa>}
      {saude.dados?.modo === "mock" && (
        <Faixa tipo="aviso">DADOS SIMULADOS: a API de BI está em modo mock e não está lendo o BigQuery.</Faixa>
      )}

      <main role="tabpanel">
        {aba === "historico" && <Historico />}
        {aba === "aovivo" && <AoVivo vivo={vivo} />}
        {aba === "previsto" && <PrevistoReal />}
        {aba === "saude" && <Saude />}
      </main>
    </div>
  );
}
