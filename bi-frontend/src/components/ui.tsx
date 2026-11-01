import { ReactNode, useState } from "react";
import { Consulta } from "../hooks";

export function Kpi({ rotulo, valor, detalhe }: { rotulo: string; valor: ReactNode; detalhe?: ReactNode }) {
  return (
    <div className="kpi">
      <div className="rotulo">{rotulo}</div>
      <div className="valor">{valor}</div>
      {detalhe && <div className="detalhe">{detalhe}</div>}
    </div>
  );
}

/** Marca a origem do dado. Ícone (ponto) + texto: a cor nunca sozinha. */
export function SeloOrigem({ origem }: { origem: "batch" | "stream" }) {
  return (
    <span className={`selo ${origem}`}>
      <i />
      {origem === "batch" ? "HISTÓRICO · batch" : "AO VIVO · stream"}
    </span>
  );
}

export function Selo({ tipo, children }: { tipo: "ok" | "espera" | "falha"; children: ReactNode }) {
  return (
    <span className={`selo ${tipo}`} role="status">
      <i />
      {children}
    </span>
  );
}

export function Vazio({ titulo, children }: { titulo: string; children?: ReactNode }) {
  return (
    <div className="vazio">
      <strong>{titulo}</strong>
      {children}
    </div>
  );
}

export function Faixa({ tipo, children, onRetry }: { tipo: "aviso" | "erro"; children: ReactNode; onRetry?: () => void }) {
  return (
    <div className={`faixa ${tipo}`} role={tipo === "erro" ? "alert" : "status"}>
      <span aria-hidden>{tipo === "erro" ? "✕" : "⚠"}</span>
      <span>{children}</span>
      {onRetry && <button className="btn" onClick={onRetry}>Tentar de novo</button>}
    </div>
  );
}

/** Estados de uma consulta: carregando, erro (com a tela anterior preservada, se houver) e vazio. */
export function Carga<T>({ q, vazio, ehVazio, children }: {
  q: Consulta<T>;
  vazio: ReactNode;
  ehVazio?: (d: T) => boolean;
  children: (dados: T) => ReactNode;
}) {
  if (!q.dados) {
    if (q.erro) return <Faixa tipo="erro" onRetry={q.recarregar}>{q.erro}</Faixa>;
    return <div className="esqueleto" aria-busy="true" aria-label="Carregando" />;
  }
  const corpo = (ehVazio ?? ((d: T) => Array.isArray(d) && d.length === 0))(q.dados) ? vazio : children(q.dados);
  return (
    <>
      {q.erro && <Faixa tipo="erro" onRetry={q.recarregar}>Falha ao atualizar: {q.erro} Mostrando o último dado recebido.</Faixa>}
      {corpo}
    </>
  );
}

export interface Tabela {
  colunas: { chave: string; rotulo: string; numerica?: boolean }[];
  linhas: Record<string, ReactNode>[];
}

export function TabelaDados({ colunas, linhas }: Tabela) {
  return (
    <div className="tabela-rolagem">
      <table>
        <thead>
          <tr>{colunas.map((c) => <th key={c.chave} className={c.numerica ? "num" : ""}>{c.rotulo}</th>)}</tr>
        </thead>
        <tbody>
          {linhas.map((l, i) => (
            <tr key={i}>{colunas.map((c) => <td key={c.chave} className={c.numerica ? "num" : ""}>{l[c.chave]}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Cartão de gráfico. Com `tabela`, oferece a mesma informação como tabela (acessibilidade). */
export function Cartao({ titulo, sub, selo, tabela, children }: {
  titulo: string; sub?: string; selo?: ReactNode; tabela?: Tabela; children: ReactNode;
}) {
  const [modoTabela, setModoTabela] = useState(false);
  return (
    <section className="cartao">
      <header>
        <h2>{titulo}</h2>
        {sub && <span className="sub">{sub}</span>}
        <div className="acoes">
          {selo}
          {tabela && (
            <button className="btn" aria-pressed={modoTabela} onClick={() => setModoTabela(!modoTabela)}>
              {modoTabela ? "Ver gráfico" : "Ver tabela"}
            </button>
          )}
        </div>
      </header>
      {modoTabela && tabela ? <TabelaDados {...tabela} /> : children}
    </section>
  );
}

export function Legenda({ itens }: { itens: { rotulo: string; cor: string }[] }) {
  return (
    <div className="legenda">
      {itens.map((i) => <span key={i.rotulo}><i style={{ background: i.cor }} />{i.rotulo}</span>)}
    </div>
  );
}

interface TipProps {
  active?: boolean;
  label?: string | number;
  payload?: { name?: string; value?: number | string; color?: string; dataKey?: string | number }[];
  titulo?: (label: string | number) => string;
  valor?: (v: number | string, nome: string) => string;
}

/** Tooltip único para os gráficos de linha/barra. */
export function Tip({ active, label, payload, titulo, valor }: TipProps) {
  if (!active || !payload?.length) return null;
  return (
    <div className="tip">
      <b>{titulo ? titulo(label ?? "") : label}</b>
      {payload.filter((p) => p.value != null).map((p) => (
        <div key={String(p.dataKey)}>
          <span><i style={{ background: p.color }} />{p.name}</span>
          <span>{valor ? valor(p.value as number, p.name ?? "") : p.value}</span>
        </div>
      ))}
    </div>
  );
}
