import { useEffect, useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Piloto, PontoDegradacao, Sessao, Volta } from "../api";
import { Cartao, Carga, Kpi, Legenda, SeloOrigem, Tip, Vazio } from "../components/ui";
import { COMPOSTOS, corComposto, corSerie, dominioRobusto, fmtInt, fmtVolta, rotuloPiloto } from "../format";
import { useApi } from "../hooks";

const MAX_PILOTOS = 5;
const dominioTempo = [(min: number) => Math.floor(min - 1), (max: number) => Math.ceil(max + 1)] as [
  (min: number) => number, (max: number) => number];

export default function Historico() {
  const sessoes = useApi<Sessao[]>("/api/sessoes", {});
  const [sessao, setSessao] = useState<number | null>(null);
  const [escolhidos, setEscolhidos] = useState<number[] | null>(null);   // null = os 3 primeiros

  useEffect(() => {
    if (sessao === null && sessoes.dados?.length) setSessao(sessoes.dados[0].session_key);
  }, [sessoes.dados, sessao]);

  const pilotos = useApi<Piloto[]>(sessao ? `/api/sessoes/${sessao}/pilotos` : "/api/sessoes", sessao ? {} : null);
  const voltas = useApi<Volta[]>("/api/voltas", sessao ? { session_key: sessao, origem: "batch" } : null);
  const degradacao = useApi<PontoDegradacao[]>("/api/degradacao", sessao ? { session_key: sessao } : null);

  const listaPilotos = pilotos.dados ?? [];
  const ativos = escolhidos ?? listaPilotos.slice(0, 3).map((p) => p.driver_number);
  const nome = (n: number) => rotuloPiloto(listaPilotos.find((p) => p.driver_number === n)?.sigla ?? null, n);
  const cor = (n: number) => corSerie(listaPilotos.findIndex((p) => p.driver_number === n));

  const alternar = (n: number) => {
    const novo = ativos.includes(n) ? ativos.filter((x) => x !== n) : [...ativos, n].slice(-MAX_PILOTOS);
    setEscolhidos(novo);
  };

  // Uma linha por volta, uma coluna por piloto. Voltas de box ficam de fora (buraco na linha):
  // são 15–20 s mais lentas e esmagariam a escala.
  const ritmo = useMemo(() => {
    const porVolta = new Map<number, Record<string, number | null>>();
    for (const v of voltas.dados ?? []) {
      if (!ativos.includes(v.driver_number)) continue;
      const linha = porVolta.get(v.lap_number) ?? { volta: v.lap_number };
      linha[`d${v.driver_number}`] = v.duracao_s != null && !v.eh_entrada_box && !v.eh_saida_box ? v.duracao_s : null;
      porVolta.set(v.lap_number, linha);
    }
    return [...porVolta.values()].sort((a, b) => (a.volta as number) - (b.volta as number));
  }, [voltas.dados, ativos.join(",")]);   // eslint-disable-line react-hooks/exhaustive-deps

  const escalaRitmo = useMemo(
    () => dominioRobusto(ritmo.flatMap((l) => Object.entries(l).filter(([k, v]) => k !== "volta" && v != null).map(([, v]) => v as number))),
    [ritmo]);

  const curva = useMemo(() => {
    const porIdade = new Map<number, Record<string, number>>();
    for (const p of degradacao.dados ?? []) {
      const linha = porIdade.get(p.idade_pneu) ?? { idade: p.idade_pneu };
      linha[p.composto] = p.tempo_medio_s;
      porIdade.set(p.idade_pneu, linha);
    }
    return [...porIdade.values()].sort((a, b) => a.idade - b.idade);
  }, [degradacao.dados]);

  const melhor = useMemo(() => {
    const limpas = (voltas.dados ?? []).filter((v) => v.duracao_s != null && !v.eh_entrada_box && !v.eh_saida_box);
    return limpas.length ? limpas.reduce((a, b) => (b.duracao_s! < a.duracao_s! ? b : a)) : null;
  }, [voltas.dados]);

  return (
    <>
      <div className="filtros">
        <label>
          Corrida
          <select value={sessao ?? ""} onChange={(e) => { setSessao(Number(e.target.value)); setEscolhidos(null); }}
                  disabled={!sessoes.dados?.length}>
            {(sessoes.dados ?? []).map((s) => (
              <option key={s.session_key} value={s.session_key}>
                {s.nome_evento ?? s.circuito} · {s.session_name} ({s.year})
              </option>
            ))}
          </select>
        </label>
        <div>
          <div style={{ fontSize: 12, color: "var(--ink-2)", marginBottom: 3 }}>Pilotos (até {MAX_PILOTOS})</div>
          <div className="chips">
            {listaPilotos.map((p) => (
              <button key={p.driver_number} className="chip" aria-pressed={ativos.includes(p.driver_number)}
                      onClick={() => alternar(p.driver_number)} title={p.nome_completo ?? undefined}>
                <i style={{ background: cor(p.driver_number) }} />
                {rotuloPiloto(p.sigla, p.driver_number)}
              </button>
            ))}
          </div>
        </div>
      </div>

      <Carga q={sessoes} vazio={<Vazio titulo="Nenhuma corrida com dados">A Silver não tem sessões com voltas registradas.</Vazio>}>
        {() => (
          <>
            <div className="kpis">
              <Kpi rotulo="Voltas registradas" valor={fmtInt(voltas.dados?.length)} detalhe="todos os pilotos da corrida" />
              <Kpi rotulo="Melhor volta" valor={fmtVolta(melhor?.duracao_s)}
                   detalhe={melhor ? `${nome(melhor.driver_number)} · volta ${melhor.lap_number}` : "—"} />
              <Kpi rotulo="Pilotos" valor={fmtInt(listaPilotos.length)} />
            </div>

            <div className="grade dois">
              <Cartao titulo="Ritmo por volta" sub="voltas de box omitidas; voltas muito lentas (safety car) saem da escala" selo={<SeloOrigem origem="batch" />}
                      tabela={{
                        colunas: [{ chave: "volta", rotulo: "Volta", numerica: true },
                          ...ativos.map((n) => ({ chave: `d${n}`, rotulo: nome(n), numerica: true }))],
                        linhas: ritmo.map((l) => Object.fromEntries(Object.entries(l).map(([k, v]) =>
                          [k, k === "volta" ? v : fmtVolta(v as number | null)]))),
                      }}>
                <Carga q={voltas} vazio={<Vazio titulo="Sem voltas para esta corrida" />}>
                  {() => ativos.length === 0 ? <Vazio titulo="Escolha ao menos um piloto" /> : (
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={ritmo} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                        <CartesianGrid vertical={false} />
                        <XAxis dataKey="volta" tickLine={false} />
                        <YAxis domain={escalaRitmo ?? dominioTempo} allowDataOverflow tickFormatter={fmtVolta} width={64} tickLine={false} />
                        <Tooltip content={<Tip titulo={(l) => `Volta ${l}`} valor={(v) => fmtVolta(v as number)} />} />
                        {ativos.map((n) => (
                          <Line key={n} dataKey={`d${n}`} name={nome(n)} stroke={cor(n)} strokeWidth={2}
                                dot={false} activeDot={{ r: 5 }} connectNulls={false} isAnimationActive={false} />
                        ))}
                      </LineChart>
                    </ResponsiveContainer>
                  )}
                </Carga>
              </Cartao>

              <Cartao titulo="Degradação do pneu" sub="tempo médio por idade do pneu (Gold)" selo={<SeloOrigem origem="batch" />}
                      tabela={{
                        colunas: [{ chave: "idade", rotulo: "Idade (voltas)", numerica: true },
                          ...COMPOSTOS.map((c) => ({ chave: c, rotulo: c, numerica: true }))],
                        linhas: curva.map((l) => Object.fromEntries(Object.entries(l).map(([k, v]) =>
                          [k, k === "idade" ? v : fmtVolta(v)]))),
                      }}>
                <Carga q={degradacao} vazio={<Vazio titulo="Sem dados de degradação" >A Gold não tem voltas limpas desta corrida.</Vazio>}>
                  {() => (
                    <>
                      <ResponsiveContainer width="100%" height={280}>
                        <LineChart data={curva} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                          <CartesianGrid vertical={false} />
                          <XAxis dataKey="idade" tickLine={false} />
                          <YAxis domain={dominioTempo} tickFormatter={fmtVolta} width={64} tickLine={false} />
                          <Tooltip content={<Tip titulo={(l) => `Pneu com ${l} voltas`} valor={(v) => fmtVolta(v as number)} />} />
                          {COMPOSTOS.map((c) => (
                            <Line key={c} dataKey={c} name={c} stroke={corComposto(c)} strokeWidth={2}
                                  dot={{ r: 2 }} activeDot={{ r: 5 }} connectNulls isAnimationActive={false} />
                          ))}
                        </LineChart>
                      </ResponsiveContainer>
                      <Legenda itens={COMPOSTOS.map((c) => ({ rotulo: c, cor: corComposto(c) }))} />
                    </>
                  )}
                </Carga>
              </Cartao>
            </div>
          </>
        )}
      </Carga>
    </>
  );
}
