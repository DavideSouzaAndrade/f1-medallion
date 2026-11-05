import { useMemo, useState } from "react";
import {
  CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis,
} from "recharts";
import { PrevistoReal as Dados, Replay } from "../api";
import { Cartao, Carga, Faixa, Kpi, Legenda, SeloOrigem, Tip, Vazio } from "../components/ui";
import { completa, execucaoPadrao, rotuloExecucao } from "../execucoes";
import { escalaComum, fmtDec, fmtInt, fmtVolta } from "../format";
import { useApi } from "../hooks";

const dominio = [(min: number) => Math.floor(min - 1), (max: number) => Math.ceil(max + 1)] as [
  (min: number) => number, (max: number) => number];

export default function PrevistoReal() {
  const replays = useApi<Replay[]>("/api/stream/replays", {}, 15000);
  const [escolhido, setEscolhido] = useState<string | null>(null);   // escolha da pessoa no seletor
  // Sem escolha, a mais recente que esteja em curso ou completa (ver execucaoPadrao). Guarda a anterior
  // para não trocar de corrida sozinha; o ajuste durante a renderização é o padrão do React para isso.
  const [automatico, setAutomatico] = useState<string | undefined>();
  const padrao = execucaoPadrao(replays.dados ?? [], automatico);
  if (padrao !== automatico) setAutomatico(padrao);
  const replayId = escolhido ?? padrao;
  const meta = replays.dados?.find((r) => r.replay_id === replayId);
  const parcial = meta && !meta.em_curso && !completa(meta) ? meta : null;
  const completaRecente = replays.dados?.find(completa);

  // Só consulta depois de saber qual execução do gerador mostrar.
  const q = useApi<Dados>("/api/previsto-real", replays.dados ? { replay_id: replayId, limite: 400 } : null, 5000);
  const [pilotoEscolhido, setPilotoEscolhido] = useState<string | null>(null);

  const pares = q.dados?.pares ?? [];
  const pilotos = useMemo(() => [...new Set(pares.map((p) => p.piloto))].sort(), [pares]);
  const piloto = pilotoEscolhido && pilotos.includes(pilotoEscolhido) ? pilotoEscolhido : pilotos[0];

  // Real e previsto saem da MESMA linha do stream: a chave (corrida, piloto, volta) já os casa.
  const porVolta = useMemo(() => pares.filter((p) => p.piloto === piloto)
    .map((p) => ({ volta: p.lap_number, real: p.duracao_s, previsto: p.tempo_previsto_s }))
    .sort((a, b) => a.volta - b.volta), [pares, piloto]);
  const dispersao = useMemo(() => pares.map((p) => ({ real: p.duracao_s, previsto: p.tempo_previsto_s })), [pares]);
  const escala = useMemo(() => escalaComum(dispersao.flatMap((d) => [d.real, d.previsto])), [dispersao]);
  const faixas = useMemo(() => {
    const ext = (vs: number[]) => (vs.length ? `${fmtVolta(Math.min(...vs))} a ${fmtVolta(Math.max(...vs))}` : "—");
    return { real: ext(dispersao.map((d) => d.real)), previsto: ext(dispersao.map((d) => d.previsto)) };
  }, [dispersao]);

  return (
    <>
      <div className="filtros">
        <label>
          Execução do gerador
          <select value={replayId ?? ""} onChange={(e) => setEscolhido(e.target.value)} disabled={!replays.dados?.length}>
            {(replays.dados ?? []).map((r) => (
              <option key={r.replay_id} value={r.replay_id}>{rotuloExecucao(r)}</option>
            ))}
          </select>
        </label>
        <label>
          Piloto (gráfico por volta)
          <select value={piloto ?? ""} onChange={(e) => setPilotoEscolhido(e.target.value)} disabled={!pilotos.length}>
            {pilotos.map((p) => <option key={p} value={p}>{p}</option>)}
          </select>
        </label>
      </div>

      {parcial && (
        <Faixa tipo="aviso">
          <span>
            Amostra pequena: esta execução tem {fmtInt(parcial.eventos)}
            {parcial.voltas_na_corrida ? <> de {fmtInt(parcial.voltas_na_corrida)}</> : null} voltas da corrida.
            Com poucas voltas as entradas do modelo quase não variam e a previsão sai quase constante.
            {completaRecente && <> A execução completa mais recente é <b>{completaRecente.replay_id}</b>.</>}
          </span>
        </Faixa>
      )}

      <Carga q={q} vazio={null} ehVazio={() => false}>
        {(d) => d.metricas.voltas === 0 ? (
          <section className="cartao">
            <Vazio titulo="Ainda não há previsões gravadas">
              O Dataflow ainda não está anexando a previsão do modelo às voltas (<code>tempo_previsto_s</code> vem nulo).
              {d.aguardando_predicao > 0 && <> Há <b>{fmtInt(d.aguardando_predicao)}</b> voltas aptas ao modelo esperando previsão.</>}
              {" "}Esta tela passa a mostrar previsto × real assim que a etapa de inferência estiver ligada.
            </Vazio>
          </section>
        ) : (
          <>
            <div className="kpis">
              <Kpi rotulo="Voltas comparadas" valor={fmtInt(d.metricas.voltas)} detalhe="com tempo real e previsão" />
              <Kpi rotulo="Erro absoluto médio" valor={`${fmtDec(d.metricas.mae_s)} s`} detalhe="|previsto − real|" />
              <Kpi rotulo="Viés" valor={`${(d.metricas.vies_s ?? 0) > 0 ? "+" : ""}${fmtDec(d.metricas.vies_s)} s`}
                   detalhe={(d.metricas.vies_s ?? 0) > 0 ? "o modelo superestima o tempo" : "o modelo subestima o tempo"} />
              <Kpi rotulo="Aguardando previsão" valor={fmtInt(d.aguardando_predicao)} detalhe="voltas aptas sem previsão" />
            </div>

            <div className="grade dois">
              <Cartao titulo={`Real × previsto — ${piloto ?? ""}`} sub="por volta" selo={<SeloOrigem origem="stream" />}
                      tabela={{
                        colunas: [{ chave: "volta", rotulo: "Volta", numerica: true }, { chave: "real", rotulo: "Real", numerica: true },
                          { chave: "previsto", rotulo: "Previsto", numerica: true }, { chave: "erro", rotulo: "Erro (s)", numerica: true }],
                        linhas: porVolta.map((l) => ({ volta: l.volta, real: fmtVolta(l.real), previsto: fmtVolta(l.previsto),
                          erro: fmtDec(l.previsto - l.real) })),
                      }}>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={porVolta} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="volta" tickLine={false} />
                    <YAxis domain={dominio} tickFormatter={fmtVolta} width={64} tickLine={false} />
                    <Tooltip content={<Tip titulo={(l) => `Volta ${l}`} valor={(v) => fmtVolta(v as number)} />} />
                    <Line dataKey="real" name="Real" stroke="var(--ink-2)" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} />
                    <Line dataKey="previsto" name="Previsto" stroke="var(--s3)" strokeWidth={2} strokeDasharray="6 4"
                          dot={{ r: 2 }} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
                <Legenda itens={[{ rotulo: "Real", cor: "var(--ink-2)" }, { rotulo: "Previsto (tracejado)", cor: "var(--s3)" }]} />
              </Cartao>

              <Cartao titulo="Previsto × real, todas as voltas" sub="mesma escala nos dois eixos: quanto mais perto da diagonal, melhor"
                      selo={<SeloOrigem origem="stream" />}>
                <ResponsiveContainer width="100%" height={300}>
                  <ScatterChart margin={{ top: 8, right: 12, bottom: 16, left: 0 }}>
                    <CartesianGrid />
                    <XAxis type="number" dataKey="real" name="Real" domain={escala?.dominio} ticks={escala?.ticks}
                           tickFormatter={fmtVolta}
                           label={{ value: "Real", position: "insideBottom", offset: -8, className: "recharts-text" }} />
                    <YAxis type="number" dataKey="previsto" name="Previsto" domain={escala?.dominio} ticks={escala?.ticks}
                           tickFormatter={fmtVolta} width={72}
                           label={{ value: "Previsto", angle: -90, position: "insideLeft", offset: 4, className: "recharts-text" }} />
                    {escala && (
                      <ReferenceLine segment={[{ x: escala.dominio[0], y: escala.dominio[0] }, { x: escala.dominio[1], y: escala.dominio[1] }]}
                                     stroke="var(--ink-2)" strokeDasharray="4 4" />
                    )}
                    <Tooltip content={<Tip titulo={() => "Volta"} valor={(v) => fmtVolta(v as number)} />} />
                    <Scatter data={dispersao} fill="var(--stream)" fillOpacity={0.75} isAnimationActive={false} />
                  </ScatterChart>
                </ResponsiveContainer>
                <Legenda itens={[{ rotulo: "Tracejado: previsão perfeita (previsto = real)", cor: "var(--ink-2)" }]} />
                <div className="legenda">Previsto de {faixas.previsto} · real de {faixas.real}</div>
              </Cartao>
            </div>
          </>
        )}
      </Carga>
    </>
  );
}
