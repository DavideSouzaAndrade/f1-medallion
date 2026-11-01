import { useMemo, useState } from "react";
import {
  CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis,
} from "recharts";
import { PrevistoReal as Dados, Replay } from "../api";
import { Cartao, Carga, Kpi, Legenda, SeloOrigem, Tip, Vazio } from "../components/ui";
import { fmtDec, fmtInt, fmtVolta } from "../format";
import { useApi } from "../hooks";

const dominio = [(min: number) => Math.floor(min - 1), (max: number) => Math.ceil(max + 1)] as [
  (min: number) => number, (max: number) => number];

export default function PrevistoReal() {
  const replays = useApi<Replay[]>("/api/stream/replays", {}, 15000);
  const [escolhido, setEscolhido] = useState<string | null>(null);
  const replayId = escolhido ?? replays.dados?.[0]?.replay_id;

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
  const faixa = useMemo(() => {
    const todos = dispersao.flatMap((d) => [d.real, d.previsto]);
    return todos.length ? [Math.min(...todos), Math.max(...todos)] : [0, 0];
  }, [dispersao]);

  return (
    <>
      <div className="filtros">
        <label>
          Execução do gerador
          <select value={replayId ?? ""} onChange={(e) => setEscolhido(e.target.value)} disabled={!replays.dados?.length}>
            {(replays.dados ?? []).map((r) => (
              <option key={r.replay_id} value={r.replay_id}>{r.replay_id} · {r.circuito ?? r.session_key}</option>
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

              <Cartao titulo="Previsto × real, todas as voltas" sub="quanto mais perto da diagonal, melhor"
                      selo={<SeloOrigem origem="stream" />}>
                <ResponsiveContainer width="100%" height={300}>
                  <ScatterChart margin={{ top: 8, right: 12, bottom: 16, left: 0 }}>
                    <CartesianGrid />
                    <XAxis type="number" dataKey="real" name="Real" domain={dominio} tickFormatter={fmtVolta}
                           label={{ value: "Real", position: "insideBottom", offset: -8, className: "recharts-text" }} />
                    <YAxis type="number" dataKey="previsto" name="Previsto" domain={dominio} tickFormatter={fmtVolta} width={72}
                           label={{ value: "Previsto", angle: -90, position: "insideLeft", offset: 4, className: "recharts-text" }} />
                    <ReferenceLine segment={[{ x: faixa[0], y: faixa[0] }, { x: faixa[1], y: faixa[1] }]}
                                   stroke="var(--ink-2)" strokeDasharray="4 4" />
                    <Tooltip content={<Tip titulo={() => "Volta"} valor={(v) => fmtVolta(v as number)} />} />
                    <Scatter data={dispersao} fill="var(--stream)" fillOpacity={0.75} isAnimationActive={false} />
                  </ScatterChart>
                </ResponsiveContainer>
              </Cartao>
            </div>
          </>
        )}
      </Carga>
    </>
  );
}
