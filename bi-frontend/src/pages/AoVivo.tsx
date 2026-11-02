import { useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Replay } from "../api";
import { Cartao, Faixa, Kpi, SeloOrigem, Tip, Vazio } from "../components/ui";
import { corSerie, dominioRobusto, fmtHaData, fmtHora, fmtInt, fmtMs, fmtVolta } from "../format";
import { EstadoAoVivo, useApi, useRelogio } from "../hooks";

const SEM_DADOS_NOVOS_S = 60;
const dominioTempo = [(min: number) => Math.floor(min - 1), (max: number) => Math.ceil(max + 1)] as [
  (min: number) => number, (max: number) => number];

export default function AoVivo({ vivo }: { vivo: EstadoAoVivo }) {
  const agora = useRelogio();
  const [replayEscolhido, setReplayEscolhido] = useState<string | null>(null);

  const replays = useMemo(() => [...new Set(vivo.eventos.map((e) => e.replay_id))].reverse(), [vivo.eventos]);
  const replayId = replayEscolhido && replays.includes(replayEscolhido) ? replayEscolhido : replays[0] ?? null;
  const eventos = useMemo(() => vivo.eventos.filter((e) => e.replay_id === replayId), [vivo.eventos, replayId]);

  const pilotos = useMemo(() => [...new Map(eventos.map((e) => [e.driver_number, e.piloto])).entries()]
    .sort((a, b) => a[0] - b[0]), [eventos]);

  const ritmo = useMemo(() => {
    const porVolta = new Map<number, Record<string, number | null>>();
    for (const e of eventos) {
      const linha = porVolta.get(e.lap_number) ?? { volta: e.lap_number };
      linha[`d${e.driver_number}`] = e.duracao_s != null && !e.eh_entrada_box && !e.eh_saida_box ? e.duracao_s : null;
      porVolta.set(e.lap_number, linha);
    }
    return [...porVolta.values()].sort((a, b) => (a.volta as number) - (b.volta as number));
  }, [eventos]);

  const escalaRitmo = useMemo(
    () => dominioRobusto(ritmo.flatMap((l) => Object.entries(l).filter(([k, v]) => k !== "volta" && v != null).map(([, v]) => v as number))),
    [ritmo]);

  // Contagens e total de voltas da corrida (Silver) vêm da API. Só atualiza enquanto há replay.
  const replaysApi = useApi<Replay[]>("/api/stream/replays", replayId ? {} : null, 5000);
  const meta = replaysApi.dados?.find((r) => r.replay_id === replayId);
  const totalVoltas = meta?.voltas_na_corrida ?? null;

  // O placar conta a execução inteira, não o buffer do SSE: o buffer guarda só as últimas
  // voltas (MAX_EVENTOS) e, ao abrir a aba, recebe só as mais recentes. Contar por ele fazia
  // o placar travar em 600 no meio da corrida. O max() cobre os até 5 s em que o SSE já
  // entregou voltas que a consulta de /replays ainda não viu.
  const recebidas = Math.max(meta?.eventos ?? 0, eventos.length);
  const aptos = Math.max(meta?.aptos_modelo ?? 0, eventos.filter((e) => e.apto_modelo).length);
  const comPrevisao = Math.max(meta?.com_predicao ?? 0, eventos.filter((e) => e.tempo_previsto_s != null).length);
  const progresso = totalVoltas ? Math.min(100, Math.round((100 * recebidas) / totalVoltas)) : null;

  const ultimo = eventos[eventos.length - 1];
  const latencias = eventos.map((e) => e.latencia_ms).filter((x): x is number => x != null).sort((a, b) => a - b);
  const mediana = latencias.length ? latencias[Math.floor(latencias.length / 2)] : null;
  // Medido pelo TS do evento, e nao por quando ele chegou nesta aba: o SSE
  // reenvia o buffer a cada reconexao, entao a chegada reinicia no F5.
  const ultimoTs = ultimo ? new Date(ultimo.ts_evento).getTime() : null;
  const parado = ultimoTs != null && (agora - ultimoTs) / 1000 > SEM_DADOS_NOVOS_S;

  // A execucao e um replay ou o feed de verdade? E ela ainda esta chegando?
  // Procedencia vem da linha; atividade a API deriva do relogio.
  const eReplay = (meta?.fonte ?? "replay") === "replay";
  const emCurso = meta?.em_curso ?? !parado;
  const rotuloOrigem = `${eReplay ? "Replay" : "OpenF1"} · ${emCurso ? "em curso" : "encerrado"}`;

  // A ultima latencia NAO nula. As voltas que nunca receberam versao final
  // saem por ultimo e tem latencia nula de proposito (nao houve publish_time
  // para medir contra), entao a mais recente seria sempre "—".
  const ultimaLatencia = [...eventos].reverse().find((e) => e.latencia_ms != null)?.latencia_ms ?? null;

  return (
    <>
      {vivo.conexao === "fora" && (
        <Faixa tipo="erro">Sem conexão com a API de BI. Tentando reconectar a cada poucos segundos.</Faixa>
      )}
      {vivo.erroServidor && (
        <Faixa tipo="erro">A API está no ar, mas não conseguiu consultar o BigQuery agora. Tentando de novo.</Faixa>
      )}
      {parado && vivo.conexao === "conectado" && (
        <Faixa tipo="aviso">
          Sem voltas novas {fmtHaData(ultimo?.ts_evento, agora)}. O gerador pode ter terminado ou o job do Dataflow estar parado.
        </Faixa>
      )}

      {replays.length > 1 && (
        <div className="filtros">
          <label>
            Execução do gerador
            <select value={replayId ?? ""} onChange={(e) => setReplayEscolhido(e.target.value)}>
              {replays.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
          </label>
        </div>
      )}

      {/* Procedência e atividade, lado a lado: um replay encerrado é dado
          histórico, e a tela não pode insinuar que está acontecendo. */}
      <Faixa tipo={emCurso ? "ok" : "aviso"}>
        <strong>{rotuloOrigem}</strong>
        {eReplay
          ? " — corrida reproduzida a partir do histórico, não uma corrida acontecendo agora."
          : " — feed ao vivo da OpenF1."}
        {!emCurso && " Esta execução terminou: o que você vê aqui é dado histórico."}
      </Faixa>

      <div className="kpis">
        <Kpi rotulo="Voltas recebidas" valor={fmtInt(recebidas)} detalhe={replayId ?? "nenhuma execução ainda"} />
        <Kpi rotulo="Última volta" valor={ultimo ? `${ultimo.piloto} · V${ultimo.lap_number}` : "—"}
             detalhe={ultimo ? `chegou ${fmtHaData(ultimo.ts_evento, agora)}` : undefined} />
        <Kpi rotulo="Latência do pipeline" valor={fmtMs(ultimaLatencia)} detalhe={`mediana ${fmtMs(mediana)}`} />
        <Kpi rotulo="Aptas ao modelo" valor={fmtInt(aptos)} detalhe={`${fmtInt(comPrevisao)} já com previsão`} />
      </div>

      {progresso !== null && totalVoltas && (
        <div className="progresso">
          <label htmlFor="progresso-replay">
            Replay da corrida: {fmtInt(recebidas)} de {fmtInt(totalVoltas)} voltas ({progresso}%)
          </label>
          <progress id="progresso-replay" max={totalVoltas} value={Math.min(recebidas, totalVoltas)} />
        </div>
      )}

      {eventos.length === 0 ? (
        <section className="cartao">
          {vivo.conexao === "conectado" ? (
            <Vazio titulo="Nenhuma volta chegou ainda">
              Para ver voltas aqui, o gerador precisa estar publicando no Pub/Sub e o job do Dataflow ligado.
              Dispare o job <code>f1-gerador</code> (ou a DAG do Airflow) e esta tela se atualiza sozinha.
            </Vazio>
          ) : (
            <Vazio titulo="Aguardando conexão com a API de BI">Assim que a conexão voltar, as voltas aparecem aqui.</Vazio>
          )}
        </section>
      ) : (
        <div className="grade dois">
          <Cartao titulo="Ritmo ao vivo" sub="tempo de cada volta, por piloto; box e voltas muito lentas ficam fora da escala"
                  selo={<SeloOrigem origem="stream" />}>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={ritmo} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid vertical={false} />
                <XAxis dataKey="volta" tickLine={false} />
                <YAxis domain={escalaRitmo ?? dominioTempo} allowDataOverflow tickFormatter={fmtVolta} width={64} tickLine={false} />
                <Tooltip content={<Tip titulo={(l) => `Volta ${l}`} valor={(v) => fmtVolta(v as number)} />} />
                {pilotos.map(([n, sigla], i) => (
                  <Line key={n} dataKey={`d${n}`} name={sigla} stroke={corSerie(i)} strokeWidth={2}
                        dot={{ r: 2 }} activeDot={{ r: 5 }} isAnimationActive={false} />
                ))}
              </LineChart>
            </ResponsiveContainer>
            <div className="legenda">
              {pilotos.map(([n, sigla], i) => <span key={n}><i style={{ background: corSerie(i) }} />{sigla}</span>)}
            </div>
          </Cartao>

          <Cartao titulo="Últimas voltas" sub="mais recente primeiro" selo={<SeloOrigem origem="stream" />}>
            <div className="tabela-rolagem">
              <table>
                <thead>
                  <tr>
                    <th>Hora</th><th>Piloto</th><th className="num">Volta</th><th>Pneu</th>
                    <th className="num">Tempo</th><th className="num">Previsto</th><th className="num">Latência</th>
                  </tr>
                </thead>
                <tbody>
                  {eventos.slice(-15).reverse().map((e, i) => (
                    <tr key={e.evento_id} className={i === 0 && vivo.ultimaVolta && agora - vivo.ultimaVolta < 2500 ? "nova" : ""}>
                      <td>{fmtHora(e.ts_evento)}</td>
                      <td>{e.piloto}</td>
                      <td className="num">{e.lap_number}</td>
                      <td>{e.composto ?? "—"}</td>
                      <td className="num">{fmtVolta(e.duracao_s)}</td>
                      <td className="num" title={e.modelo_versao ?? undefined}>
                        {e.tempo_previsto_s != null
                          ? fmtVolta(e.tempo_previsto_s)
                          : e.apto_modelo ? "aguardando"
                          : e.eh_entrada_box || e.eh_saida_box ? "n/a (volta de box)"
                          : e.duracao_s == null ? "n/a (volta incompleta)"
                          : "n/a (fora do recorte)"}
                      </td>
                      <td className="num">{fmtMs(e.latencia_ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Cartao>
        </div>
      )}
    </>
  );
}
