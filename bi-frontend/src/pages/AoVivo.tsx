import { useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Cartao, Faixa, Kpi, SeloOrigem, Tip, Vazio } from "../components/ui";
import { corSerie, dominioRobusto, fmtHa, fmtHora, fmtInt, fmtMs, fmtVolta } from "../format";
import { EstadoAoVivo, useRelogio } from "../hooks";

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

  const ultimo = eventos[eventos.length - 1];
  const latencias = eventos.map((e) => e.latencia_ms).filter((x): x is number => x != null).sort((a, b) => a - b);
  const mediana = latencias.length ? latencias[Math.floor(latencias.length / 2)] : null;
  const aptos = eventos.filter((e) => e.apto_modelo).length;
  const comPrevisao = eventos.filter((e) => e.tempo_previsto_s != null).length;
  const parado = vivo.ultimaVolta != null && (agora - vivo.ultimaVolta) / 1000 > SEM_DADOS_NOVOS_S;

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
          Sem voltas novas {fmtHa(vivo.ultimaVolta, agora)}. O gerador pode ter terminado ou o job do Dataflow estar parado.
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

      <div className="kpis">
        <Kpi rotulo="Voltas recebidas" valor={fmtInt(eventos.length)} detalhe={replayId ?? "nenhuma execução ainda"} />
        <Kpi rotulo="Última volta" valor={ultimo ? `${ultimo.piloto} · V${ultimo.lap_number}` : "—"}
             detalhe={ultimo ? `chegou ${fmtHa(vivo.ultimaVolta, agora)}` : undefined} />
        <Kpi rotulo="Latência do pipeline" valor={fmtMs(ultimo?.latencia_ms)} detalhe={`mediana ${fmtMs(mediana)}`} />
        <Kpi rotulo="Aptas ao modelo" valor={fmtInt(aptos)} detalhe={`${fmtInt(comPrevisao)} já com previsão`} />
      </div>

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
                        {e.tempo_previsto_s != null ? fmtVolta(e.tempo_previsto_s) : e.apto_modelo ? "aguardando" : "n/a (volta de box)"}
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
