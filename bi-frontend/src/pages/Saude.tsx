import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ResumoOrigem, ResumoStream, StatusPipeline } from "../api";
import { Cartao, Carga, Kpi, Selo, SeloOrigem, Tip, Vazio } from "../components/ui";
import { fmtHora, fmtInt, fmtMs } from "../format";
import { useApi } from "../hooks";

const pct = (parte: number, todo: number) => (todo ? `${((parte / todo) * 100).toFixed(1).replace(".", ",")}%` : "—");

export default function Saude() {
  const resumo = useApi<ResumoStream>("/api/stream/resumo", {}, 5000);
  const origens = useApi<ResumoOrigem[]>("/api/comparativo/origem", {}, 30000);
  // As mesmas checagens que a DAG do Airflow consulta; sempre_200 para a tela poder mostrar a falha.
  const status = useApi<StatusPipeline>("/api/pipeline/status", { sempre_200: "true" }, 10000);

  return (
    <>
      <h2 style={{ fontSize: 15, margin: "0 0 12px" }}>Histórico × streaming no BigQuery</h2>
      <Carga q={origens} vazio={<Vazio titulo="Sem dados na view unificada" />}>
        {(lista) => {
          const total = lista.reduce((s, o) => s + o.voltas, 0);
          return (
            <div className="kpis">
              {lista.map((o) => (
                <div className="kpi" key={o.origem}>
                  <SeloOrigem origem={o.origem} />
                  <div className="valor">{fmtInt(o.voltas)} voltas</div>
                  <div className="detalhe">
                    {pct(o.voltas, total)} da <code>vw_voltas_unificada</code> · {fmtInt(o.sessoes)} corrida(s)
                  </div>
                </div>
              ))}
            </div>
          );
        }}
      </Carga>

      <h2 style={{ fontSize: 15, margin: "20px 0 12px" }}>Saúde do pipeline (Pub/Sub → Dataflow → BigQuery)</h2>
      <Carga q={status} vazio={null} ehVazio={() => false}>
        {(st) => (
          <section className="cartao" style={{ marginBottom: 16 }}>
            <header>
              <h2>Checagens</h2>
              <span className="sub">as mesmas que o Airflow consulta em <code>/api/pipeline/status</code></span>
              <div className="acoes"><Selo tipo={st.status === "ok" ? "ok" : "falha"}>{st.status === "ok" ? "Pipeline saudável" : "Pipeline com falha"}</Selo></div>
            </header>
            <table>
              <tbody>
                {st.checagens.map((c) => (
                  <tr key={c.nome}>
                    <td><Selo tipo={c.estado === "ok" ? "ok" : c.estado === "falha" ? "falha" : "espera"}>{c.estado}</Selo></td>
                    <td><code>{c.nome}</code></td>
                    <td>{c.detalhe}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        )}
      </Carga>
      <Carga q={resumo} ehVazio={(r) => r.total_eventos === 0 && r.dlq_total === 0}
             vazio={<section className="cartao"><Vazio titulo="Nenhum evento passou pelo pipeline ainda">
               A tabela <code>f1_stream.voltas</code> e a DLQ estão vazias. Inicie o gerador e o job do Dataflow.</Vazio></section>}>
        {(r) => (
          <>
            <div className="kpis">
              <Kpi rotulo="Eventos no stream" valor={fmtInt(r.total_eventos)} detalhe={`último às ${fmtHora(r.ultimo_evento)}`} />
              <Kpi rotulo="Latência p50 / p95" valor={`${fmtMs(r.latencia_p50_ms)} / ${fmtMs(r.latencia_p95_ms)}`} detalhe="últimas 24 h · publicação → linha no BigQuery" />
              <Kpi rotulo="Rejeitados (DLQ)" valor={fmtInt(r.dlq_total)} detalhe={`${pct(r.dlq_total, r.dlq_total + r.total_eventos)} do que chegou`} />
              <Kpi rotulo="Aptos ao modelo" valor={pct(r.aptos_modelo, r.total_eventos)} detalhe={`${fmtInt(r.aptos_modelo)} voltas`} />
              <Kpi rotulo="Com previsão" valor={pct(r.com_predicao, r.total_eventos)} detalhe={`${fmtInt(r.com_predicao)} voltas`} />
            </div>

            <div className="grade dois">
              <Cartao titulo="Vazão" sub="eventos por minuto, últimos 30 min" selo={<SeloOrigem origem="stream" />}
                      tabela={{ colunas: [{ chave: "m", rotulo: "Minuto" }, { chave: "e", rotulo: "Eventos", numerica: true }],
                        linhas: r.por_minuto.map((p) => ({ m: fmtHora(p.minuto), e: p.eventos })) }}>
                {r.por_minuto.length === 0 ? <Vazio titulo="Nada chegou nos últimos 30 minutos" /> : (
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={r.por_minuto.map((p) => ({ ...p, hora: fmtHora(p.minuto).slice(0, 5) }))}
                              margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                      <CartesianGrid vertical={false} />
                      <XAxis dataKey="hora" tickLine={false} />
                      <YAxis allowDecimals={false} width={36} tickLine={false} />
                      <Tooltip content={<Tip titulo={(l) => `${l}`} valor={(v) => `${v} eventos`} />} cursor={{ fill: "var(--hover)" }} />
                      <Bar dataKey="eventos" name="Eventos" fill="var(--stream)" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </Cartao>

              <Cartao titulo="Rejeitados por motivo" sub="f1_stream.voltas_dlq" selo={<SeloOrigem origem="stream" />}
                      tabela={{ colunas: [{ chave: "m", rotulo: "Motivo" }, { chave: "e", rotulo: "Eventos", numerica: true }],
                        linhas: r.dlq_por_motivo.map((d) => ({ m: d.motivo, e: d.eventos })) }}>
                {r.dlq_por_motivo.length === 0 ? <Vazio titulo="Nenhum evento rejeitado">A DLQ está vazia.</Vazio> : (
                  <ResponsiveContainer width="100%" height={Math.max(180, r.dlq_por_motivo.length * 42)}>
                    <BarChart data={r.dlq_por_motivo} layout="vertical" margin={{ top: 4, right: 24, bottom: 0, left: 0 }}>
                      <CartesianGrid horizontal={false} />
                      <XAxis type="number" allowDecimals={false} tickLine={false} />
                      <YAxis type="category" dataKey="motivo" width={170} tickLine={false} />
                      <Tooltip content={<Tip titulo={(l) => `${l}`} valor={(v) => `${v} eventos`} />} cursor={{ fill: "var(--hover)" }} />
                      <Bar dataKey="eventos" name="Eventos" fill="var(--stream)" radius={[0, 4, 4, 0]} isAnimationActive={false} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </Cartao>
            </div>
          </>
        )}
      </Carga>
    </>
  );
}
