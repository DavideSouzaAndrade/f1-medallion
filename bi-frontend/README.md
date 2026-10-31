# Front-end de BI

React + Vite + TypeScript + Recharts. Consome só a [API de BI](../bi-api/README.md); nunca o
BigQuery. Tema claro/escuro (segue o sistema, com botão).

| Aba | Dado | Origem |
|---|---|---|
| **Histórico** | ritmo por volta e degradação do pneu, por corrida e piloto | batch (Silver/Gold) |
| **Ao vivo** | voltas chegando por SSE, últimas voltas, latência | stream (Dataflow) |
| **Previsto × Real** | MAE, viés, real × previsto por volta e dispersão | stream (+ modelo) |
| **Saúde do pipeline** | batch × stream na view unificada, vazão, latência, DLQ por motivo | os dois |

Convenções que valem para o app todo:

- **Batch e stream nunca se misturam visualmente.** Cada cartão leva o selo `HISTÓRICO · batch`
  (azul) ou `AO VIVO · stream` (laranja), sempre com ícone + texto.
- **Previsto e real da mesma volta** vêm da mesma linha; a chave é (corrida, piloto, volta).
- **Erros e vazios são visíveis:** API fora do ar, consulta ao BigQuery falhando, sem voltas
  novas há 60 s, e "ainda não há previsões" (parte 3). A tela anterior é mantida se só o refresh falha.
- **Acessibilidade:** todo gráfico tem "Ver tabela"; cor nunca é o único canal.
- Paleta validada com o `validate_palette.js` do guia de dataviz (claro e escuro).

## Executar

```bash
npm install

# sem GCP (terminal 1): API com dados simulados
cd ../bi-api && MOCK=1 uvicorn main:app --port 8080
# terminal 2
npm run dev                 # http://localhost:5173  (o Vite encaminha /api para a porta 8080)

npm test                    # formatadores
npm run build               # tsc + build de produção
```

Em produção: `bash deploy.sh` (Cloud Run + nginx). Ele lê a URL da API de BI e a embute no build
(`VITE_API_URL`). Depois, libere a URL do front no CORS da API (`CORS_ORIGINS`).

## Estrutura

```
src/
  api.ts          tipos (espelham bi-api/schemas.py) e fetch com erro tratado
  hooks.ts        useApi (consulta + polling), useAoVivo (SSE com reconexão), useRelogio
  format.ts       formatadores (1:23.412, "há 12 s") e cores por entidade
  theme.css       tokens claro/escuro
  components/ui   Cartao, Kpi, selos, Carga (carregando/erro/vazio), tooltip
  pages/          Historico, AoVivo, PrevistoReal, Saude
```
