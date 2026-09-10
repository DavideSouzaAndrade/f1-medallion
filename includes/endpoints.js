// Catalogo dos endpoints da OpenF1 que existem no bucket.
//
// Espelha ingestion/config.py. Se um endpoint entrar la, entra aqui tambem.
//
// escopo:
//   "ano"     -> um objeto por temporada, caminho so com year=
//                  openf1/sessions/year=2024/data.json.gz
//   "sessao"  -> um objeto por sessao, caminho com year= e session_key=
//                  openf1/laps/year=2024/session_key=9472/data.json.gz
//   "piloto"  -> um objeto por piloto por sessao, mesmas particoes
//                  openf1/car_data/year=2024/session_key=9472/driver_1.json.gz
//
// As particoes Hive sao as mesmas para "sessao" e "piloto": o numero do
// piloto esta no nome do arquivo, nao no caminho de particao.

const ENDPOINTS = [
  { nome: "meetings",              escopo: "ano" },
  { nome: "sessions",              escopo: "ano" },
  { nome: "drivers",               escopo: "sessao" },
  { nome: "laps",                  escopo: "sessao" },
  { nome: "stints",                escopo: "sessao" },
  { nome: "pit",                   escopo: "sessao" },
  { nome: "weather",               escopo: "sessao" },
  { nome: "position",              escopo: "sessao" },
  { nome: "race_control",          escopo: "sessao" },
  { nome: "session_result",        escopo: "sessao" },
  { nome: "intervals",             escopo: "sessao" },
  { nome: "starting_grid",         escopo: "sessao" },
  { nome: "team_radio",            escopo: "sessao" },
  { nome: "overtakes",             escopo: "sessao" },
  { nome: "championship_drivers",  escopo: "sessao" },
  { nome: "championship_teams",    escopo: "sessao" },
  { nome: "car_data",              escopo: "piloto" },
  { nome: "location",              escopo: "piloto" },
];

// Endpoints particionados apenas por ano sao listas que crescem a cada fim
// de semana: nao ha "sessao nova" a acrescentar, a propria lista muda. Por
// isso sao recarregados por inteiro, e nao incrementalmente.
const soAno = (ep) => ep.escopo === "ano";

const raiz = (prefixo, endpoint) => `${prefixo}/${endpoint}`;

module.exports = { ENDPOINTS, soAno, raiz };
