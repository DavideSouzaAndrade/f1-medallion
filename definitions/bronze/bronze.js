// Camada Bronze: uma tabela nativa por endpoint.
//
// Por que materializar em vez de consultar a externa direto
// ---------------------------------------------------------
// Arquivos gzip nao sao divisiveis, entao o BigQuery nao consegue le-los em
// paralelo. Silver e Gold vao consultar esses dados repetidas vezes; o custo
// de armazenamento (centavos por mes) compensa com folga.
//
// Por que a carga e incremental
// -----------------------------
// A ingestao so traz sessoes novas -- ver docs/ingestao-incremental.md. Se a
// Bronze recriasse a tabela do zero a cada execucao, releria os 12 mil
// objetos e o ganho da ingestao morreria aqui.
//
// A chave incremental e o ARQUIVO, nao a sessao
// ---------------------------------------------
// car_data e location tem um arquivo por piloto, entao uma sessao sao varios
// objetos. Se a chave fosse o session_key, este cenario perderia dado em
// silencio:
//
//   1. a ingestao grava parte dos pilotos da sessao X
//   2. a Bronze roda e marca a sessao X como carregada
//   3. a ingestao grava os pilotos restantes
//   4. a Bronze roda de novo, ve a sessao X como feita, e pula
//      -> os arquivos que chegaram depois nunca entram
//
// Nao e hipotetico: a Bronze pode rodar enquanto a ingestao ainda escreve.
//
// Por isso a chave e o par (session_key, driver_number). O driver_number vem
// do NOME DO ARQUIVO -- metadado de caminho, igual a year e session_key, e
// nao conteudo do payload. Note que o codigo nunca assume quantos pilotos
// uma sessao tem: ele subtrai conjuntos. Sao 18, 20 ou 22 conforme o ano e
// quem de fato foi a pista, e isso nao muda nada aqui.
//
// Endpoints de escopo "ano" (meetings, sessions) sao a excecao: sao listas
// que crescem, e nao colecoes de sessoes independentes. Nesses a tabela e
// recarregada por inteiro -- sao dois objetos por temporada, custo irrisorio.
//
// O que a Bronze NAO faz
// ----------------------
// Nao tipa, nao renomeia, nao filtra e nao interpreta o payload. Ele fica
// como STRING, e a Silver decide o que fazer com ele. Isso torna a camada
// imune a mudanca de schema na origem.

const { ENDPOINTS, soAno } = require("includes/endpoints");

ENDPOINTS.forEach((ep) => {
  const anual = soAno(ep);
  const porPiloto = ep.escopo === "piloto";

  publish(`bronze_${ep.nome}`, {
    type: anual ? "table" : "incremental",
    schema: "f1_bronze",
    tags: ["bronze", anual ? "bronze_anual" : "bronze_incremental"],
    description:
      `Camada bronze de ${ep.nome}: copia fiel da origem, um registro por linha. ` +
      (anual
        ? "Recarregada por inteiro a cada execucao (lista de nivel de temporada)."
        : `Carga incremental por ${porPiloto ? "(session_key, driver_number)" : "session_key"}.`),
    bigquery: {
      partitionBy: "DATE(_data_ingestao_bronze)",
      ...(anual
        ? {}
        : { clusterBy: porPiloto ? ["session_key", "driver_number"] : ["session_key"] }),
    },
    columns: {
      id_registro: "MD5 do payload. Deterministico: o mesmo registro na origem gera sempre o mesmo id, o que facilita a deduplicacao na Silver.",
      year: "Temporada. Vem da particao Hive do caminho, nao do JSON.",
      ...(anual ? {} : { session_key: "Chave da sessao. Vem da particao Hive do caminho, nao do JSON." }),
      ...(porPiloto ? { driver_number: "Numero do piloto, extraido do NOME DO ARQUIVO (driver_N.json.gz). Metadado de caminho, nao conteudo do payload. Junto com session_key, forma a chave da carga incremental." } : {}),
      payload: "A linha do NDJSON, integra e sem tipagem.",
      _arquivo_origem: "Objeto do GCS de onde a linha veio (pseudo-coluna _FILE_NAME).",
      _data_ingestao_bronze: "Instante da carga nesta camada. Coluna de particionamento.",
    },
    assertions: {
      nonNull: ["id_registro", "year", "payload"],
    },
  }).query((ctx) => `
SELECT
  TO_HEX(MD5(origem.payload))       AS id_registro,
  origem.year,
  ${anual ? "" : "origem.session_key,"}
  ${porPiloto ? "CAST(REGEXP_EXTRACT(origem._FILE_NAME, r'driver_(\\d+)\\.json\\.gz$') AS INT64) AS driver_number," : ""}
  origem.payload,
  origem._FILE_NAME                 AS _arquivo_origem,
  CURRENT_TIMESTAMP()               AS _data_ingestao_bronze
FROM ${ctx.ref(`stg_${ep.nome}`)} AS origem
-- Objeto vazio e checkpoint legitimo: a combinacao foi consultada e a API
-- nao tinha registro. Ver ingestion/openf1_client.py sobre o HTTP 404.
WHERE origem.payload IS NOT NULL
  AND origem.payload != ''
${ctx.when(
    ctx.incremental() && !anual,
    `  -- Anti-join contra o que ja foi carregado. Compara colunas INT64
  -- pequenas, e nao o nome do arquivo, que custaria varios GB por execucao.
  AND NOT EXISTS (
    SELECT 1
    FROM ${ctx.self()} AS carregado
    WHERE carregado.session_key = origem.session_key
${porPiloto
      ? `      AND carregado.driver_number = CAST(REGEXP_EXTRACT(origem._FILE_NAME, r'driver_(\\d+)\\.json\\.gz$') AS INT64)`
      : ""}
  )`
  )}
`);
});
