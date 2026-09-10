// Tabelas externas sobre o bucket raw, uma por endpoint.
//
// Sao a unica ponte entre o Cloud Storage e o BigQuery. Nao guardam dado:
// apontam para os .json.gz e sao lidas sob demanda.
//
// O truque do formato CSV
// -----------------------
// A Bronze precisa preservar cada linha do NDJSON exatamente como veio da
// API. Se declarassemos format='JSON', o BigQuery tentaria inferir o schema
// e tipar os campos -- que e trabalho da Silver, e quebraria na primeira
// mudanca de schema na origem.
//
// Declarando format='CSV' com um delimitador que nao existe em JSON (\x01)
// e sem caractere de aspas, cada linha do arquivo vira uma unica coluna
// STRING intocada. O BigQuery nem olha para dentro do conteudo.
//
// Particionamento Hive
// --------------------
// year= e session_key= estao no CAMINHO, nao dentro do JSON. Declarados
// como particoes, viram colunas consultaveis e permitem que o BigQuery pule
// arquivos irrelevantes -- sem isso seria impossivel relacionar as tabelas
// por sessao.

const { ENDPOINTS, soAno } = require("includes/endpoints");

const bucket = dataform.projectConfig.vars.bucket;
const prefixo = dataform.projectConfig.vars.prefixo;

ENDPOINTS.forEach((ep) => {
  const raiz = `gs://${bucket}/${prefixo}/${ep.nome}`;

  const particoes = soAno(ep)
    ? "year INT64"
    : "year INT64, session_key INT64";

  // O nome qualificado vem de ctx.self(): montar a string a mao com
  // dataform.projectConfig.defaultProject resolve para "undefined" em
  // tempo de execucao, ainda que a compilacao passe.
  operate(`stg_${ep.nome}`, (ctx) => `
    CREATE OR REPLACE EXTERNAL TABLE ${ctx.self()}
    (
      payload STRING
    )
    WITH PARTITION COLUMNS (${particoes})
    OPTIONS (
      format = 'CSV',
      field_delimiter = '\\x01',
      quote = '',
      compression = 'GZIP',
      uris = ['${raiz}/*'],
      hive_partition_uri_prefix = '${raiz}',
      require_hive_partition_filter = false,
      max_bad_records = 0,
      description = 'Staging externo de ${ep.nome}. Uma linha NDJSON por registro, sem tipagem.'
    )
  `)
    .hasOutput(true)
    .tags(["bronze", "externas"])
    .description(`Tabela externa sobre gs://${bucket}/${prefixo}/${ep.nome}/`);
});
