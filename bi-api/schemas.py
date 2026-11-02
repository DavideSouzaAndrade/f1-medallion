"""Formato das respostas. Nomes de campo iguais aos das colunas do BigQuery."""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel

Origem = Literal["batch", "stream"]


class Sessao(BaseModel):
    session_key: int
    year: int
    session_name: Optional[str] = None
    nome_evento: Optional[str] = None
    circuito: Optional[str] = None
    circuit_type: Optional[str] = None
    pais: Optional[str] = None
    eh_corrida: bool
    data_inicio_agendada: Optional[datetime] = None


class Piloto(BaseModel):
    driver_number: int
    sigla: Optional[str] = None
    nome_completo: Optional[str] = None
    equipe: Optional[str] = None


class Volta(BaseModel):
    """Uma volta de vw_voltas_unificada. A mesma volta pode vir das duas origens num replay."""
    origem: Origem
    session_key: int
    driver_number: int
    lap_number: int
    duracao_s: Optional[float] = None
    setor_1_s: Optional[float] = None
    setor_2_s: Optional[float] = None
    setor_3_s: Optional[float] = None
    composto: Optional[str] = None
    stint_number: Optional[int] = None
    idade_pneu: Optional[int] = None
    eh_saida_box: Optional[bool] = None
    eh_entrada_box: Optional[bool] = None
    gap_medio_a_frente: Optional[float] = None
    temp_pista: Optional[float] = None


class PontoDegradacao(BaseModel):
    composto: str
    idade_pneu: int
    tempo_medio_s: float
    voltas: int


class ResumoOrigem(BaseModel):
    origem: Origem
    voltas: int
    sessoes: int
    primeira_volta: Optional[datetime] = None
    ultima_volta: Optional[datetime] = None


class MotivoDlq(BaseModel):
    motivo: str
    eventos: int


class PontoPorMinuto(BaseModel):
    minuto: datetime
    eventos: int


class ResumoStream(BaseModel):
    total_eventos: int
    ultimo_evento: Optional[datetime] = None
    aptos_modelo: int
    com_predicao: int
    latencia_p50_ms: Optional[int] = None
    latencia_p95_ms: Optional[int] = None
    dlq_total: int
    dlq_por_motivo: list[MotivoDlq]
    por_minuto: list[PontoPorMinuto]


class Replay(BaseModel):
    replay_id: str
    session_key: int
    circuito: Optional[str] = None
    eventos: int
    ultimo_evento: datetime
    # Contagens da execução inteira. O front não pode contá-las pelo SSE: o buffer dele
    # guarda só as voltas mais recentes, e o placar travava no meio da corrida.
    aptos_modelo: int = 0
    com_predicao: int = 0
    # Voltas que a corrida tem no histórico (Silver): o denominador do progresso do replay.
    voltas_na_corrida: Optional[int] = None
    # De onde o dado veio: "replay" (corrida reproduzida) ou "openf1" (feed ao vivo).
    # Procedência é imutável — um replay encerrado continua sendo replay.
    fonte: str = "replay"
    # Está chegando evento agora? Derivado de ultimo_evento, não armazenado.
    em_curso: bool = False


class EventoStream(BaseModel):
    """Uma volta chegando ao vivo (f1_stream.voltas)."""
    evento_id: str
    replay_id: str
    ts_evento: datetime
    ts_processamento: datetime
    session_key: int
    driver_number: int
    piloto: str
    lap_number: int
    year: int
    circuito: Optional[str] = None
    composto: Optional[str] = None
    idade_pneu: Optional[int] = None
    duracao_s: Optional[float] = None
    setor_1_s: Optional[float] = None
    setor_2_s: Optional[float] = None
    setor_3_s: Optional[float] = None
    eh_saida_box: bool
    eh_entrada_box: bool
    apto_modelo: bool
    latencia_ms: Optional[int] = None
    tempo_previsto_s: Optional[float] = None
    modelo_versao: Optional[str] = None


class ParPrevistoReal(BaseModel):
    """Previsto e real da mesma volta: a chave (sessão, piloto, volta) vem da própria linha."""
    evento_id: str
    replay_id: str
    session_key: int
    driver_number: int
    piloto: str
    lap_number: int
    composto: Optional[str] = None
    duracao_s: float
    tempo_previsto_s: float
    erro_s: float
    modelo_versao: Optional[str] = None


class MetricasPrevisao(BaseModel):
    voltas: int
    mae_s: Optional[float] = None       # erro absoluto médio
    vies_s: Optional[float] = None      # erro médio com sinal: > 0 = o modelo superestima


class PrevistoReal(BaseModel):
    metricas: MetricasPrevisao
    pares: list[ParPrevistoReal]
    # Voltas válidas do stream que ainda estão sem previsão (a parte 3 as preenche).
    aguardando_predicao: int


class Checagem(BaseModel):
    nome: str
    estado: Literal["ok", "falha", "pendente"]
    detalhe: str


class StatusPipeline(BaseModel):
    """Saúde do pipeline em formato de checagens: para uma DAG do Airflow e para a aba Saúde."""
    status: Literal["ok", "falha"]
    verificado_em: datetime
    checagens: list[Checagem]
