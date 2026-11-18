import json
import os
import random
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

CHEGADA = Path(os.environ.get("AGROFACIL_CHEGADA", Path(__file__).resolve().parent.parent / "chegada"))

MUNICIPIOS_PADRAO = [
    {"codigo_ibge": "3509502", "nome": "Campinas", "lat": -22.90, "lon": -47.06},
    {"codigo_ibge": "3543402", "nome": "Ribeirão Preto", "lat": -21.17, "lon": -47.81},
    {"codigo_ibge": "3538709", "nome": "Piracicaba", "lat": -22.73, "lon": -47.64},
]

URL_CLIMA = "https://api.open-meteo.com/v1/forecast"
URL_IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/SP/municipios"


# ----------------------------------------------------------------------------- caminhos
def caminho_clima(codigo_ibge, ds):
    return CHEGADA / "clima" / f"clima_{codigo_ibge}_{ds}.json"


def caminho_municipios(ds):
    return CHEGADA / "ibge_municipios" / f"ibge_municipios_{ds}.json"


def caminho_lavouras(ds):
    return CHEGADA / "lavouras" / f"lavouras_{ds}.json"


def _municipios():
    return json.loads(Variable.get("AGROFACIL_MUNICIPIOS", json.dumps(MUNICIPIOS_PADRAO)))


def _gravar(destino: Path, texto: str):
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".tmp")
    tmp.write_text(texto, encoding="utf-8")
    tmp.replace(destino)


# ----------------------------------------------------------------------------- coletas
def coletar_clima(ds, codigo_ibge, **_):
    mun = next((m for m in _municipios() if m["codigo_ibge"] == codigo_ibge), None)
    if mun is None:
        raise ValueError(f"Município {codigo_ibge} não está na Variable AGROFACIL_MUNICIPIOS")

    params = {
        "latitude": mun["lat"],
        "longitude": mun["lon"],
        "start_date": ds,
        "end_date": ds,
        "hourly": "temperature_2m,precipitation,soil_moisture_0_to_7cm",
        "timezone": "America/Sao_Paulo",
    }
    r = requests.get(URL_CLIMA, params=params, timeout=30)
    r.raise_for_status()

    hourly = r.json().get("hourly") or {}
    if not hourly.get("time"):
        raise ValueError(f"Open-Meteo não devolveu horários para {mun['nome']} em {ds}")
    _gravar(caminho_clima(codigo_ibge, ds), r.text)


def coletar_ibge_municipios(ds, **_):
    r = requests.get(URL_IBGE_MUNICIPIOS, timeout=60)
    r.raise_for_status()
    dados = r.json()
    if not isinstance(dados, list) or not dados:
        raise ValueError("IBGE Localidades devolveu resposta vazia ou inesperada")
    ids = {str(m["id"]) for m in dados}
    faltando = [m["codigo_ibge"] for m in _municipios() if m["codigo_ibge"] not in ids]
    if faltando:
        raise ValueError(f"Municípios monitorados fora do cadastro do IBGE: {faltando}")
    _gravar(caminho_municipios(ds), r.text)


# ----------------------------------------------------------------------------- gerador (simulado)
CATALOGO_LAVOURAS = {
    "3509502": [("Cana-de-açúcar", 9800, 80.0), ("Milho", 3100, 6.5), ("Soja", 1900, 3.5)],
    "3543402": [("Cana-de-açúcar", 52000, 82.0), ("Soja", 6500, 3.6), ("Milho", 4200, 7.0)],
    "3538709": [("Cana-de-açúcar", 41000, 79.0), ("Milho", 5000, 6.8), ("Laranja", 1200, 30.0)],
}
ANO_REFERENCIA = 2025


def gerar_lavouras(ds):
    linhas = []
    for codigo, culturas in CATALOGO_LAVOURAS.items():
        nome = next((m["nome"] for m in MUNICIPIOS_PADRAO if m["codigo_ibge"] == codigo), codigo)
        for cultura, area_base, prod_base in culturas:
            rng = random.Random(f"{codigo}|{cultura}|{ANO_REFERENCIA}")
            area = round(area_base * rng.uniform(0.95, 1.05), 1)
            producao = round(area * prod_base * rng.uniform(0.95, 1.05), 1)
            linhas.append({
                "codigo_ibge": codigo, "municipio": nome, "uf": "SP", "cultura": cultura,
                "area_plantada_ha": area, "producao_toneladas": producao,
                "ano_referencia": ANO_REFERENCIA, "origem": "simulado",
            })

    dia = int(ds[-2:])
    if dia % 6 == 0:
        base = linhas[0]
        linhas.append({**base, "cultura": "Soja", "area_plantada_ha": 0.0})
        linhas.append({**linhas[4], "producao_toneladas": -1500.0})
        linhas.append({**base, "codigo_ibge": "9999999", "municipio": "Município Inexistente"})
    if dia % 5 == 0:
        linhas.append(dict(linhas[1]))
    return linhas


def gerar_lavouras_simuladas(ds, **_):
    _gravar(caminho_lavouras(ds), json.dumps(gerar_lavouras(ds), ensure_ascii=False))


# ----------------------------------------------------------------------------- verificação
def conferir_chegada(ds, **_):
    esperados = [caminho_clima(m["codigo_ibge"], ds) for m in _municipios()]
    esperados += [caminho_municipios(ds), caminho_lavouras(ds)]

    faltando = [str(p) for p in esperados if not p.exists() or p.stat().st_size == 0]
    if faltando:
        raise FileNotFoundError(f"Arquivos não chegaram ou estão vazios: {faltando}")

    invalidos = []
    for p in esperados:
        try:
            json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            invalidos.append(str(p))
    if invalidos:
        raise ValueError(f"Arquivos com JSON inválido: {invalidos}")


def avisar_falha(context):
    ti = context["task_instance"]
    print(f"FALHA na tarefa {ti.task_id} do dia {context['ds']}")


# ----------------------------------------------------------------------------- DAG
with DAG(
    dag_id="agrofacil_coleta_dados",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 25, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "on_failure_callback": avisar_falha,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["agrofacil", "gs2026"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    coletas = [
        PythonOperator(
            task_id=f"coletar_clima_{m['codigo_ibge']}",
            python_callable=coletar_clima,
            op_kwargs={"codigo_ibge": m["codigo_ibge"]},
        )
        for m in MUNICIPIOS_PADRAO
    ]
    coletas.append(PythonOperator(task_id="coletar_ibge_municipios", python_callable=coletar_ibge_municipios))
    coletas.append(PythonOperator(task_id="gerar_lavouras_simuladas", python_callable=gerar_lavouras_simuladas))

    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    inicio >> coletas >> conferir