#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import base64
import json
import mimetypes
import time
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from typing import Iterable, List, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


BASE_URL = "https://gateway.cloud.pje.jus.br/banco-sentencas/api/sentencas/indexarOrgao"

ORGAOS_PADRAO = [
    4060, 4435, 8326, 4182, 11435,
    3012, 14141, 14142, 14143, 81647,
    81648, 3815, 6386, 4504, 84227,
    6036, 6773, 86926, 11726, 26457,
    7346, 81314, 4452, 13231, 13232,
    13234, 13235, 81338, 15661, 15662,
    15663, 17970, 29004, 29005, 5201,
]


def chunked(items: List[int], chunk_size: int) -> Iterable[List[int]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def format_api_datetime(day: date) -> str:
    dt = datetime.combine(day, dtime.min)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")


def daterange_windows(start_day: date, end_day_inclusive: date, window_days: int) -> Iterable[Tuple[date, date]]:
    """
    Gera janelas contínuas.
    Exemplo com window_days=2:
      2016-01-01 -> 2016-01-03
      2016-01-03 -> 2016-01-05
      ...
    """
    current = start_day
    while current <= end_day_inclusive:
        next_day = current + timedelta(days=window_days)
        yield current, next_day
        current = next_day


def build_url(batch_ids: List[int], start_day: date, end_day: date) -> str:
    params = {
        "idOrgaoJulgador": [str(x) for x in batch_ids],
        "dataHoraAtualizacaoInicio": format_api_datetime(start_day),
        "dataHoraAtualizacaoFim": format_api_datetime(end_day),
    }
    return f"{BASE_URL}?{urlencode(params, doseq=True)}"


def build_basic_auth_header(username: str, password: str) -> str:
    token = f"{username}:{password}".encode("utf-8")
    encoded = base64.b64encode(token).decode("ascii")
    return f"Basic {encoded}"


def ensure_min_interval(last_request_finished_at: float | None, min_interval_seconds: int) -> None:
    if last_request_finished_at is None:
        return

    elapsed = time.monotonic() - last_request_finished_at
    remaining = min_interval_seconds - elapsed
    if remaining > 0:
        print(f"[INFO] Aguardando {remaining:.1f}s...")
        time.sleep(remaining)


def request_url(url: str, timeout_seconds: int, username: str, password: str) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; indexador-pje/1.0)",
        "Accept": "*/*",
        "Authorization": build_basic_auth_header(username, password),
    }

    req = Request(url, headers=headers, method="GET")
    started_at = datetime.now()

    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read()
            status_code = resp.getcode()
            response_headers = dict(resp.headers.items())
            return {
                "ok": True,
                "status_code": status_code,
                "body": body,
                "headers": response_headers,
                "error": None,
                "requested_at": started_at.isoformat(),
            }

    except HTTPError as e:
        body = e.read() if e.fp else b""
        response_headers = dict(e.headers.items()) if e.headers else {}
        return {
            "ok": False,
            "status_code": e.code,
            "body": body,
            "headers": response_headers,
            "error": f"HTTPError: {e}",
            "requested_at": started_at.isoformat(),
        }

    except URLError as e:
        return {
            "ok": False,
            "status_code": None,
            "body": b"",
            "headers": {},
            "error": f"URLError: {e}",
            "requested_at": started_at.isoformat(),
        }

    except Exception as e:
        return {
            "ok": False,
            "status_code": None,
            "body": b"",
            "headers": {},
            "error": f"Exception: {e}",
            "requested_at": started_at.isoformat(),
        }


def try_parse_json_bytes(body: bytes):
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return None


def save_result(
    output_dir: Path,
    day_start: date,
    day_end: date,
    batch_index: int,
    batch_ids: List[int],
    url: str,
    result: dict,
) -> None:
    dir_name = f"{day_start.strftime('%Y-%m-%d')}__{day_end.strftime('%Y-%m-%d')}"
    day_dir = output_dir / dir_name
    day_dir.mkdir(parents=True, exist_ok=True)

    ids_str = "-".join(str(x) for x in batch_ids)
    base_name = (
        f"batch_{batch_index:03d}"
        f"__inicio_{day_start.strftime('%Y-%m-%d')}"
        f"__fim_{day_end.strftime('%Y-%m-%d')}"
        f"__ids_{ids_str}"
    )

    meta_path = day_dir / f"{base_name}.meta.json"

    parsed_json = try_parse_json_bytes(result["body"])
    content_type = result["headers"].get("Content-Type", "")

    if parsed_json is not None:
        body_path = day_dir / f"{base_name}.json"
        with body_path.open("w", encoding="utf-8") as f:
            json.dump(parsed_json, f, ensure_ascii=False, indent=2)
    else:
        try:
            text = result["body"].decode("utf-8")
            body_path = day_dir / f"{base_name}.txt"
            with body_path.open("w", encoding="utf-8") as f:
                f.write(text)
        except Exception:
            ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) or ".bin"
            body_path = day_dir / f"{base_name}{ext}"
            with body_path.open("wb") as f:
                f.write(result["body"])

    meta = {
        "url": url,
        "ok": result["ok"],
        "status_code": result["status_code"],
        "error": result["error"],
        "requested_at": result["requested_at"],
        "saved_body_file": body_path.name,
        "batch_index": batch_index,
        "ids": batch_ids,
        "dataHoraAtualizacaoInicio": format_api_datetime(day_start),
        "dataHoraAtualizacaoFim": format_api_datetime(day_end),
        "response_headers": result["headers"],
    }

    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def append_log(log_file: Path, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_file.open("a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Indexação do PJe por lotes de órgãos, janelas de 2 dias e Basic Auth."
    )
    parser.add_argument("--inicio", required=True, help="Data inicial no formato YYYY-MM-DD")
    parser.add_argument("--fim", required=True, help="Última data base no formato YYYY-MM-DD")
    parser.add_argument("--usuario", required=True, help="Usuário do Basic Auth")
    parser.add_argument("--senha", required=True, help="Senha do Basic Auth")
    parser.add_argument("--saida", default="./saida_indexacao_pje", help="Diretório de saída")
    parser.add_argument("--intervalo", type=int, default=1, help="Intervalo mínimo entre requisições em segundos")
    parser.add_argument("--lote", type=int, default=1, help="Quantidade de órgãos por requisição")
    parser.add_argument("--janela", type=int, default=1, help="Quantidade de dias por janela")
    parser.add_argument("--timeout", type=int, default=180, help="Timeout da requisição em segundos")
    args = parser.parse_args()

    start_day = parse_date(args.inicio)
    end_day_inclusive = parse_date(args.fim)

    if end_day_inclusive < start_day:
        raise ValueError("A data final não pode ser menor que a data inicial.")

    output_dir = Path(args.saida)
    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = output_dir / "execucao.log"

    append_log(log_file, "Início da execução")
    append_log(log_file, f"Período: {start_day} até {end_day_inclusive}")
    append_log(log_file, f"Total de órgãos: {len(ORGAOS_PADRAO)}")
    append_log(log_file, f"Tamanho do lote: {args.lote}")
    append_log(log_file, f"Janela em dias: {args.janela}")
    append_log(log_file, f"Intervalo mínimo entre requisições: {args.intervalo}s")

    last_request_finished_at = None
    total_requests = 0
    total_success = 0
    total_fail = 0

    for window_start, window_end in daterange_windows(start_day, end_day_inclusive, args.janela):
        print(f"\n=== Processando janela {window_start} -> {window_end} ===")
        append_log(log_file, f"Iniciando janela {window_start} -> {window_end}")

        for batch_index, batch_ids in enumerate(chunked(ORGAOS_PADRAO, args.lote), start=1):
            ensure_min_interval(last_request_finished_at, args.intervalo)

            url = build_url(batch_ids, window_start, window_end)

            print(f"[REQ] Janela {window_start}->{window_end} | lote {batch_index:03d} | IDs {batch_ids}")
            append_log(
                log_file,
                f"Enviando requisição | janela={window_start}->{window_end} | lote={batch_index:03d} | ids={batch_ids}"
            )

            result = request_url(
                url=url,
                timeout_seconds=args.timeout,
                username=args.usuario,
                password=args.senha,
            )
            last_request_finished_at = time.monotonic()

            save_result(
                output_dir=output_dir,
                day_start=window_start,
                day_end=window_end,
                batch_index=batch_index,
                batch_ids=batch_ids,
                url=url,
                result=result,
            )

            total_requests += 1
            if result["ok"] and result["status_code"] and 200 <= result["status_code"] < 300:
                total_success += 1
                print(f"[OK ] status={result['status_code']}")
                append_log(
                    log_file,
                    f"Sucesso | janela={window_start}->{window_end} | lote={batch_index:03d} | status={result['status_code']}"
                )
            else:
                total_fail += 1
                print(f"[ERR] status={result['status_code']} erro={result['error']}")
                append_log(
                    log_file,
                    f"Falha | janela={window_start}->{window_end} | lote={batch_index:03d} | "
                    f"status={result['status_code']} | erro={result['error']}"
                )

    append_log(
        log_file,
        f"Fim da execução | total_requests={total_requests} | success={total_success} | fail={total_fail}"
    )

    print("\n=== RESUMO ===")
    print(f"Total de requisições: {total_requests}")
    print(f"Sucessos: {total_success}")
    print(f"Falhas: {total_fail}")
    print(f"Saída: {output_dir.resolve()}")
    print(f"Log: {log_file.resolve()}")


if __name__ == "__main__":
    main()