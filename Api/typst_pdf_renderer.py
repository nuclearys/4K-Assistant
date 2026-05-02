from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


class TypstRenderError(RuntimeError):
    pass


class TypstUnavailableError(TypstRenderError):
    pass


def _typst_binary() -> str:
    configured_binary = os.getenv("AGENT4K_TYPST_BIN")
    if configured_binary:
        return configured_binary
    discovered_binary = shutil.which("typst")
    if discovered_binary:
        return discovered_binary
    raise TypstUnavailableError("Typst binary was not found")


def _render_with_python_package(source_path: Path, temp_dir: Path) -> bytes:
    try:
        import typst
    except ImportError as exc:
        raise TypstUnavailableError("Typst Python package was not found") from exc

    try:
        return typst.compile(str(source_path), root=str(temp_dir))
    except Exception as exc:
        raise TypstRenderError(str(exc)) from exc


def _render_with_cli(source_path: Path, output_path: Path, temp_dir: Path, timeout_seconds: float) -> bytes:
    typst_binary = _typst_binary()
    try:
        result = subprocess.run(
            [
                typst_binary,
                "compile",
                "--root",
                str(temp_dir),
                str(source_path),
                str(output_path),
            ],
            cwd=temp_dir,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise TypstUnavailableError(f"Typst binary was not found: {typst_binary}") from exc
    except subprocess.TimeoutExpired as exc:
        raise TypstRenderError(f"Typst rendering timed out after {timeout_seconds:g}s") from exc
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "Typst failed without output").strip()
        raise TypstRenderError(message)
    if not output_path.exists():
        raise TypstRenderError("Typst did not produce a PDF output file")
    return output_path.read_bytes()


def render_typst_report(payload: dict[str, Any], template_name: str) -> bytes:
    engine = os.getenv("AGENT4K_PDF_ENGINE", "auto").strip().lower()
    if engine not in {"auto", "typst"}:
        raise TypstRenderError(f"Unsupported AGENT4K_PDF_ENGINE value: {engine}")

    template_path = Path(__file__).with_name("pdf_templates") / template_name
    if not template_path.exists():
        raise TypstRenderError(f"Typst template not found: {template_path}")

    timeout_seconds = float(os.getenv("AGENT4K_TYPST_TIMEOUT_SECONDS", "20"))
    with tempfile.TemporaryDirectory(prefix="agent4k-typst-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        source_path = temp_dir / template_name
        data_path = temp_dir / "report-data.json"
        output_path = temp_dir / "report.pdf"

        shutil.copyfile(template_path, source_path)
        data_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        try:
            return _render_with_python_package(source_path, temp_dir)
        except TypstUnavailableError:
            return _render_with_cli(source_path, output_path, temp_dir, timeout_seconds)


def render_typst_competency_report(payload: dict[str, Any]) -> bytes:
    return render_typst_report(payload, "competency_report.typ")


def render_typst_admin_reports(payload: dict[str, Any]) -> bytes:
    return render_typst_report(payload, "admin_reports.typ")


def render_typst_admin_dialogue(payload: dict[str, Any]) -> bytes:
    return render_typst_report(payload, "admin_dialogue.typ")
