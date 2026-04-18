from __future__ import annotations

from collections import defaultdict
from io import BytesIO
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from Api.database import get_level_percent_map
from Api.report_growth_logic import (
    ZERO_SIGNAL_RECOMMENDATIONS,
    WEAK_SIGNAL_RECOMMENDATIONS,
    build_ai_insight_copy,
    build_competency_growth_recommendation,
    build_interpretation_basis_items,
    build_response_pattern_text,
)


class PdfReportService:
    FONT_NAME = "ArialUnicodeAgent4K"
    FONT_PATH = Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf")

    def __init__(self) -> None:
        self._font_registered = False

    def _ensure_font(self) -> None:
        if self._font_registered:
            return
        if not self.FONT_PATH.exists():
            raise FileNotFoundError(f"Font not found: {self.FONT_PATH}")
        pdfmetrics.registerFont(TTFont(self.FONT_NAME, str(self.FONT_PATH)))
        self._font_registered = True

    def _load_user(self, connection, user_id: int):
        row = connection.execute(
            """
            SELECT u.id, u.full_name, u.job_description, u.phone
            FROM users u
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()
        return dict(row) if row else None

    def _load_assessments(self, connection, user_id: int, session_id: int) -> list[dict]:
        rows = connection.execute(
            """
            SELECT
                competency_name,
                skill_code,
                skill_name,
                assessed_level_code,
                assessed_level_name,
                rationale,
                red_flags,
                found_evidence,
                block_coverage_percent,
                (
                    SELECT ROUND(AVG(scsa.artifact_compliance_percent))::int
                    FROM session_case_skill_analysis scsa
                    WHERE scsa.session_id = session_skill_assessments.session_id
                      AND scsa.user_id = session_skill_assessments.user_id
                      AND scsa.skill_id = session_skill_assessments.skill_id
                      AND scsa.artifact_compliance_percent IS NOT NULL
                ) AS artifact_compliance_percent
            FROM session_skill_assessments
            WHERE user_id = %s
              AND session_id = %s
            ORDER BY competency_name ASC, skill_code ASC NULLS LAST, skill_name ASC
            """,
            (user_id, session_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def _parse_json_array_field(self, value) -> list:
        if not value:
            return []
        if isinstance(value, list):
            return value
        import json

        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _build_signal_metrics(self, rows: list[dict]) -> dict[str, float]:
        if not rows:
            return {
                "evidence_hit_rate": 0.0,
                "avg_block_coverage": 0.0,
                "avg_artifact_compliance": 0.0,
                "avg_red_flag_count": 0.0,
            }

        evidence_hits = 0
        block_values: list[float] = []
        artifact_values: list[float] = []
        red_flag_total = 0

        for row in rows:
            if self._parse_json_array_field(row.get("found_evidence")):
                evidence_hits += 1
            if row.get("block_coverage_percent") is not None:
                block_values.append(float(row["block_coverage_percent"]))
            if row.get("artifact_compliance_percent") is not None:
                artifact_values.append(float(row["artifact_compliance_percent"]))
            red_flag_total += len(self._parse_json_array_field(row.get("red_flags")))

        return {
            "evidence_hit_rate": evidence_hits / len(rows),
            "avg_block_coverage": sum(block_values) / len(block_values) if block_values else 0.0,
            "avg_artifact_compliance": sum(artifact_values) / len(artifact_values) if artifact_values else 0.0,
            "avg_red_flag_count": red_flag_total / len(rows),
        }

    def _has_enough_interpretation_signal(self, grouped_rows: list[dict], rows: list[dict]) -> bool:
        if not grouped_rows or not any(item["avg_percent"] > 0 for item in grouped_rows):
            return False
        metrics = self._build_signal_metrics(rows)
        return (
            metrics["evidence_hit_rate"] >= 0.2
            and metrics["avg_block_coverage"] >= 25
            and metrics["avg_artifact_compliance"] >= 25
            and metrics["avg_red_flag_count"] <= 4
        )

    def _calculate_insight_score(self, item: dict) -> int:
        metrics = item.get("metrics", {})
        return round(
            item["avg_percent"] * 0.5
            + metrics.get("evidence_hit_rate", 0) * 100 * 0.2
            + metrics.get("avg_block_coverage", 0) * 0.15
            + metrics.get("avg_artifact_compliance", 0) * 0.15
            - min(metrics.get("avg_red_flag_count", 0) * 10, 40)
        )

    def _select_strongest_competency(self, grouped_rows: list[dict]) -> tuple[dict | None, bool]:
        if not grouped_rows:
            return None, False
        ranked = sorted(
            grouped_rows,
            key=lambda item: (self._calculate_insight_score(item), item["avg_percent"]),
            reverse=True,
        )
        strongest = ranked[0]
        second = ranked[1] if len(ranked) > 1 else None
        gap = self._calculate_insight_score(strongest) - self._calculate_insight_score(second) if second else 999
        is_confident = self._calculate_insight_score(strongest) >= 35 and gap >= 5
        return strongest, is_confident

    def _group_by_competency(self, rows: list[dict], level_percent_map: dict[str, int]) -> list[dict]:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            grouped[row["competency_name"] or "Без категории"].append(row)

        result: list[dict] = []
        for competency_name, skills in grouped.items():
            avg_percent = round(
                sum(level_percent_map.get(skill["assessed_level_code"], 0) for skill in skills) / len(skills)
            )
            evidence_hits = sum(1 for skill in skills if self._parse_json_array_field(skill.get("found_evidence")))
            block_values = [float(skill["block_coverage_percent"]) for skill in skills if skill.get("block_coverage_percent") is not None]
            artifact_values = [float(skill["artifact_compliance_percent"]) for skill in skills if skill.get("artifact_compliance_percent") is not None]
            red_flag_total = sum(len(self._parse_json_array_field(skill.get("red_flags"))) for skill in skills)
            result.append(
                {
                    "competency_name": competency_name,
                    "skills": skills,
                    "avg_percent": avg_percent,
                    "metrics": {
                        "evidence_hit_rate": evidence_hits / len(skills),
                        "avg_block_coverage": sum(block_values) / len(block_values) if block_values else 0.0,
                        "avg_artifact_compliance": sum(artifact_values) / len(artifact_values) if artifact_values else 0.0,
                        "avg_red_flag_count": red_flag_total / len(skills),
                    },
                }
            )
        result.sort(key=lambda item: item["competency_name"])
        return result

    def _build_recommendations(self, grouped_rows: list[dict]) -> list[str]:
        if not any(item["avg_percent"] > 0 for item in grouped_rows):
            return list(ZERO_SIGNAL_RECOMMENDATIONS)
        weakest = sorted(grouped_rows, key=lambda item: item["avg_percent"])[:3]
        recommendations = [
            build_competency_growth_recommendation(item["competency_name"], item.get("metrics", {}))
            for item in weakest
        ]
        return recommendations or ["Завершите полный цикл оценки, чтобы получить персональные рекомендации."]

    def build_pdf(self, connection, user_id: int, session_id: int) -> tuple[str, bytes]:
        self._ensure_font()

        user = self._load_user(connection, user_id)
        if user is None:
            raise ValueError("User not found")

        assessments = self._load_assessments(connection, user_id, session_id)
        if not assessments:
            raise ValueError("No skill assessments found for this session")

        level_percent_map = get_level_percent_map(connection)
        grouped_rows = self._group_by_competency(assessments, level_percent_map)
        overall_score = round(sum(item["avg_percent"] for item in grouped_rows) / len(grouped_rows))
        strongest, has_confident_strongest = self._select_strongest_competency(grouped_rows)
        has_manifested_results = any(item["avg_percent"] > 0 for item in grouped_rows)
        has_interpretation_signal = self._has_enough_interpretation_signal(grouped_rows, assessments)
        response_pattern = build_response_pattern_text(
            self._build_signal_metrics(assessments),
            has_interpretation_signal=has_interpretation_signal,
        )
        basis_items = build_interpretation_basis_items(self._build_signal_metrics(assessments))
        insight_title, insight_text = build_ai_insight_copy(
            strongest["competency_name"] if strongest else None,
            strongest["avg_percent"] if strongest else None,
            has_manifested_results=has_manifested_results,
            has_interpretation_signal=has_interpretation_signal,
            has_confident_strongest=has_confident_strongest,
            response_pattern=response_pattern,
        )
        recommendations = self._build_recommendations(grouped_rows)
        if not has_interpretation_signal:
            recommendations = list(WEAK_SIGNAL_RECOMMENDATIONS)

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "Agent4KTitle",
            parent=styles["Heading1"],
            fontName=self.FONT_NAME,
            fontSize=22,
            leading=28,
            textColor=colors.HexColor("#202334"),
            alignment=TA_LEFT,
            spaceAfter=10,
        )
        subtitle_style = ParagraphStyle(
            "Agent4KSubtitle",
            parent=styles["BodyText"],
            fontName=self.FONT_NAME,
            fontSize=10,
            leading=15,
            textColor=colors.HexColor("#6f7690"),
            spaceAfter=10,
        )
        heading_style = ParagraphStyle(
            "Agent4KHeading",
            parent=styles["Heading2"],
            fontName=self.FONT_NAME,
            fontSize=14,
            leading=18,
            textColor=colors.HexColor("#202334"),
            spaceAfter=8,
            spaceBefore=10,
        )
        body_style = ParagraphStyle(
            "Agent4KBody",
            parent=styles["BodyText"],
            fontName=self.FONT_NAME,
            fontSize=10,
            leading=14,
            textColor=colors.HexColor("#2a2f45"),
        )
        small_style = ParagraphStyle(
            "Agent4KSmall",
            parent=styles["BodyText"],
            fontName=self.FONT_NAME,
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#6f7690"),
        )

        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            leftMargin=16 * mm,
            rightMargin=16 * mm,
            topMargin=14 * mm,
            bottomMargin=14 * mm,
            title="Профиль компетенций 4K Assistant",
            author="Agent_4K",
        )

        story = [
            Paragraph("Ваш профиль компетенций", title_style),
            Paragraph(
                "Глубокий анализ оценок по четырем направлениям и детализация результатов по каждому навыку пользователя.",
                subtitle_style,
            ),
            Spacer(1, 4),
        ]

        profile_table = Table(
            [
                [
                    Paragraph(f"<b>Пользователь:</b> {user.get('full_name') or 'Не указан'}", body_style),
                    Paragraph(f"<b>Интегральный показатель:</b> {overall_score}%", body_style),
                ],
                [
                    Paragraph(f"<b>Должность:</b> {user.get('job_description') or 'Не указана'}", body_style),
                    Paragraph(f"<b>Телефон:</b> {user.get('phone') or 'Не указан'}", body_style),
                ],
            ],
            colWidths=[88 * mm, 82 * mm],
        )
        profile_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f7f8fe")),
                    ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#d9def3")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e6e9f6")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 10),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        story.extend([profile_table, Spacer(1, 10)])

        story.append(Paragraph("Сводные показатели по компетенциям", heading_style))
        competency_data = [["Компетенция", "Средний показатель", "Интерпретация"]]
        for item in grouped_rows:
            competency_data.append(
                [
                    Paragraph(item["competency_name"], body_style),
                    Paragraph(f"{item['avg_percent']}%", body_style),
                    Paragraph(
                        "Высокий потенциал" if item["avg_percent"] >= 80 else
                        "Стабильный уровень" if item["avg_percent"] >= 55 else
                        "Требует развития",
                        body_style,
                    ),
                ]
            )
        competency_table = Table(competency_data, colWidths=[72 * mm, 42 * mm, 56 * mm])
        competency_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e9ecfb")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#202334")),
                    ("FONTNAME", (0, 0), (-1, -1), self.FONT_NAME),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#d9def3")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e6e9f6")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 7),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ]
            )
        )
        story.extend([competency_table, Spacer(1, 10)])

        story.append(Paragraph("AI insights", heading_style))
        story.append(Paragraph(f"<b>{insight_title}.</b> {insight_text}", body_style))
        story.append(Spacer(1, 4))
        story.append(Paragraph("<b>Основание вывода.</b>", body_style))
        for item in basis_items:
            story.append(Paragraph("• " + item, small_style))
        story.append(Spacer(1, 8))

        story.append(Paragraph("Рекомендации по росту", heading_style))
        for recommendation in recommendations:
            story.append(Paragraph("• " + recommendation, body_style))
            story.append(Spacer(1, 2))

        for item in grouped_rows:
            story.append(Spacer(1, 8))
            story.append(Paragraph(item["competency_name"], heading_style))
            table_data = [["Навык", "Уровень", "Прогресс", "Комментарий"]]
            for skill in item["skills"]:
                percent = level_percent_map.get(skill["assessed_level_code"], 0)
                table_data.append(
                    [
                        Paragraph(skill["skill_name"], body_style),
                        Paragraph(skill["assessed_level_name"], body_style),
                        Paragraph(f"{percent}%", body_style),
                        Paragraph(skill.get("rationale") or "Оценка сформирована по рубрике и структурным признакам ответа.", small_style),
                    ]
                )
            detail_table = Table(table_data, colWidths=[58 * mm, 30 * mm, 22 * mm, 60 * mm])
            detail_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f2ff")),
                        ("FONTNAME", (0, 0), (-1, -1), self.FONT_NAME),
                        ("FONTSIZE", (0, 0), (-1, -1), 8.7),
                        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#d9def3")),
                        ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e6e9f6")),
                        ("LEFTPADDING", (0, 0), (-1, -1), 6),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ]
                )
            )
            story.append(detail_table)

        doc.build(story)

        safe_name = (user.get("full_name") or f"user-{user_id}").replace(" ", "_")
        filename = f"competency_profile_{safe_name}_session_{session_id}.pdf"
        return filename, buffer.getvalue()


pdf_report_service = PdfReportService()
