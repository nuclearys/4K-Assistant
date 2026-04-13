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


class PdfReportService:
    FONT_NAME = "ArialUnicodeAgent4K"
    FONT_PATH = Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf")
    LEVEL_PERCENT = {
        "L1": 45,
        "L2": 70,
        "L3": 92,
        "N/A": 12,
    }

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
                rationale
            FROM session_skill_assessments
            WHERE user_id = %s
              AND session_id = %s
            ORDER BY competency_name ASC, skill_code ASC NULLS LAST, skill_name ASC
            """,
            (user_id, session_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def _group_by_competency(self, rows: list[dict]) -> list[dict]:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            grouped[row["competency_name"] or "Без категории"].append(row)

        result: list[dict] = []
        for competency_name, skills in grouped.items():
            avg_percent = round(
                sum(self.LEVEL_PERCENT.get(skill["assessed_level_code"], 0) for skill in skills) / len(skills)
            )
            result.append(
                {
                    "competency_name": competency_name,
                    "skills": skills,
                    "avg_percent": avg_percent,
                }
            )
        result.sort(key=lambda item: item["competency_name"])
        return result

    def _build_recommendations(self, grouped_rows: list[dict]) -> list[str]:
        weakest = sorted(grouped_rows, key=lambda item: item["avg_percent"])[:3]
        recommendations: list[str] = []
        for item in weakest:
            competency = item["competency_name"]
            if competency == "Коммуникация":
                recommendations.append("Усилить коммуникацию: чаще фиксировать позицию, вопросы и договоренности в явном виде.")
            elif competency == "Командная работа":
                recommendations.append("Усилить командную работу: показывать распределение ролей, синхронизацию и поддержку участников.")
            elif competency == "Креативность":
                recommendations.append("Усилить креативность: предлагать альтернативы, пилоты и нестандартные варианты решений.")
            else:
                recommendations.append("Усилить критическое мышление: добавлять критерии, риски, гипотезы и проверку решений.")
        return recommendations or ["Завершите полный цикл оценки, чтобы получить персональные рекомендации."]

    def build_pdf(self, connection, user_id: int, session_id: int) -> tuple[str, bytes]:
        self._ensure_font()

        user = self._load_user(connection, user_id)
        if user is None:
            raise ValueError("User not found")

        assessments = self._load_assessments(connection, user_id, session_id)
        if not assessments:
            raise ValueError("No skill assessments found for this session")

        grouped_rows = self._group_by_competency(assessments)
        overall_score = round(sum(item["avg_percent"] for item in grouped_rows) / len(grouped_rows))
        strongest = max(grouped_rows, key=lambda item: item["avg_percent"])
        recommendations = self._build_recommendations(grouped_rows)

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
        story.append(
            Paragraph(
                f"<b>Сильная сторона — {strongest['competency_name']}.</b> "
                f"Наиболее выраженный показатель зафиксирован по направлению «{strongest['competency_name']}». "
                f"Средний интегральный результат по связанным навыкам составил {strongest['avg_percent']}%.",
                body_style,
            )
        )
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
                percent = self.LEVEL_PERCENT.get(skill["assessed_level_code"], 0)
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
