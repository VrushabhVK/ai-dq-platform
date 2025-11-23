# dqtool/reports.py
"""
Generate simple PDF validation reports.

Requires: reportlab
    pip install reportlab
"""

from io import BytesIO
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors


def generate_pdf_report_bytes(df, profile_df, dq_score, rules):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    # Title
    story.append(Paragraph("Data Quality Report", styles["Title"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"Generated at: {datetime.utcnow().isoformat()} UTC", styles["Normal"]))
    story.append(Spacer(1, 12))

    # Summary
    story.append(Paragraph(f"Rows: {len(df)}", styles["Normal"]))
    story.append(Paragraph(f"Columns: {len(df.columns)}", styles["Normal"]))
    story.append(Paragraph(f"Data Quality Score: {dq_score} / 100", styles["Normal"]))
    story.append(Spacer(1, 12))

    # Profile table (top 15 columns)
    story.append(Paragraph("Column Profiling (truncated)", styles["Heading2"]))
    story.append(Spacer(1, 6))
    pdf_profile = profile_df.head(15).fillna("").astype(str)
    data = [list(pdf_profile.columns)] + pdf_profile.values.tolist()
    table = Table(data, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 12))

    # Rules
    story.append(Paragraph("Active Rules", styles["Heading2"]))
    if not rules:
        story.append(Paragraph("No rules configured.", styles["Normal"]))
    else:
        for r in rules:
            text = f"Column: <b>{r.get('column')}</b> — Rule: {r.get('rule')} (conf: {r.get('confidence', 'N/A')})"
            story.append(Paragraph(text, styles["Normal"]))
            story.append(Spacer(1, 4))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes
