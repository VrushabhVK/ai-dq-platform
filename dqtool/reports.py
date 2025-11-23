# dqtool/reports.py
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from io import BytesIO
import datetime


def generate_pdf_report_bytes(df, profile_df, dq_score, rules):
    """
    Generate a complete PDF report as bytes.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer)

    styles = getSampleStyleSheet()
    story = []

    # Title
    title = Paragraph("AI Data Quality Report", styles["Title"])
    story.append(title)
    story.append(Spacer(1, 20))

    # Timestamp
    ts = Paragraph(f"Generated on: {datetime.datetime.now()}", styles["Normal"])
    story.append(ts)
    story.append(Spacer(1, 20))

    # Score
    score = Paragraph(f"<b>Data Quality Score:</b> {dq_score} / 100", styles["Heading2"])
    story.append(score)
    story.append(Spacer(1, 20))

    # Profiling Table
    story.append(Paragraph("Profiling Summary", styles["Heading2"]))
    profile_table = Table([profile_df.columns.tolist()] + profile_df.values.tolist())

    profile_table.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.gray),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ])
    )
    story.append(profile_table)
    story.append(Spacer(1, 20))

    # Rules
    story.append(Paragraph("Applied / Suggested Rules", styles["Heading2"]))
    for r in rules:
        story.append(Paragraph(f"• <b>{r.get('column')}:</b> {r.get('rule')}", styles["Normal"]))

    doc.build(story)
    return buffer.getvalue()
