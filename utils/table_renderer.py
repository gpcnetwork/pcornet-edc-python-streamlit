class TableRenderer:
    """Renders Snowpark Row results (or plain dicts) as styled HTML tables."""

    @staticmethod
    def _to_dicts(records) -> list:
        """Normalize Snowpark Row objects or plain dicts into a list of dicts."""
        if not records:
            return []
        if isinstance(records[0], dict):
            return records
        return [{col: getattr(row, col) for col in row._fields} for row in records]

    @staticmethod
    def generate_html_table(records, red_columns) -> str:
        records = TableRenderer._to_dicts(records)
        if not records:
            return ""
        tr = ''
        for row in records:
            row_html = '<tr>'
            for col, value in row.items():
                style = 'border: 1px solid rgba(128,128,128,0.4); padding: 8px; text-align: center;'
                if col in red_columns and value is not None and value < -5:
                    style += ' color: red;'
                row_html += f'<td style="{style}">{value}</td>'
            row_html += '</tr>'
            tr += row_html

        headers = ''.join([
            f'<th style="border: 1px solid rgba(128,128,128,0.4); padding: 8px; '
            f'text-align: center; background-color: #4CAF50; color: white; width: 450px;">{col}</th>'
            for col in records[0]
        ])
        return f'<table style="width:100%; border-collapse: collapse;">{headers}{tr}</table>'

    @staticmethod
    def generate_generic_table(records, red_columns, ignore_columns, bold_columns, red_value) -> str:
        records = TableRenderer._to_dicts(records)
        if not records:
            return "<p><em>No results returned.</em></p>"
        tr = ''
        for row in records:
            row_html = '<tr style="border-bottom: 1px solid rgba(128,128,128,0.3);">'
            for col, value in row.items():
                if col in ignore_columns:
                    continue
                extra_style = ''
                if col in red_columns and value is not None:
                    if (isinstance(value, str) and value == red_value) or \
                       (not isinstance(value, str) and value > red_value):
                        extra_style += ' color: red;'
                if col in bold_columns and value is not None:
                    extra_style += ' font-weight: bold;'
                row_html += (
                    f'<td style="border: 1px solid rgba(128,128,128,0.3); padding: 8px; '
                    f'text-align: center; white-space: nowrap;{extra_style}">'
                    f'{value}</td>'
                )
            row_html += '</tr>'
            tr += row_html

        headers = ''.join([
            f'<th style="border: 1px solid rgba(128,128,128,0.3); padding: 8px; '
            f'text-align: center; background-color: #4CAF50; color: white; '
            f'position: sticky; top: 0; white-space: nowrap;">{col}</th>'
            for col in records[0] if col not in ignore_columns
        ])
        return (
            '<div style="width:100%; max-width:100%; overflow-x:auto;'
            ' -webkit-overflow-scrolling:touch; margin:25px 0;">'
            '<table style="border-collapse:collapse; width:max-content;'
            ' min-width:100%; font-size:14px;">'
            f'<thead><tr>{headers}</tr></thead>'
            f'<tbody>{tr}</tbody>'
            '</table></div>'
        )
