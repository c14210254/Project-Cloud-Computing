from app.models.commands import CompressedPDFCommand

def handle_get_compressed_pdf_query(file_id):
    return CompressedPDFCommand.query.get(file_id)
