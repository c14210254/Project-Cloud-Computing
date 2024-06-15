from app import db
from app.models.commands import CompressedPDFCommand

def handle_compress_pdf_command(filename, data):
    new_file = CompressedPDFCommand(filename=filename, data=data)
    db.session.add(new_file)
    db.session.commit()
    return new_file.id  # Return the ID of the new file record.
