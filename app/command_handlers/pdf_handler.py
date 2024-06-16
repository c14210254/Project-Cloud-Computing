from app import db
from app.models.commands import CompressedPDFCommand

def handle_compress_pdf_command(filename, data):
    # Check if file already exists in DB
    existing_file = db.session.query(CompressedPDFCommand).filter_by(filename=filename).first()
    if existing_file:
        return existing_file.id  # Return the ID of the existing file record.

    # If the file doesn't exist, create a new entry in the DB
    new_file = CompressedPDFCommand(filename=filename, data=data)
    db.session.add(new_file)
    db.session.commit()
    return new_file.id  # Return the ID of the new file record.
