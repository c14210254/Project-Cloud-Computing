from flask import render_template, request, send_file, redirect, url_for
from PyPDF2 import PdfReader, PdfWriter
import io

from app import app, db
from app.command_handlers.pdf_handler import handle_compress_pdf_command
from app.query_handlers.pdf_handler import handle_get_compressed_pdf_query

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        pdf_reader = PdfReader(uploaded_file)
        pdf_writer = PdfWriter()
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            pdf_writer.add_page(page)
        compressed_pdf = io.BytesIO()
        pdf_writer.write(compressed_pdf)
        compressed_pdf.seek(0)

        file_id = handle_compress_pdf_command('compressed_' + uploaded_file.filename, compressed_pdf.read())

        return redirect(url_for('download_file', file_id=file_id))

    return 'No file has been uploaded.', 400

@app.route('/download/<int:file_id>')
def download_file(file_id):
    file_data = handle_get_compressed_pdf_query(file_id)
    if file_data:
        return send_file(
            io.BytesIO(file_data.data),
            as_attachment=True,
            attachment_filename=file_data.filename,
            mimetype='application/pdf'
        )
    return 'File not found.', 404

if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
