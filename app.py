from flask import render_template, request, send_file, redirect, url_for, jsonify
from PyPDF2 import PdfReader, PdfWriter
import io
import pika
import json
from app import app, db, cache
from app.models.commands import CompressedPDFCommand

# Set up RabbitMQ connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue for compression tasks
channel.queue_declare(queue='compression_tasks')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    uploaded_file = request.files['file']
    file_data = uploaded_file.read()
    
    # Check if file already exists in DB
    existing_file = db.session.query(CompressedPDFCommand).filter_by(filename=uploaded_file.filename).first()
    if existing_file:
        # Send task to RabbitMQ queue to cache the existing file
        task_message = {
            'task_id': existing_file.id,  # Use existing file ID
            'filename': uploaded_file.filename,
            'filedata': file_data.decode('latin-1')
        }
        channel.basic_publish(exchange='',
                              routing_key='compression_tasks',
                              body=json.dumps(task_message))
        return redirect(url_for('download_compressed_pdf', task_id=existing_file.id))

    # Create a new CompressedPDFCommand instance
    new_file = CompressedPDFCommand(filename=uploaded_file.filename, data=file_data)
    db.session.add(new_file)
    db.session.commit()
    
    # Send task to RabbitMQ queue
    task_message = {
        'task_id': new_file.id,  # Assuming 'id' is the primary key in your database model
        'filename': uploaded_file.filename,
        'filedata': file_data.decode('latin-1')
    }
    channel.basic_publish(exchange='',
                          routing_key='compression_tasks',
                          body=json.dumps(task_message))

    return redirect(url_for('download_compressed_pdf', task_id=new_file.id))

@app.route('/download/<int:task_id>')
def download_compressed_pdf(task_id):
    compressed_data = cache.get(f'file_{task_id}')
    if compressed_data:
        return send_file(
            io.BytesIO(compressed_data),
            as_attachment=True,
            attachment_filename=f"compressed_{task_id}.pdf",
            mimetype='application/pdf'
        )

    file_record = CompressedPDFCommand.query.get(task_id)
    if file_record:
        return send_file(
            io.BytesIO(file_record.data),
            as_attachment=True,
            attachment_filename=f"compressed_{file_record.filename}",
            mimetype='application/pdf'
        )
    return "File not found", 404

if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
