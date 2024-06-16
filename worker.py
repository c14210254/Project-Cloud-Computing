import pika
import json
import io
from PyPDF2 import PdfReader, PdfWriter
from app import db
from app.models.commands import CompressedPDFCommand
from cachelib import SimpleCache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

cache = SimpleCache()

def compress_pdf(filedata):
    logging.info("Compressing PDF")
    pdf_reader = PdfReader(io.BytesIO(filedata))
    pdf_writer = PdfWriter()
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        pdf_writer.add_page(page)
    compressed_pdf = io.BytesIO()
    pdf_writer.write(compressed_pdf)
    compressed_pdf.seek(0)
    return compressed_pdf.getvalue()

def store_compressed_pdf(task_id, filename, compressed_data):
    logging.info(f"Storing compressed PDF: {filename}")
    # Check if file already exists in DB
    existing_file = db.session.query(CompressedPDFCommand).filter_by(filename=filename).first()
    if existing_file:
        logging.info(f"File already exists in DB, caching it: {filename}")
        # If the file exists, cache it instead of storing it again
        cache.set(f'file_{task_id}', compressed_data)
    else:
        logging.info(f"File does not exist in DB, storing and caching it: {filename}")
        # If the file doesn't exist, create a new entry in the DB and cache it
        new_file = CompressedPDFCommand(id=task_id, filename=filename, data=compressed_data)
        db.session.add(new_file)
        db.session.commit()
        cache.set(f'file_{task_id}', compressed_data)

def on_compression_task(ch, method, properties, body):
    try:
        logging.info("Received a compression task")
        task_data = json.loads(body)
        logging.info(f"Task data: {task_data}")

        # Check if all required keys are present
        if 'task_id' not in task_data or 'filename' not in task_data or 'filedata' not in task_data:
            raise KeyError('Missing one or more required keys: task_id, filename, filedata')

        # Decode the file data from string back to binary
        filedata = task_data['filedata'].encode('latin-1')

        # Compress the PDF
        compressed_data = compress_pdf(filedata)

        # Store the compressed PDF in the database and cache
        store_compressed_pdf(task_data['task_id'], task_data['filename'], compressed_data)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Task processed successfully")
    except KeyError as e:
        logging.error(f"Error: {e}")
        # Reject the message and do not requeue it
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        # Reject the message and do not requeue it
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Set up RabbitMQ connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue for compression tasks
channel.queue_declare(queue='compression_tasks')

# Start consuming messages from the queue
channel.basic_consume(queue='compression_tasks',
                      on_message_callback=on_compression_task)

logging.info(' [*] Waiting for compression tasks. To exit press CTRL+C')
channel.start_consuming()
