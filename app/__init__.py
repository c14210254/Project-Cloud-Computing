from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_caching import Cache

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///pdf_files.db'

# Configure cache
app.config['CACHE_TYPE'] = 'SimpleCache'
app.config['CACHE_DEFAULT_TIMEOUT'] = 300
db = SQLAlchemy(app)
cache = Cache(app)
