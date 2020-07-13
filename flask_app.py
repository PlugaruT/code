from datetime import datetime

from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
import model
import orm
import repository
import services


orm.start_mappers()
get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))
app = Flask(__name__)

@app.route('/add_batch', methods=['POST'])
def add_batch():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    print(request.json)
    batch = model.Batch(
        request.json['batchref'],
        request.json['sku'],
        request.json['qty'],
        datetime.strptime(request.json['eta'], '%Y-%m-%d')
    )
    try:
        services.add_batch(batch, repo, session)
    except Exception as e:
        return jsonify({"message": str(e)}), 400
    return jsonify({'batchref': batch.reference}), 201

@app.route("/allocate", methods=['POST'])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(
        request.json['orderid'],
        request.json['sku'],
        request.json['qty'],
    )
    try:
        batchref = services.allocate(line, repo, session)
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({'message': str(e)}), 400

    return jsonify({'batchref': batchref}), 201

@app.route("/deallocate", methods=['POST'])
def deallocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(request.json['orderid'], request.json['sku'], request.json['qty'])

    try:
        batchref = services.deallocate(line, repo, session)
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({'message': str(e)}), 400
    
    return jsonify({'batchid': batchref}), 201