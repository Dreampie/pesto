import datetime

from flask import Blueprint, jsonify, request

from model.example import Example
from repository.example import ExampleRepository

app_example = Blueprint('example', __name__, url_prefix='/examples')

example_repository = ExampleRepository()


@app_example.route('', methods=['GET'])
def examples():
    # 条件查询
    data = example_repository.query_by(where="")
    if len(data) <= 0:
        jsonify(error="not found any data"), 404

    return jsonify(data)


@app_example.route('/<id>', methods=['GET'])
def example(id):
    # 条件查询
    data = example_repository.query_first_by(where="`id`= %s", params=(id,))
    if data is None:
        jsonify(error="not found any data"), 404

    return jsonify(data)


@app_example.route('', methods=['POST'])
def save():
    data = request.get_json()
    if data is not None and len(data) > 0:
        example = Example()
        example.set_attrs(data)
        example.created_at = datetime.datetime.now()
        example.updated_at = datetime.datetime.now()
        # 保存数据
        example.save()
        return jsonify(example.id)
    else:
        return jsonify(error="not found any data to save"), 400


@app_example.route('/<id>', methods=['DELETE'])
def delete(id):
    result = True
    example = Example()
    example.id = id
    example.deleted_at = datetime.datetime.now()
    # 根据id删除数据
    result = example.delete() > 0
    return jsonify(result)


@app_example.route('/<id>', methods=['PUT'])
def update(id):
    result = True
    data = request.get_json()
    example = Example()
    example.set_attrs(data)
    example.id = id
    example.updated_at = datetime.datetime.now()
    # 根据id更新数据
    result = example.update() > 0
    return jsonify(result)
