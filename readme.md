Python ORM and Utils 
====

python下的极简orm框架，核心思想，领域对象+仓库
领域对象：领域模型对应的属性和行为
仓库：批量操作领域对象，或者特殊的一些数据操作逻辑
领域服务：统筹领域模型的行为，或者更复杂的单个模型无法完成的行为

只需要配置数据库相关的参数，通过领域模型，或者仓库即可操作数据，简单易用，业务逻辑复杂可以加入领域服务概念

pesto-example(flask + pesto-orm)
  
add dependencies in requirements(重要):
```text
pesto-orm==0.0.1
mysql-connector-python==8.0.11
Flask==1.0.2
```

add config in config.ini(重要):
```ini
[DEFAULT]
app.key = pesto-orm
log.path = /opt/logs/pesto-orm/pesto-orm.log
log.level = INFO
; db config
db.database = example
db.raise_on_warnings = True
db.charset = utf8mb4
db.show_sql = True

; profiles config for env
[local]

db.user = root
db.password =
db.host = 127.0.0.1
db.port = 3306

[dev]

[test]

[prod]

```

run with env(default is local, dev, test, prod)
```bash
env=$ENV python ./pesto_example/main.py >> std_out.log 2>&1
```

main 
```python
@app.route('/')
def index():
    data = {'name': 'pesto-example'}
    return jsonify(data)


if __name__ == '__main__':
    port = 8080
    try:
        app.run(host='0.0.0.0', port=port)
    except (KeyboardInterrupt, SystemExit):
        print('')
        logger.info('Program exited.')
    except (Exception,):
        logger.error('Program exited. error info:\n')
        logger.error(traceback.format_exc())
        sys.exit(0)
```

model 模型创建，只需要配置对应的表名和主键，领域模型的行为可以扩展到该类
```python
class Example(MysqlBaseModel):
    def __init__(self):
        super(Example, self).__init__(table_name='example', primary_key='id')

```

repository 依赖于模型，执行批量或者复杂的sql逻辑
```python
class ExampleRepository(MysqlBaseRepository):
    def __init__(self):
        super(ExampleRepository, self).__init__(Example)
```

router api的路由信息，数据操作方式
```python
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
    example.delete()
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
    example.update()
    return jsonify(result)

```

创建数据库
```sql
create database example;

create table example(
    id INT UNSIGNED AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    deleted_at DATETIME NOT NULL,
    PRIMARY KEY(id)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

测试
```bash
# 查询全部数据
curl -X GET \
  http://localhost:8080/examples

# 添加一条数据
curl -X POST \
  http://localhost:8080/examples \
  -H 'content-type: application/json' \
  -d '{
	"title":"第三个测试"
}'

# 根据id查询
curl -X GET \
  http://localhost:8080/examples/1
  
# 根据id 更新数据
curl -X PUT \
  http://localhost:8080/examples/1 \
  -H 'content-type: application/json' \
  -d '{
	"title":"这是第一个已改测试"
}'

# 根据id删除数据
curl -X DELETE \
  http://localhost:8080/examples/3

```