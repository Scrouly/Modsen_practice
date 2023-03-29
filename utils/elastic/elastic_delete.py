from utils.elastic.elastic_connection import connect_elasticsearch


def delete_by_id_elastic(es_object, index_name, id):
    """"Удаляет документ по его id из elastic"""
    try:

        if es_object.exists(index=index_name, id=id):
            es_object.delete(index=index_name, id=id)
            return f'ES|The document with id {id} was deleted'
        else:
            return f'ES|Error: document with id {id} does not exist'
    except Exception as er:
        print(f"ES|The document was not deleted, error: {er.args[0]}")
        return {f"ES|The document was not deleted, error: {er.args[0]}"}


if __name__ == "__main__":
    es = connect_elasticsearch()
    delete_by_id_elastic(es, 'documents', 3)
