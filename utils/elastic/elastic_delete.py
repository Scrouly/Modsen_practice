from utils.elastic.elastic_connection import connect_elasticsearch


def delete_by_id_elastic(es_object, index_name, id):
    """"Удаляет документ по его id из elastic"""
    try:
        es_object.delete(index=index_name, id=id)
        print('ES|The document was deleted')
    except Exception as er:
        print(f"ES|The document was not deleted, error: {er.args[0]}")
    finally:
        es.close()


if __name__ == "__main__":
    es = connect_elasticsearch()
    delete_by_id_elastic(es, 'documents', 4)
