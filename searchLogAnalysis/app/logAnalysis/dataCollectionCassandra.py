from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def get_cass_instance():
    auth_provider = PlainTextAuthProvider(
        username='cassread', password='re@dcaSs')
    cluster = Cluster(['18.139.106.59'], auth_provider=auth_provider)
    session = cluster.connect('products')
    return session


def get_brand_id_name_dict():
    cass_session = get_cass_instance()
    brand_id_name_dict = {}
    rows = cass_session.execute('SELECT id_brand,brand_name from products.brand_details where ')
    for data in rows:
        brand_id_name_dict[str(data[1].lower())] = data[0]
        # print(brand_id_name_dict)
    return brand_id_name_dict


def get_category_id_name_dict():
    cass_session = get_cass_instance()
    category_id_name_dict = {}
    rows = cass_session.execute('SELECT category_code,category_name from products.category_details')
    for data in rows:
        category_id_name_dict[str(data[1].lower())] = data[0]
        # print(brand_id_name_dict)
    return category_id_name_dict



