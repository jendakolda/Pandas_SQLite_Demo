# Write your code here
import sqlite3
import openpyxl
import numpy as np
import pandas as pd
from lxml import etree as et
from lxml import objectify


def filter_nums(string: str):
    return ''.join(filter(lambda x: x.isnumeric(), string))  # orig fcn


def get_score(engine_capacity: int, fuel_consumption: int, maximum_load: int):
    points: int = 0
    DISTANCE = 450

    pit_stops: float = (DISTANCE * fuel_consumption / 100) / engine_capacity
    if pit_stops < 1:
        points += 2
    elif 1 <= pit_stops < 2:
        points += 1
    fuel_consumed: float = DISTANCE * fuel_consumption / 100
    points = points + 2 if fuel_consumed <= 230 else points + 1
    points = points + 2 if maximum_load >= 20 else points
    return points


def sql_string(header: list):
    string = f'create table convoy({header[0]} INTEGER primary key,'
    for i in header[1:]:
        string += f' {i}  INTEGER not null,'
    string = string.rstrip(',')
    string += ');'
    return string


def create_xml():
    conn = sqlite3.connect(file.rstrip('[CHECKED]') + '.s3db')
    my_df = pd.read_sql_query(f'''select vehicle_id, engine_capacity, fuel_consumption, maximum_load
     from convoy WHERE score <= 3''', con=conn)

    root = et.Element('convoy')
    for row in my_df.iterrows():
        vehicle = et.SubElement(root, 'vehicle')
        et.SubElement(vehicle, 'vehicle_id').text = str(row[1]['vehicle_id'])
        et.SubElement(vehicle, 'engine_capacity').text = str(row[1]['engine_capacity'])
        et.SubElement(vehicle, 'fuel_consumption').text = str(row[1]['fuel_consumption'])
        et.SubElement(vehicle, 'maximum_load').text = str(row[1]['maximum_load'])

    if root.text is None:
        root.text = ''
    objectify.deannotate(root)
    et.cleanup_namespaces(root)
    obj_xml = et.tostring(root, pretty_print=True)
    with open(file.rstrip('[CHECKED]') + ".xml", "wb") as f:
        f.write(obj_xml)
    conn.close()

    msg = 'vehicle was' if my_df.shape[0] == 1 else 'vehicles were'
    print('{} {} saved into {}.xml'.format(my_df.shape[0], msg, file.rstrip('[CHECKED]')))


def create_json():
    conn = sqlite3.connect(file.rstrip('[CHECKED]') + '.s3db')
    my_df = pd.read_sql_query('''select vehicle_id, engine_capacity, fuel_consumption, maximum_load
     from convoy WHERE score > 3''', con=conn)
    conn.close()
    result = my_df.to_json(orient="records", indent=4)

    with open(file.rstrip('[CHECKED]') + '.json', 'w') as f:
        f.write('{\n"convoy": ')
        f.write(result)
        f.write('\n}')

    msg = 'vehicle was' if my_df.shape[0] == 1 else 'vehicles were'
    print('{} {} saved into {}.json'.format(my_df.shape[0], msg, file.rstrip('[CHECKED]')))


def create_sqldb():
    my_df = pd.read_csv(file.rstrip('[CHECKED]') + '[CHECKED].csv', dtype=str)
    my_df = my_df.applymap(int)
    my_df['score'] = np.vectorize(get_score)(my_df["engine_capacity"], my_df["fuel_consumption"], my_df['maximum_load'])
    conn = sqlite3.connect(file.rstrip('[CHECKED]') + '.s3db')
    my_df.to_sql(file + '.s3db', index=False, con=conn)
    header = list(my_df.columns.values)

    cur = conn.cursor()
    cur.execute(sql_string(header))
    cur.executemany("insert into convoy values (?, ?, ?, ?, ?);", my_df.values.tolist())

    conn.commit()
    conn.close()
    msg = 'record was' if my_df.shape[0] == 1 else 'records were'
    print('{} {} inserted into {}.s3db'.format(my_df.shape[0], msg, file.rstrip('[CHECKED]')))
    create_json()
    create_xml()


def csv_corrector():
    old_df = pd.read_csv(file + '.csv', dtype=str)
    new_df = old_df.applymap(filter_nums)
    difference_locations = np.where(old_df != new_df)
    counter = len(difference_locations[0])
    msg = 'cell was' if counter == 1 else 'cells were'
    print('{} {} corrected in {}[CHECKED].csv'.format(counter, msg, file))
    new_df.to_csv(file + '[CHECKED].csv', header=True, index=False)
    create_sqldb()


def file_import():
    my_df = pd.read_excel(file + '.xlsx', sheet_name='Vehicles', dtype=str)
    msg = 'line was' if my_df.shape[0] == 1 else 'lines were'
    print('{} {} imported to {}.csv'.format(my_df.shape[0], msg, file))
    my_df.to_csv(file + '.csv', index=False)
    csv_corrector()


def main():
    if extension == 's3db':
        create_json()
        create_xml()
    elif file.endswith('[CHECKED]') and extension == 'csv':
        create_sqldb()
    elif extension == 'csv':
        csv_corrector()
    elif extension == 'xlsx':
        file_import()
    else:
        quit('Unknown file format')


if __name__ == '__main__':
    # file, extension = input('Input file name\n').rsplit('.')
    file, extension = 'data_final_xlsx.xlsx'.rsplit('.')
    main()
