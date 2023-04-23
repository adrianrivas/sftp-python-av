import pysftp
import json
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
import pymsteams

def connect_sftp():

    with open('connection.json', mode='r') as f:
        __config = json.load(f)
    
    hostname = __config['conexion_sftp']['hostname']
    username = __config['conexion_sftp']['username']
    password = __config['conexion_sftp']['password']
    port = __config['conexion_sftp']['port']
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    
    sftp = pysftp.Connection(hostname, username=username, password=password, port=port, private_key=".ppk", cnopts=cnopts)
    return sftp

def search_files_sftp(sftp):

    url = '/ftp/DataSensible/LabData/'
    now = datetime.now().strftime('%Y%m%d')

    files_structure_one = ['RESERVAS_ACTIVAS_BF_GLOBAL', 'TIPS_DE_VIAJE_EASYFLY']
    files_structure_two = ['TipsViaje']

    foundfiles = []
    notfoundfiles = []

    for file in files_structure_one:
        if sftp.exists('{}{}_{}.csv'.format(url, now, file)):
            file_stats = sftp.lstat('{}{}_{}.csv'.format(url, now, file))
            filename = '{}_{}.csv'.format(now, file)
            filesize = '{}MB'.format(round(file_stats.st_size / 1000000, 2))
            lastmod = datetime.fromtimestamp(file_stats.st_mtime)
            filelastmod = '{}-{}-{} {}:{}:{}'.format(lastmod.year, lastmod.month, lastmod.day, lastmod.hour, lastmod.minute, lastmod.second)
            foundfiles.append([filename, filesize, filelastmod])
        else:
            filename = '{}_{}.csv'.format(now, file)
            notfoundfiles.append(filename)

    for file in files_structure_two:
        if sftp.exists('{}{}{}.csv'.format(url, file, now)):
            file_stats = sftp.lstat('{}{}{}.csv'.format(url, file, now))
            filename = '{}{}.csv'.format(file, now)
            filesize = '{}MB'.format(round(file_stats.st_size / 1000000, 2))
            lastmod = datetime.fromtimestamp(file_stats.st_mtime)
            filelastmod = '{}-{}-{} {}:{}:{}'.format(lastmod.year, lastmod.month, lastmod.day, lastmod.hour, lastmod.minute, lastmod.second)
            foundfiles.append([filename, filesize, filelastmod])
        else:
            filename = '{}{}.csv'.format(file, now)
            notfoundfiles.append(filename)

    return foundfiles, notfoundfiles

def create_message(foundfiles, notfoundfiles):
    today = datetime.now().strftime('%Y-%m-%d')

    title = """ Files Found in SFTP Today {} """.format(today)
    message = """"""
    if len(foundfiles) > 0:
        for file in foundfiles:
            message += """
            File: {}
            Size: {}
            Last Modification Time: {}
            """.format(file[0], file[1], file[2])
    else:
        message = """
            None
            """

    title_not_found = """ Files Not Found in SFTP Today {} """.format(today)
    message_not_found = """"""
    if len(notfoundfiles) > 0:
        for file in notfoundfiles:
            message_not_found += """
            File: {}
            """.format(file)
    else:
        message_not_found = """
            None
            """

    final_message = """"""
    final_message = """
    {}
    {}
    {}
    {}
    """.format(title, message, title_not_found, message_not_found)

    return final_message

def send_message(final_message):
    myTeamsMessage = pymsteams.connectorcard("https://grupovass.webhook.office.com/webhookb2/2a245bb7-faad-43d8-bc00-7b186b956db9@b716c11f-16a3-4d15-8dbc-f11f7fdefe5a/IncomingWebhook/2e9a73b9317344989346b1d5107735ae/47faf4da-56ba-4553-a940-852d7cef8833")
    myTeamsMessage.text(final_message)
    if myTeamsMessage.send():
        print('Envío realizado exitosamente a Teams')

def close_connection_sftp(sftp):
    sftp.close()

if __name__ == '__main__':
    connection = connect_sftp()
    foundfiles, notfoundfiles = ([] for i in range(2))
    foundfiles, notfoundfiles = search_files_sftp(connection)
    final_message = create_message(foundfiles, notfoundfiles)
    send_message(final_message)
    close_connection_sftp(connection)
