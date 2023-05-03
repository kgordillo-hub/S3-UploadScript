import os.path
import time
import boto3
from pathlib import Path


s3_client = None
bucket_name = None
files_per_batch = 4
mod_time_minutes = 5
audio_files_path = '../audios/'
audio_files_processed = '../audios/processed/'
device_name = None


def get_aws_config():
    global bucket_name, device_name, s3_client
    bucket_name = os.environ['S3_BUCKET_NAME']
    device_name = os.environ['DEVICE_NAME']
    access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    access_key_secret = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_client = boto3.client("s3",
                             aws_access_key_id=access_key_id,
                             aws_secret_access_key=access_key_secret
                             )


def subir_audios(files_path):
    try:
        get_aws_config()
        print('as', bucket_name)
        path_list = Path(files_path).glob('*.wav')
        cont = 0
        print('Inicia proceso de subir audios...')
        for path in path_list:
            path_in_str = str(path)
            last_mod_time = os.path.getmtime(path_in_str)
            current_time = time.time()
            file_name = os.path.basename(path_in_str)
            # Si el tiempo de la ultima modificacion es mayor a 5 minutos
            # Se toman lotes de 5 archivos
            if current_time - last_mod_time > mod_time_minutes * 60 and cont < files_per_batch:
                cont = cont + 1
                new_file_name = file_name.replace(" ", "_").replace("Rec", device_name)
                date = ''
                name_parts = new_file_name.split('_')
                if len(name_parts) > 2:
                    date = name_parts[2]
                device_date = 'device=' + device_name + '/date=' + date + '/'
                boto3_upload(file_path=path_in_str, file_name= device_date + new_file_name, bucket=bucket_name)
                move_file(source_file_path=path_in_str, dest_path=audio_files_processed, file_name=new_file_name)
        print('Finaliza proceso de subir audios')
    except Exception as re:
        print('Error al subir los archivos', re)


def move_file(source_file_path='', dest_path='', file_name=''):
    Path(dest_path).mkdir(exist_ok=True)
    os.replace(source_file_path, dest_path + file_name)


def boto3_upload(file_path='', file_name='', bucket=''):
    s3_client.upload_file(
        Filename=file_path,
        Bucket=bucket,
        Key=file_name,
    )


subir_audios(audio_files_path)
