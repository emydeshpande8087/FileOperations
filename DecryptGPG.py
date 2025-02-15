import base64
import json
import gnupg 
import sys,os

from UtilityFunctions import sm,env_cpu_count, register_to_snowflake,upload_manager,get_list_of_files_from_s3,get_logger,register_to_dynamo,update_into_dynamo,download_manager,local_path
from concurrent.futures import ProcessPoolExecutor
# Get a logger for the current script
log = get_logger(__name__)


def StartEncrypting(local_file_name,secret_name):
    try:
        log.info("Starting Encryption of :%s",local_file_name)
        get_secret_value_response = sm.get_secret_value(SecretId=secret_name)
        secret = dict(json.loads(get_secret_value_response['SecretString']))
        pub_path=secret['public_key']
        decoded_string = base64.b64decode(pub_path).decode("utf-8")
        pub_key=str(decoded_string).strip()
        gpg = gnupg.GPG(gpgbinary="/usr/bin/gpg", gnupghome="/tmp")
        import_result = gpg.import_keys(pub_key)
        if import_result.count > 0:
            key_id = import_result.fingerprints[0]
            gpg.trust_keys(import_result.fingerprints,'TRUST_ULTIMATE')
            tmpOutputFilename=os.path.join(local_path,local_file_name.replace('.','_')+'.pgp')
            log.info("Output File name will be : %s",tmpOutputFilename)
            with open(local_file_name,'rb') as f:
                encrypted_data = gpg.encrypt_file(f, armor=True, recipients=[key_id], always_trust=True)
            if encrypted_data.ok:
                with open(tmpOutputFilename, 'wb') as encrypted_file:
                        encrypted_file.write(str(encrypted_data).encode('utf-8'))
                log.info(f"{local_file_name} encrypted successfully to {tmpOutputFilename}")
                log.info("Removing residuals..")
                os.remove(os.path.join(local_path,local_file_name))
            else:
                raise Exception(f"Failed to create encrypted string for the file content of {local_file_name}")        
    except Exception as error:
        raise error

def EncryptFiles(**kwargs):
    try:
        i_secret_name=kwargs['secret_name']
        p_task_id=kwargs['task_id']
        log.info("Secret Name is : %s",i_secret_name)
        file_q=os.listdir(local_path)
        log.info("List of files in processing queue: %s" , file_q)
        with ProcessPoolExecutor(max_workers=env_cpu_count) as ppe:
            futures = [ppe.submit(StartEncrypting, local_file_name=os.path.join(local_path,onefile),secret_name=i_secret_name) for onefile in file_q]
            for future in futures:
                future.result()  # Wait for each task to complete            
        log.info("Files ready for Upload are : %s", os.listdir(local_path))
    except Exception as e:
        error_message=str(e)
        log.error(f"Failed to Encrypt files {error_message}")
        update_into_dynamo(task_id=p_task_id,processing_status='failed',desc_msg=error_message)
        raise

if __name__=='__main__':       
    if len(sys.argv)!=8:
        log.info('Incorrect arguments passsed ! , please send args in this sequence')
        log.info(f"Task id  , Customer id , job id , execution id , input folder , output folder , Public key name")
    else:
        try:
            task_id=sys.argv[1]
            customerId=sys.argv[2]
            jobId=sys.argv[3]
            executionId=sys.argv[4]
            inputFolder=sys.argv[5]      
            outputFolder=sys.argv[6]
            secret_name=sys.argv[7]
            if len(secret_name)==0: 
                log.error("No secret key provided ! please provide a secret key name")
                sys.exit(-1)
            log.info("Recieved arguments for Encryption request as follows")
            log.info(f"Task id : {task_id} , Customer id : {customerId}, job id : {jobId}, execution id : {executionId}, input folder : {inputFolder}, output folder : {outputFolder}, secret name : {secret_name}")
            #create registry 
            register_to_dynamo(customer_name=customerId,execution_id=executionId,
                           job_id=jobId,operation_type='encryption',task_id=task_id)
            register_to_snowflake(id = executionId, job_id = jobId, transformation_type = 'encryption', processing_status = 'inprogress')
            #get list of files from s3
            file_queue=get_list_of_files_from_s3(p_input_folder=inputFolder,p_task_id=task_id)
            #download files to tmp folder 
            download_manager(p_file_queue=file_queue,p_task_id=task_id)
            #start compressing files
            EncryptFiles(secret_name=secret_name,task_id=task_id) 
            #upload back to s3 
            upload_manager(p_task_id=task_id,p_output_folder=outputFolder)
            update_into_dynamo(task_id=task_id,processing_status='finished')
            register_to_snowflake(id = executionId, job_id = jobId, transformation_type = 'encryption', processing_status = 'finished')
        except Exception as e:
            sys.exit(-1)        
